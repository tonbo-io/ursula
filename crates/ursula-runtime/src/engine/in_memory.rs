use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use ursula_shard::{BucketStreamId, CoreId, RaftGroupId, ShardId, ShardPlacement};
use ursula_stream::{
    AppendStreamInput, ProducerRequest, StreamCommand, StreamErrorCode, StreamMessageRecord,
    StreamReadPlan, StreamReadSegment, StreamResponse, StreamSnapshot, StreamStateMachine,
};

use super::{
    GroupAckColdGcFuture, GroupAppendBatchFuture, GroupAppendBatchResponse, GroupAppendFuture,
    GroupBootstrapStreamFuture, GroupCloseStreamFuture, GroupColdHotBacklogFuture,
    GroupCreateStreamFuture, GroupDeleteSnapshotFuture, GroupDeleteStreamFuture, GroupEngine,
    GroupEngineCreateFuture, GroupEngineError, GroupEngineFactory, GroupEngineMetrics,
    GroupFlushColdFuture, GroupForkRefFuture, GroupHeadStreamFuture, GroupInstallSnapshotFuture,
    GroupPlanColdFlushFuture, GroupPlanColdGcFuture, GroupPlanNextColdFlushBatchFuture,
    GroupPlanNextColdFlushFuture, GroupPublishSnapshotFuture, GroupReadSnapshotFuture,
    GroupReadStreamFuture, GroupReadStreamPartsFuture, GroupSnapshotFuture,
    GroupTouchStreamAccessFuture, GroupWriteResponse,
};
use crate::cold_index::{
    ColdIndexPageCache, ColdStoreColdIndexPageStore, write_cold_chunk_index_pages,
    write_external_segment_index_pages,
};
use crate::cold_store::{ColdStoreHandle, DEFAULT_CONTENT_TYPE};
use crate::command::{GroupSnapshot, GroupWriteCommand};
use crate::request::{
    AckColdGcResponse, AppendBatchRequest, AppendExternalRequest, AppendRequest, AppendResponse,
    BootstrapStreamRequest, BootstrapStreamResponse, BootstrapUpdate, CloseStreamRequest,
    CloseStreamResponse, ColdHotBacklog, ColdWriteAdmission, CreateStreamExternalRequest,
    CreateStreamRequest, CreateStreamResponse, DeleteSnapshotRequest, DeleteStreamRequest,
    DeleteStreamResponse, FlushColdRequest, FlushColdResponse, ForkRefResponse,
    GroupReadStreamParts, HeadStreamRequest, HeadStreamResponse, PlanColdFlushRequest,
    PlanGroupColdFlushRequest, PublishSnapshotRequest, PublishSnapshotResponse,
    ReadSnapshotRequest, ReadSnapshotResponse, ReadStreamRequest, StreamAppendCount,
    TouchStreamAccessResponse,
};

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
    pub(crate) stream_append_counts: HashMap<BucketStreamId, u64>,
    pub(crate) cold_store: Option<ColdStoreHandle>,
    pub(crate) cold_index_cache: Option<Arc<ColdIndexPageCache<ColdStoreColdIndexPageStore>>>,
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
            GroupWriteCommand::CreateStream {
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
                now_ms,
            } => {
                ensure_bucket_exists(&mut self.state_machine, &stream_id)?;
                let response = self.state_machine.apply(StreamCommand::CreateStream {
                    stream_id,
                    content_type,
                    initial_payload: initial_payload.to_vec(),
                    close_after,
                    stream_seq,
                    producer,
                    stream_ttl_seconds,
                    stream_expires_at_ms,
                    forked_from,
                    fork_offset,
                    now_ms,
                });
                match response {
                    StreamResponse::Created {
                        next_offset,
                        closed,
                        ..
                    } => {
                        self.commit_index += 1;
                        Ok(GroupWriteResponse::CreateStream(CreateStreamResponse {
                            placement,
                            next_offset,
                            closed,
                            already_exists: false,
                            group_commit_index: self.commit_index,
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
                    })),
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected create stream response: {other:?}"
                    ))),
                }
            }
            GroupWriteCommand::CreateExternal {
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
                now_ms,
            } => {
                ensure_bucket_exists(&mut self.state_machine, &stream_id)?;
                let response = self.state_machine.apply(StreamCommand::CreateExternal {
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
                    now_ms,
                });
                match response {
                    StreamResponse::Created {
                        next_offset,
                        closed,
                        ..
                    } => {
                        self.commit_index += 1;
                        Ok(GroupWriteResponse::CreateStream(CreateStreamResponse {
                            placement,
                            next_offset,
                            closed,
                            already_exists: false,
                            group_commit_index: self.commit_index,
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
                    })),
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected create external stream response: {other:?}"
                    ))),
                }
            }
            GroupWriteCommand::Append {
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
                        content_type: Some(&content_type),
                        payload: &payload,
                        close_after,
                        stream_seq,
                        producer,
                        now_ms,
                    },
                    placement,
                )
                .map(GroupWriteResponse::Append),
            GroupWriteCommand::AppendExternal {
                stream_id,
                content_type,
                payload,
                close_after,
                stream_seq,
                producer,
                now_ms,
            } => {
                let response = self.state_machine.apply(StreamCommand::AppendExternal {
                    stream_id: stream_id.clone(),
                    content_type: Some(content_type),
                    payload,
                    close_after,
                    stream_seq,
                    producer,
                    now_ms,
                });
                match response {
                    StreamResponse::Appended {
                        offset,
                        next_offset,
                        closed,
                        deduplicated,
                        producer,
                        ..
                    } => {
                        let stream_append_count =
                            self.stream_append_counts.entry(stream_id).or_insert(0);
                        if !deduplicated {
                            self.commit_index += 1;
                            *stream_append_count += 1;
                        }
                        Ok(GroupWriteResponse::Append(AppendResponse {
                            placement,
                            start_offset: offset,
                            next_offset,
                            stream_append_count: *stream_append_count,
                            group_commit_index: self.commit_index,
                            closed,
                            deduplicated,
                            producer,
                        }))
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected append external response: {other:?}"
                    ))),
                }
            }
            GroupWriteCommand::AppendBatch {
                stream_id,
                content_type,
                payloads,
                producer,
                now_ms,
            } => {
                if producer.is_some() {
                    let payload_refs = payloads.iter().map(Bytes::as_ref).collect::<Vec<_>>();
                    let batch = self
                        .state_machine
                        .append_batch_borrowed(
                            stream_id.clone(),
                            Some(&content_type),
                            &payload_refs,
                            producer,
                            now_ms,
                        )
                        .map_err(stream_response_error)?;
                    let old_commit_index = self.commit_index;
                    let old_append_count = *self.stream_append_counts.get(&stream_id).unwrap_or(&0);
                    if !batch.deduplicated {
                        let count = u64::try_from(batch.items.len()).expect("item count fits u64");
                        self.commit_index += count;
                        *self.stream_append_counts.entry(stream_id).or_insert(0) += count;
                    }
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
                            content_type: Some(&content_type),
                            payload: &payload,
                            close_after: false,
                            stream_seq: None,
                            producer: None,
                            now_ms,
                        },
                        placement,
                    ));
                }
                Ok(GroupWriteResponse::AppendBatch(GroupAppendBatchResponse {
                    placement,
                    items,
                }))
            }
            GroupWriteCommand::PublishSnapshot {
                stream_id,
                snapshot_offset,
                content_type,
                payload,
                now_ms,
            } => {
                let response = self.state_machine.apply(StreamCommand::PublishSnapshot {
                    stream_id,
                    snapshot_offset,
                    content_type,
                    payload: payload.to_vec(),
                    now_ms,
                });
                match response {
                    StreamResponse::SnapshotPublished { snapshot_offset } => {
                        self.commit_index += 1;
                        Ok(GroupWriteResponse::PublishSnapshot(
                            PublishSnapshotResponse {
                                placement,
                                snapshot_offset,
                                group_commit_index: self.commit_index,
                            },
                        ))
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected publish snapshot response: {other:?}"
                    ))),
                }
            }
            GroupWriteCommand::TouchStreamAccess {
                stream_id,
                now_ms,
                renew_ttl,
            } => {
                let response = self.state_machine.apply(StreamCommand::TouchStreamAccess {
                    stream_id,
                    now_ms,
                    renew_ttl,
                });
                match response {
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
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected touch stream access response: {other:?}"
                    ))),
                }
            }
            GroupWriteCommand::AddForkRef { stream_id, now_ms } => {
                let response = self
                    .state_machine
                    .apply(StreamCommand::AddForkRef { stream_id, now_ms });
                match response {
                    StreamResponse::ForkRefAdded { fork_ref_count } => {
                        self.commit_index += 1;
                        Ok(GroupWriteResponse::AddForkRef(ForkRefResponse {
                            placement,
                            fork_ref_count,
                            hard_deleted: false,
                            parent_to_release: None,
                            group_commit_index: self.commit_index,
                        }))
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected add fork ref response: {other:?}"
                    ))),
                }
            }
            GroupWriteCommand::ReleaseForkRef { stream_id } => {
                let response = self
                    .state_machine
                    .apply(StreamCommand::ReleaseForkRef { stream_id });
                match response {
                    StreamResponse::ForkRefReleased {
                        hard_deleted,
                        fork_ref_count,
                        parent_to_release,
                    } => {
                        self.commit_index += 1;
                        Ok(GroupWriteResponse::ReleaseForkRef(ForkRefResponse {
                            placement,
                            fork_ref_count,
                            hard_deleted,
                            parent_to_release,
                            group_commit_index: self.commit_index,
                        }))
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected release fork ref response: {other:?}"
                    ))),
                }
            }
            GroupWriteCommand::FlushCold { stream_id, chunk } => {
                let response = self
                    .state_machine
                    .apply(StreamCommand::FlushCold { stream_id, chunk });
                match response {
                    StreamResponse::ColdFlushed { hot_start_offset } => {
                        self.commit_index += 1;
                        Ok(GroupWriteResponse::FlushCold(FlushColdResponse {
                            placement,
                            hot_start_offset,
                            group_commit_index: self.commit_index,
                        }))
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected flush cold response: {other:?}"
                    ))),
                }
            }
            GroupWriteCommand::CloseStream {
                stream_id,
                stream_seq,
                producer,
                now_ms,
            } => {
                let response = self.state_machine.apply(StreamCommand::Close {
                    stream_id,
                    stream_seq,
                    producer,
                    now_ms,
                });
                match response {
                    StreamResponse::Closed {
                        next_offset,
                        deduplicated,
                        ..
                    } => {
                        if !deduplicated {
                            self.commit_index += 1;
                        }
                        Ok(GroupWriteResponse::CloseStream(CloseStreamResponse {
                            placement,
                            next_offset,
                            group_commit_index: self.commit_index,
                            deduplicated,
                        }))
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected close stream response: {other:?}"
                    ))),
                }
            }
            GroupWriteCommand::DeleteStream { stream_id } => {
                let response = self.state_machine.apply(StreamCommand::DeleteStream {
                    stream_id: stream_id.clone(),
                });
                match response {
                    StreamResponse::Deleted {
                        hard_deleted,
                        parent_to_release,
                    } => {
                        self.commit_index += 1;
                        if hard_deleted {
                            // Stream is gone: drop its runtime append count so the
                            // map stays bounded under delete churn (snapshot build
                            // also filters, but this avoids unbounded growth).
                            self.stream_append_counts.remove(&stream_id);
                        }
                        Ok(GroupWriteResponse::DeleteStream(DeleteStreamResponse {
                            placement,
                            group_commit_index: self.commit_index,
                            hard_deleted,
                            parent_to_release,
                        }))
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected delete stream response: {other:?}"
                    ))),
                }
            }
            GroupWriteCommand::AckColdGc { up_to_seq } => {
                let response = self
                    .state_machine
                    .apply(StreamCommand::AckColdGc { up_to_seq });
                match response {
                    StreamResponse::ColdGcAcked { removed } => {
                        self.commit_index += 1;
                        Ok(GroupWriteResponse::AckColdGc(AckColdGcResponse {
                            placement,
                            removed,
                            group_commit_index: self.commit_index,
                        }))
                    }
                    other => Err(GroupEngineError::new(format!(
                        "unexpected ack cold gc response: {other:?}"
                    ))),
                }
            }
            GroupWriteCommand::Batch { commands } => Ok(GroupWriteResponse::Batch(
                self.apply_committed_write_batch(commands, placement),
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

    pub fn check_cold_write_admission_bytes(
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
        Err(GroupEngineError::new(format!(
            "ColdBackpressure: stream '{stream_id}' would raise group hot bytes from {before} to {after}, above limit {limit}"
        )))
    }

    pub(crate) fn create_stream_with_admission_inner(
        &mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> Result<CreateStreamResponse, GroupEngineError> {
        let stream_id = request.stream_id.clone();
        if admission.is_enabled() {
            let mut preview = self.clone();
            let preview_response = match preview
                .apply_committed_write(GroupWriteCommand::from(request.clone()), placement)?
            {
                GroupWriteResponse::CreateStream(response) => response,
                other => {
                    return Err(GroupEngineError::new(format!(
                        "unexpected create stream preview response: {other:?}"
                    )));
                }
            };
            if !preview_response.already_exists {
                self.check_cold_write_admission_bytes(
                    &stream_id,
                    admission,
                    u64::try_from(request.initial_payload.len()).expect("payload len fits u64"),
                )?;
            }
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
        let stream_id = request.stream_id.clone();
        if admission.is_enabled() {
            let mut preview = self.clone();
            let preview_response = match preview
                .apply_committed_write(GroupWriteCommand::from(request.clone()), placement)?
            {
                GroupWriteResponse::Append(response) => response,
                other => {
                    return Err(GroupEngineError::new(format!(
                        "unexpected append preview response: {other:?}"
                    )));
                }
            };
            if !preview_response.deduplicated {
                self.check_cold_write_admission_bytes(
                    &stream_id,
                    admission,
                    u64::try_from(request.payload.len()).expect("payload len fits u64"),
                )?;
            }
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
        let stream_id = request.stream_id.clone();
        let incoming_bytes = request
            .payloads
            .iter()
            .map(|payload| u64::try_from(payload.len()).expect("payload len fits u64"))
            .sum();
        if admission.is_enabled() {
            let mut preview = self.clone();
            let preview_response = match preview
                .apply_committed_write(GroupWriteCommand::from(request.clone()), placement)?
            {
                GroupWriteResponse::AppendBatch(response) => response,
                other => {
                    return Err(GroupEngineError::new(format!(
                        "unexpected append batch preview response: {other:?}"
                    )));
                }
            };
            let mutates = preview_response
                .items
                .iter()
                .any(|item| matches!(item, Ok(response) if !response.deduplicated));
            if mutates {
                self.check_cold_write_admission_bytes(&stream_id, admission, incoming_bytes)?;
            }
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
            GroupWriteCommand::TouchStreamAccess {
                stream_id,
                now_ms,
                renew_ttl,
            },
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

    pub fn apply_committed_write_batch(
        &mut self,
        commands: Vec<GroupWriteCommand>,
        placement: ShardPlacement,
    ) -> Vec<Result<GroupWriteResponse, GroupEngineError>> {
        commands
            .into_iter()
            .map(|command| self.apply_committed_write(command, placement))
            .collect()
    }

    pub(crate) fn apply_replayed_write_command(
        &mut self,
        command: GroupWriteCommand,
    ) -> Result<(), GroupEngineError> {
        let placement = ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(0),
        };
        self.apply_committed_write(command, placement).map(|_| ())
    }

    pub(crate) fn apply_replayed_command(
        &mut self,
        command: StreamCommand,
    ) -> Result<(), GroupEngineError> {
        match command {
            StreamCommand::CreateBucket { bucket_id } => {
                match self
                    .state_machine
                    .apply(StreamCommand::CreateBucket { bucket_id })
                {
                    StreamResponse::BucketCreated { .. } => {
                        self.commit_index += 1;
                        Ok(())
                    }
                    StreamResponse::BucketAlreadyExists { .. } => Ok(()),
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected replay create bucket response: {other:?}"
                    ))),
                }
            }
            StreamCommand::DeleteBucket { bucket_id } => {
                match self
                    .state_machine
                    .apply(StreamCommand::DeleteBucket { bucket_id })
                {
                    StreamResponse::BucketDeleted { .. } => {
                        self.commit_index += 1;
                        Ok(())
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected replay delete bucket response: {other:?}"
                    ))),
                }
            }
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
                now_ms,
            } => {
                ensure_bucket_exists(&mut self.state_machine, &stream_id)?;
                let response = self.state_machine.apply(StreamCommand::CreateStream {
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
                    now_ms,
                });
                match response {
                    StreamResponse::Created { .. } => {
                        self.commit_index += 1;
                        Ok(())
                    }
                    StreamResponse::AlreadyExists { .. } => Ok(()),
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected replay create stream response: {other:?}"
                    ))),
                }
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
                now_ms,
            } => {
                ensure_bucket_exists(&mut self.state_machine, &stream_id)?;
                let response = self.state_machine.apply(StreamCommand::CreateExternal {
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
                    now_ms,
                });
                match response {
                    StreamResponse::Created { .. } => {
                        self.commit_index += 1;
                        Ok(())
                    }
                    StreamResponse::AlreadyExists { .. } => Ok(()),
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected replay external create stream response: {other:?}"
                    ))),
                }
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
                let stream_count_key = stream_id.clone();
                let response = self.state_machine.apply(StreamCommand::Append {
                    stream_id,
                    content_type,
                    payload,
                    close_after,
                    stream_seq,
                    producer,
                    now_ms,
                });
                match response {
                    StreamResponse::Appended { deduplicated, .. } => {
                        if !deduplicated {
                            self.commit_index += 1;
                            *self
                                .stream_append_counts
                                .entry(stream_count_key)
                                .or_insert(0) += 1;
                        }
                        Ok(())
                    }
                    StreamResponse::Closed { deduplicated, .. } => {
                        if !deduplicated {
                            self.commit_index += 1;
                        }
                        Ok(())
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected replay append response: {other:?}"
                    ))),
                }
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
                let stream_count_key = stream_id.clone();
                let response = self.state_machine.apply(StreamCommand::AppendExternal {
                    stream_id,
                    content_type,
                    payload,
                    close_after,
                    stream_seq,
                    producer,
                    now_ms,
                });
                match response {
                    StreamResponse::Appended { deduplicated, .. } => {
                        if !deduplicated {
                            self.commit_index += 1;
                            *self
                                .stream_append_counts
                                .entry(stream_count_key)
                                .or_insert(0) += 1;
                        }
                        Ok(())
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected replay external append response: {other:?}"
                    ))),
                }
            }
            StreamCommand::AppendBatch {
                stream_id,
                content_type,
                payloads,
                producer,
                now_ms,
            } => {
                let stream_count_key = stream_id.clone();
                let payload_refs = payloads.iter().map(Vec::as_slice).collect::<Vec<_>>();
                let response = self
                    .state_machine
                    .append_batch_borrowed(
                        stream_id,
                        content_type.as_deref(),
                        &payload_refs,
                        producer,
                        now_ms,
                    )
                    .map_err(stream_response_error)?;
                if !response.deduplicated {
                    let count = u64::try_from(response.items.len()).expect("item count fits u64");
                    self.commit_index += count;
                    *self
                        .stream_append_counts
                        .entry(stream_count_key)
                        .or_insert(0) += count;
                }
                Ok(())
            }
            StreamCommand::PublishSnapshot {
                stream_id,
                snapshot_offset,
                content_type,
                payload,
                now_ms,
            } => {
                let response = self.state_machine.apply(StreamCommand::PublishSnapshot {
                    stream_id,
                    snapshot_offset,
                    content_type,
                    payload,
                    now_ms,
                });
                match response {
                    StreamResponse::SnapshotPublished { .. } => {
                        self.commit_index += 1;
                        Ok(())
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected replay publish snapshot response: {other:?}"
                    ))),
                }
            }
            StreamCommand::TouchStreamAccess {
                stream_id,
                now_ms,
                renew_ttl,
            } => {
                let response = self.state_machine.apply(StreamCommand::TouchStreamAccess {
                    stream_id,
                    now_ms,
                    renew_ttl,
                });
                match response {
                    StreamResponse::Accessed { changed, expired } => {
                        if changed || expired {
                            self.commit_index += 1;
                        }
                        Ok(())
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected replay touch stream access response: {other:?}"
                    ))),
                }
            }
            StreamCommand::AddForkRef { stream_id, now_ms } => {
                let response = self
                    .state_machine
                    .apply(StreamCommand::AddForkRef { stream_id, now_ms });
                match response {
                    StreamResponse::ForkRefAdded { .. } => {
                        self.commit_index += 1;
                        Ok(())
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected replay add fork ref response: {other:?}"
                    ))),
                }
            }
            StreamCommand::ReleaseForkRef { stream_id } => {
                let response = self
                    .state_machine
                    .apply(StreamCommand::ReleaseForkRef { stream_id });
                match response {
                    StreamResponse::ForkRefReleased { .. } => {
                        self.commit_index += 1;
                        Ok(())
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected replay release fork ref response: {other:?}"
                    ))),
                }
            }
            StreamCommand::FlushCold { stream_id, chunk } => {
                let response = self
                    .state_machine
                    .apply(StreamCommand::FlushCold { stream_id, chunk });
                match response {
                    StreamResponse::ColdFlushed { .. } => {
                        self.commit_index += 1;
                        Ok(())
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected replay flush cold response: {other:?}"
                    ))),
                }
            }
            StreamCommand::Close {
                stream_id,
                stream_seq,
                producer,
                now_ms,
            } => {
                let response = self.state_machine.apply(StreamCommand::Close {
                    stream_id,
                    stream_seq,
                    producer,
                    now_ms,
                });
                match response {
                    StreamResponse::Closed { deduplicated, .. } => {
                        if !deduplicated {
                            self.commit_index += 1;
                        }
                        Ok(())
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected replay close stream response: {other:?}"
                    ))),
                }
            }
            StreamCommand::DeleteStream { stream_id } => {
                let response = self
                    .state_machine
                    .apply(StreamCommand::DeleteStream { stream_id });
                match response {
                    StreamResponse::Deleted { .. } => {
                        self.commit_index += 1;
                        Ok(())
                    }
                    StreamResponse::Error {
                        code,
                        message,
                        next_offset,
                    } => Err(GroupEngineError::stream_with_next_offset(
                        code,
                        message,
                        next_offset,
                    )),
                    other => Err(GroupEngineError::new(format!(
                        "unexpected replay delete stream response: {other:?}"
                    ))),
                }
            }
            StreamCommand::AckColdGc { up_to_seq } => {
                match self
                    .state_machine
                    .apply(StreamCommand::AckColdGc { up_to_seq })
                {
                    StreamResponse::ColdGcAcked { .. } => {
                        self.commit_index += 1;
                        Ok(())
                    }
                    other => Err(GroupEngineError::new(format!(
                        "unexpected replay ack cold gc response: {other:?}"
                    ))),
                }
            }
        }
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
        match response {
            StreamResponse::Appended {
                offset,
                next_offset,
                closed,
                deduplicated,
                producer,
                ..
            } => {
                let stream_append_count = self
                    .stream_append_counts
                    .entry(stream_count_key)
                    .or_insert(0);
                if !deduplicated {
                    self.commit_index += 1;
                    *stream_append_count += 1;
                }
                Ok(AppendResponse {
                    placement,
                    start_offset: offset,
                    next_offset,
                    stream_append_count: *stream_append_count,
                    group_commit_index: self.commit_index,
                    closed,
                    deduplicated,
                    producer,
                })
            }
            StreamResponse::Error {
                code,
                message,
                next_offset,
            } => Err(GroupEngineError::stream_with_next_offset(
                code,
                message,
                next_offset,
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
        let _ = metadata;
        Ok(HeadStreamResponse {
            placement,
            content_type,
            tail_offset,
            cold_hot_start_offset: self.state_machine.hot_start_offset(&request.stream_id),
            closed,
            stream_ttl_seconds,
            stream_expires_at_ms,
            snapshot_offset: self
                .state_machine
                .latest_snapshot(&request.stream_id)
                .map_err(stream_response_error)?
                .map(|snapshot| snapshot.offset),
            integrity: self
                .state_machine
                .integrity_snapshot(&request.stream_id)
                .map_err(stream_response_error)?,
        })
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
                    let objects = cache
                        .object_segments_for_read(stream_id, segment)
                        .await
                        .map_err(|err| GroupEngineError::new(err.to_string()))?;
                    let segment_end = segment
                        .read_start_offset
                        .saturating_add(u64::try_from(segment.len).expect("read len fits u64"));
                    for object in objects {
                        let start = object.start_offset.max(segment.read_start_offset);
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
                            .await
                            .map_err(|err| GroupEngineError::new(err.to_string()))?;
                        payload.extend_from_slice(&bytes);
                    }
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

    pub(crate) async fn bootstrap_updates(
        &self,
        stream_id: &BucketStreamId,
        records: &[StreamMessageRecord],
        content_type: &str,
        now_ms: u64,
    ) -> Result<Vec<BootstrapUpdate>, GroupEngineError> {
        let mut updates = Vec::with_capacity(records.len());
        for record in records {
            let len = usize::try_from(record.end_offset - record.start_offset).map_err(|_| {
                GroupEngineError::stream(
                    StreamErrorCode::InvalidSnapshot,
                    format!(
                        "bootstrap message [{}..{}) for stream '{stream_id}' is too large",
                        record.start_offset, record.end_offset
                    ),
                )
            })?;
            let plan = self
                .state_machine
                .read_plan_at(stream_id, record.start_offset, len, now_ms)
                .map_err(stream_response_error)?;
            let payload = self.read_own_payload_from_plan(stream_id, &plan).await?;
            updates.push(BootstrapUpdate {
                start_offset: record.start_offset,
                next_offset: record.end_offset,
                content_type: content_type.to_owned(),
                payload,
            });
        }
        Ok(updates)
    }

    pub(crate) fn build_snapshot(&self, placement: ShardPlacement) -> GroupSnapshot {
        let stream_snapshot = self.state_machine.snapshot();
        let stream_append_counts = self.stream_append_counts_snapshot(&stream_snapshot);
        GroupSnapshot {
            placement,
            group_commit_index: self.commit_index,
            stream_snapshot,
            stream_append_counts,
        }
    }

    pub(crate) fn stream_append_counts_snapshot(
        &self,
        stream_snapshot: &ursula_stream::StreamSnapshot,
    ) -> Vec<StreamAppendCount> {
        // Only emit append counts for streams actually present in the snapshot.
        // A deleted/expired stream can leave a stale entry in the runtime map;
        // emitting it would make every follower's `install_snapshot` fail the
        // `restore_stream_append_counts` consistency check, so a lagging node
        // could never catch up (and leadership transfer, which catches the
        // target up via a snapshot, could never complete).
        let live: HashSet<&BucketStreamId> = stream_snapshot
            .streams
            .iter()
            .map(|entry| &entry.metadata.stream_id)
            .collect();
        let mut counts = self
            .stream_append_counts
            .iter()
            .filter(|(stream_id, _)| live.contains(stream_id))
            .map(|(stream_id, append_count)| StreamAppendCount {
                stream_id: stream_id.clone(),
                append_count: *append_count,
            })
            .collect::<Vec<_>>();
        counts.sort_by(|left, right| compare_stream_ids(&left.stream_id, &right.stream_id));
        counts
    }

    pub fn stream_tail_offset(&self, stream_id: &BucketStreamId) -> Option<u64> {
        self.state_machine
            .head(stream_id)
            .map(|metadata| metadata.tail_offset)
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
        let stream_ids = stream_snapshot
            .streams
            .iter()
            .map(|entry| entry.metadata.stream_id.clone())
            .collect::<HashSet<_>>();
        let state_machine = StreamStateMachine::restore(stream_snapshot)
            .map_err(|err| GroupEngineError::new(format!("restore stream snapshot: {err}")))?;
        let stream_append_counts = restore_stream_append_counts(stream_append_counts, &stream_ids)?;

        self.commit_index = group_commit_index;
        self.state_machine = state_machine;
        self.stream_append_counts = stream_append_counts;
        Ok(())
    }
}

impl GroupEngine for InMemoryGroupEngine {
    fn create_stream<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
    ) -> GroupCreateStreamFuture<'a> {
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

    fn create_stream_with_cold_admission<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupCreateStreamFuture<'a> {
        if !admission.is_enabled() {
            return self.create_stream(request, placement);
        }
        Box::pin(
            async move { self.create_stream_with_admission_inner(request, placement, admission) },
        )
    }

    fn create_stream_external<'a>(
        &'a mut self,
        request: CreateStreamExternalRequest,
        placement: ShardPlacement,
    ) -> GroupCreateStreamFuture<'a> {
        Box::pin(async move {
            if let Some(cold_store) = self.cold_store.as_ref() {
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
                payload: snapshot.payload,
                up_to_date: snapshot.offset == tail_offset,
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
                } => Err(GroupEngineError::stream_with_next_offset(
                    code,
                    message,
                    next_offset,
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

    fn add_fork_ref<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        placement: ShardPlacement,
    ) -> GroupForkRefFuture<'a> {
        Box::pin(async move {
            match self.apply_committed_write(
                GroupWriteCommand::AddForkRef { stream_id, now_ms },
                placement,
            )? {
                GroupWriteResponse::AddForkRef(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected add fork ref write response: {other:?}"
                ))),
            }
        })
    }

    fn release_fork_ref<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        placement: ShardPlacement,
    ) -> GroupForkRefFuture<'a> {
        Box::pin(async move {
            match self
                .apply_committed_write(GroupWriteCommand::ReleaseForkRef { stream_id }, placement)?
            {
                GroupWriteResponse::ReleaseForkRef(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected release fork ref write response: {other:?}"
                ))),
            }
        })
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

    fn ack_cold_gc<'a>(
        &'a mut self,
        up_to_seq: u64,
        placement: ShardPlacement,
    ) -> GroupAckColdGcFuture<'a> {
        Box::pin(async move {
            match self
                .apply_committed_write(GroupWriteCommand::AckColdGc { up_to_seq }, placement)?
            {
                GroupWriteResponse::AckColdGc(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected ack cold gc write response: {other:?}"
                ))),
            }
        })
    }

    fn plan_cold_gc<'a>(
        &'a mut self,
        max: usize,
        _placement: ShardPlacement,
    ) -> GroupPlanColdGcFuture<'a> {
        let entries = self.state_machine.pending_cold_gc_batch(max);
        Box::pin(async move { Ok(entries) })
    }

    fn append<'a>(
        &'a mut self,
        request: AppendRequest,
        placement: ShardPlacement,
    ) -> GroupAppendFuture<'a> {
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

    fn append_with_cold_admission<'a>(
        &'a mut self,
        request: AppendRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupAppendFuture<'a> {
        if !admission.is_enabled() {
            return self.append(request, placement);
        }
        Box::pin(async move { self.append_with_admission_inner(request, placement, admission) })
    }

    fn append_external<'a>(
        &'a mut self,
        request: AppendExternalRequest,
        placement: ShardPlacement,
    ) -> GroupAppendFuture<'a> {
        Box::pin(async move {
            self.ensure_stream_access(&request.stream_id, request.now_ms, false, placement)?;
            if let Some(cold_store) = self.cold_store.as_ref() {
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
                let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
                write_external_segment_index_pages(
                    &store,
                    &request.stream_id,
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
    ) -> GroupAppendBatchFuture<'a> {
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

    fn append_batch_with_cold_admission<'a>(
        &'a mut self,
        request: AppendBatchRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupAppendBatchFuture<'a> {
        if !admission.is_enabled() {
            return self.append_batch(request, placement);
        }
        Box::pin(
            async move { self.append_batch_with_admission_inner(request, placement, admission) },
        )
    }

    fn flush_cold<'a>(
        &'a mut self,
        request: FlushColdRequest,
        placement: ShardPlacement,
    ) -> GroupFlushColdFuture<'a> {
        Box::pin(async move {
            if let Some(cold_store) = self.cold_store.as_ref() {
                let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
                write_cold_chunk_index_pages(&store, &request.stream_id, &request.chunk)
                    .await
                    .map_err(|err| GroupEngineError::new(err.to_string()))?;
            }
            let command = GroupWriteCommand::from(request);
            match self.apply_committed_write(command, placement)? {
                GroupWriteResponse::FlushCold(response) => Ok(response),
                other => Err(GroupEngineError::new(format!(
                    "unexpected flush cold write response: {other:?}"
                ))),
            }
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

    fn plan_next_cold_flush<'a>(
        &'a mut self,
        request: PlanGroupColdFlushRequest,
        _placement: ShardPlacement,
    ) -> GroupPlanNextColdFlushFuture<'a> {
        Box::pin(async move {
            self.state_machine
                .plan_next_cold_flush(request.min_hot_bytes, request.max_flush_bytes)
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
                .plan_next_cold_flush_batch(
                    request.min_hot_bytes,
                    request.max_flush_bytes,
                    max_candidates,
                )
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
        } => Err(GroupEngineError::stream_with_next_offset(
            code,
            message,
            next_offset,
        )),
        other => Err(GroupEngineError::new(format!(
            "unexpected create bucket response: {other:?}"
        ))),
    }
}

pub(crate) fn stream_response_error(response: StreamResponse) -> GroupEngineError {
    match response {
        StreamResponse::Error {
            code,
            message,
            next_offset,
        } => GroupEngineError::stream_with_next_offset(code, message, next_offset),
        other => GroupEngineError::new(format!("unexpected stream response error: {other:?}")),
    }
}

pub(crate) fn restore_stream_append_counts(
    counts: Vec<StreamAppendCount>,
    snapshot_stream_ids: &HashSet<BucketStreamId>,
) -> Result<HashMap<BucketStreamId, u64>, GroupEngineError> {
    let mut restored = HashMap::with_capacity(counts.len());
    for count in counts {
        if !snapshot_stream_ids.contains(&count.stream_id) {
            return Err(GroupEngineError::new(format!(
                "append count references missing snapshot stream '{}'",
                count.stream_id
            )));
        }
        if restored
            .insert(count.stream_id.clone(), count.append_count)
            .is_some()
        {
            return Err(GroupEngineError::new(format!(
                "snapshot contains duplicate append count for stream '{}'",
                count.stream_id
            )));
        }
    }
    Ok(restored)
}
