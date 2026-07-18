use std::path::Path;
use std::path::PathBuf;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::BucketStreamId;
use ursula_shard::ShardPlacement;
use ursula_stream::StreamErrorCode;
use ursula_stream::StreamSnapshot;

use super::GroupAppendBatchFuture;
use super::GroupAppendFuture;
use super::GroupBootstrapStreamFuture;
use super::GroupCloseStreamFuture;
use super::GroupColdHotBacklogFuture;
use super::GroupCreateStreamFuture;
use super::GroupDeleteSnapshotFuture;
use super::GroupDeleteStreamFuture;
use super::GroupEngine;
use super::GroupEngineCreateFuture;
use super::GroupEngineError;
use super::GroupEngineFactory;
use super::GroupEngineMetrics;
use super::GroupFlushColdFuture;
use super::GroupGetStreamAttrsFuture;
use super::GroupHeadStreamFuture;
use super::GroupInstallSnapshotFuture;
use super::GroupPlanColdFlushFuture;
use super::GroupPlanNextColdFlushBatchFuture;
use super::GroupPlanNextColdFlushFuture;
use super::GroupPublishSnapshotFuture;
use super::GroupReadSnapshotFuture;
use super::GroupReadStreamFuture;
use super::GroupSnapshotFuture;
use super::GroupTouchStreamAccessFuture;
use super::GroupUpdateStreamAttrsFuture;
use super::GroupWriteResponse;
use super::in_memory::InMemoryGroupEngine;
use crate::cold_store::ColdStoreHandle;
use crate::command::GroupSnapshot;
use crate::command::GroupWriteCommand;
use crate::journal;
use crate::metrics::elapsed_ns;
use crate::request::AppendBatchRequest;
use crate::request::AppendRequest;
use crate::request::BootstrapStreamRequest;
use crate::request::CloseStreamRequest;
use crate::request::ColdWriteAdmission;
use crate::request::CreateStreamRequest;
use crate::request::DeleteSnapshotRequest;
use crate::request::DeleteStreamRequest;
use crate::request::FlushColdRequest;
use crate::request::GetStreamAttrsRequest;
use crate::request::HeadStreamRequest;
use crate::request::PlanColdFlushRequest;
use crate::request::PlanGroupColdFlushRequest;
use crate::request::PublishSnapshotRequest;
use crate::request::ReadSnapshotRequest;
use crate::request::ReadStreamRequest;
use crate::request::StreamAppendCount;
use crate::request::TouchStreamAccessResponse;
use crate::request::UpdateStreamAttrsRequest;
use crate::rt::time::Instant;

#[derive(Debug, Clone)]
pub struct WalGroupEngineFactory {
    root: PathBuf,
    cold_store: Option<ColdStoreHandle>,
}

impl WalGroupEngineFactory {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            cold_store: None,
        }
    }

    pub fn with_cold_store(root: impl Into<PathBuf>, cold_store: Option<ColdStoreHandle>) -> Self {
        Self {
            root: root.into(),
            cold_store,
        }
    }
}

impl GroupEngineFactory for WalGroupEngineFactory {
    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        Box::pin(async move {
            let engine: Box<dyn GroupEngine> = Box::new(WalGroupEngine::open(
                &self.root,
                placement,
                metrics,
                self.cold_store.clone(),
            ));
            Ok(engine)
        })
    }
}

pub struct WalGroupEngine {
    inner: InMemoryGroupEngine,
    log_path: PathBuf,
    placement: ShardPlacement,
    metrics: GroupEngineMetrics,
    init_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "wal_record", rename_all = "snake_case")]
enum WalRecord {
    Command {
        command: Box<GroupWriteCommand>,
    },
    Snapshot {
        group_commit_index: u64,
        stream_snapshot: StreamSnapshot,
        stream_append_counts: Vec<StreamAppendCount>,
    },
}

impl WalGroupEngine {
    fn open(
        root: &Path,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
        cold_store: Option<ColdStoreHandle>,
    ) -> Self {
        let log_path = group_log_path(root, placement);
        match replay_group_log(&log_path) {
            Ok(mut inner) => {
                inner.set_cold_store(cold_store);
                Self {
                    inner,
                    log_path,
                    placement,
                    metrics,
                    init_error: None,
                }
            }
            Err(err) => Self {
                inner: {
                    let mut inner = InMemoryGroupEngine::default();
                    inner.set_cold_store(cold_store);
                    inner
                },
                log_path,
                placement,
                metrics,
                init_error: Some(err.message().into_owned()),
            },
        }
    }

    fn ensure_ready(&self) -> Result<(), GroupEngineError> {
        match &self.init_error {
            Some(message) => Err(GroupEngineError::new(message.clone())),
            None => Ok(()),
        }
    }

    fn append_record(&self, command: &GroupWriteCommand) -> Result<(), GroupEngineError> {
        self.append_records(std::slice::from_ref(command))
    }

    fn append_records(&self, commands: &[GroupWriteCommand]) -> Result<(), GroupEngineError> {
        let records = commands
            .iter()
            .map(|command| WalRecord::Command {
                command: Box::new(command.clone()),
            })
            .collect::<Vec<_>>();
        self.append_wal_records(&records)
    }

    /// Frame each record into the WAL, `fsync` once, and meter the batch.
    fn append_wal_records(&self, records: &[WalRecord]) -> Result<(), GroupEngineError> {
        if records.is_empty() {
            return Ok(());
        }
        let mut writer = journal::JournalWriter::new(!self.log_path.exists());
        let write_started_at = Instant::now();
        for record in records {
            writer
                .append::<journal::JsonCodec<WalRecord>>(&self.log_path, record)
                .map_err(|err| {
                    GroupEngineError::new(format!("write WAL '{}': {err}", self.log_path.display()))
                })?;
        }
        let write_ns = elapsed_ns(write_started_at);
        let sync_started_at = Instant::now();
        writer.sync(&self.log_path).map_err(|err| {
            GroupEngineError::new(format!("sync WAL '{}': {err}", self.log_path.display()))
        })?;
        self.metrics.record_wal_batch(
            self.placement,
            records.len(),
            write_ns,
            elapsed_ns(sync_started_at),
        );
        Ok(())
    }

    fn append_snapshot_record(&self, snapshot: &GroupSnapshot) -> Result<(), GroupEngineError> {
        let record = WalRecord::Snapshot {
            group_commit_index: snapshot.group_commit_index,
            stream_snapshot: snapshot.stream_snapshot.clone(),
            stream_append_counts: snapshot.stream_append_counts.clone(),
        };
        self.append_wal_records(std::slice::from_ref(&record))
    }

    fn commit_access_if_needed(
        &mut self,
        stream_id: &BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
        placement: ShardPlacement,
    ) -> Result<Option<TouchStreamAccessResponse>, GroupEngineError> {
        if !self
            .inner
            .access_requires_write(stream_id, now_ms, renew_ttl)?
        {
            return Ok(None);
        }
        let command = GroupWriteCommand::TouchStreamAccess {
            stream_id: stream_id.clone(),
            now_ms,
            renew_ttl,
        };
        let mut preview = self.inner.clone();
        let response = match preview.apply_committed_write(command.clone(), placement)? {
            GroupWriteResponse::TouchStreamAccess(response) => response,
            other => {
                return Err(GroupEngineError::new(format!(
                    "unexpected touch stream access write response: {other:?}"
                )));
            }
        };
        if response.changed || response.expired {
            self.append_record(&command)?;
        }
        self.inner = preview;
        if response.expired {
            return Err(GroupEngineError::stream(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            ));
        }
        Ok(Some(response))
    }
}

impl GroupEngine for WalGroupEngine {
    fn create_stream<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
    ) -> GroupCreateStreamFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            let command = GroupWriteCommand::from(request);
            let mut preview = self.inner.clone();
            let response = match preview.apply_committed_write(command.clone(), placement)? {
                GroupWriteResponse::CreateStream(response) => response,
                other => {
                    return Err(GroupEngineError::new(format!(
                        "unexpected create stream write response: {other:?}"
                    )));
                }
            };
            if !response.already_exists {
                self.append_record(&command)?;
            }
            self.inner = preview;
            Ok(response)
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
        Box::pin(async move {
            self.ensure_ready()?;
            let command = GroupWriteCommand::from(request.clone());
            let mut preview = self.inner.clone();
            let response =
                preview.create_stream_with_admission_inner(request, placement, admission)?;
            if !response.already_exists {
                self.append_record(&command)?;
            }
            self.inner = preview;
            Ok(response)
        })
    }

    fn head_stream<'a>(
        &'a mut self,
        request: HeadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupHeadStreamFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.commit_access_if_needed(&request.stream_id, request.now_ms, false, placement)?;
            self.inner.head_stream(request, placement).await
        })
    }

    fn get_stream_attrs<'a>(
        &'a mut self,
        request: GetStreamAttrsRequest,
        placement: ShardPlacement,
    ) -> GroupGetStreamAttrsFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.commit_access_if_needed(&request.stream_id, request.now_ms, false, placement)?;
            self.inner.get_stream_attrs(request, placement).await
        })
    }

    fn read_stream<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupReadStreamFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.commit_access_if_needed(&request.stream_id, request.now_ms, true, placement)?;
            self.inner.read_stream(request, placement).await
        })
    }

    fn publish_snapshot<'a>(
        &'a mut self,
        request: PublishSnapshotRequest,
        placement: ShardPlacement,
    ) -> GroupPublishSnapshotFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.commit_access_if_needed(&request.stream_id, request.now_ms, false, placement)?;
            let command = GroupWriteCommand::from(request);
            let mut preview = self.inner.clone();
            let response = match preview.apply_committed_write(command.clone(), placement)? {
                GroupWriteResponse::PublishSnapshot(response) => response,
                other => {
                    return Err(GroupEngineError::new(format!(
                        "unexpected publish snapshot write response: {other:?}"
                    )));
                }
            };
            self.append_record(&command)?;
            self.inner = preview;
            Ok(response)
        })
    }

    fn read_snapshot<'a>(
        &'a mut self,
        request: ReadSnapshotRequest,
        placement: ShardPlacement,
    ) -> GroupReadSnapshotFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.commit_access_if_needed(&request.stream_id, request.now_ms, true, placement)?;
            self.inner.read_snapshot(request, placement).await
        })
    }

    fn delete_snapshot<'a>(
        &'a mut self,
        request: DeleteSnapshotRequest,
        placement: ShardPlacement,
    ) -> GroupDeleteSnapshotFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.commit_access_if_needed(&request.stream_id, request.now_ms, false, placement)?;
            self.inner.delete_snapshot(request, placement).await
        })
    }

    fn bootstrap_stream<'a>(
        &'a mut self,
        request: BootstrapStreamRequest,
        placement: ShardPlacement,
    ) -> GroupBootstrapStreamFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.commit_access_if_needed(&request.stream_id, request.now_ms, true, placement)?;
            self.inner.bootstrap_stream(request, placement).await
        })
    }

    fn touch_stream_access<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
        placement: ShardPlacement,
    ) -> GroupTouchStreamAccessFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            let command = GroupWriteCommand::TouchStreamAccess {
                stream_id,
                now_ms,
                renew_ttl,
            };
            let mut preview = self.inner.clone();
            let response = match preview.apply_committed_write(command.clone(), placement)? {
                GroupWriteResponse::TouchStreamAccess(response) => response,
                other => {
                    return Err(GroupEngineError::new(format!(
                        "unexpected touch stream access write response: {other:?}"
                    )));
                }
            };
            if response.changed || response.expired {
                self.append_record(&command)?;
            }
            self.inner = preview;
            Ok(response)
        })
    }

    fn update_stream_attrs<'a>(
        &'a mut self,
        request: UpdateStreamAttrsRequest,
        placement: ShardPlacement,
    ) -> GroupUpdateStreamAttrsFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            let command = GroupWriteCommand::from(request);
            let mut preview = self.inner.clone();
            let response = match preview.apply_committed_write(command.clone(), placement)? {
                GroupWriteResponse::UpdateStreamAttrs(response) => response,
                other => {
                    return Err(GroupEngineError::new(format!(
                        "unexpected update stream attrs write response: {other:?}"
                    )));
                }
            };
            if response.changed {
                self.append_record(&command)?;
            }
            self.inner = preview;
            Ok(response)
        })
    }

    fn close_stream<'a>(
        &'a mut self,
        request: CloseStreamRequest,
        placement: ShardPlacement,
    ) -> GroupCloseStreamFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.commit_access_if_needed(&request.stream_id, request.now_ms, false, placement)?;
            let command = GroupWriteCommand::from(request);
            let mut preview = self.inner.clone();
            let response = match preview.apply_committed_write(command.clone(), placement)? {
                GroupWriteResponse::CloseStream(response) => response,
                other => {
                    return Err(GroupEngineError::new(format!(
                        "unexpected close stream write response: {other:?}"
                    )));
                }
            };
            self.append_record(&command)?;
            self.inner = preview;
            Ok(response)
        })
    }

    fn delete_stream<'a>(
        &'a mut self,
        request: DeleteStreamRequest,
        placement: ShardPlacement,
    ) -> GroupDeleteStreamFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            let command = GroupWriteCommand::from(request);
            let mut preview = self.inner.clone();
            let response = match preview.apply_committed_write(command.clone(), placement)? {
                GroupWriteResponse::DeleteStream(response) => response,
                other => {
                    return Err(GroupEngineError::new(format!(
                        "unexpected delete stream write response: {other:?}"
                    )));
                }
            };
            self.append_record(&command)?;
            self.inner = preview;
            Ok(response)
        })
    }

    fn append<'a>(
        &'a mut self,
        request: AppendRequest,
        placement: ShardPlacement,
    ) -> GroupAppendFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.commit_access_if_needed(&request.stream_id, request.now_ms, false, placement)?;
            let command = GroupWriteCommand::from(request);
            let mut preview = self.inner.clone();
            let response = match preview.apply_committed_write(command.clone(), placement)? {
                GroupWriteResponse::Append(response) => response,
                other => {
                    return Err(GroupEngineError::new(format!(
                        "unexpected append write response: {other:?}"
                    )));
                }
            };
            self.append_record(&command)?;
            self.inner = preview;
            Ok(response)
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
        Box::pin(async move {
            self.ensure_ready()?;
            self.commit_access_if_needed(&request.stream_id, request.now_ms, false, placement)?;
            let command = GroupWriteCommand::from(request.clone());
            let mut preview = self.inner.clone();
            let response = preview.append_with_admission_inner(request, placement, admission)?;
            if !response.deduplicated {
                self.append_record(&command)?;
            }
            self.inner = preview;
            Ok(response)
        })
    }

    fn append_batch<'a>(
        &'a mut self,
        request: AppendBatchRequest,
        placement: ShardPlacement,
    ) -> GroupAppendBatchFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.commit_access_if_needed(&request.stream_id, request.now_ms, false, placement)?;
            let command = GroupWriteCommand::from(request);
            let mut preview = self.inner.clone();
            let response = match preview.apply_committed_write(command.clone(), placement)? {
                GroupWriteResponse::AppendBatch(response) => response,
                other => {
                    return Err(GroupEngineError::new(format!(
                        "unexpected append batch write response: {other:?}"
                    )));
                }
            };
            if response
                .items
                .iter()
                .any(|item| matches!(item, Ok(response) if !response.deduplicated))
            {
                self.append_record(&command)?;
            }
            self.inner = preview;
            Ok(response)
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
        Box::pin(async move {
            self.ensure_ready()?;
            self.commit_access_if_needed(&request.stream_id, request.now_ms, false, placement)?;
            let command = GroupWriteCommand::from(request.clone());
            let mut preview = self.inner.clone();
            let response =
                preview.append_batch_with_admission_inner(request, placement, admission)?;
            if response
                .items
                .iter()
                .any(|item| matches!(item, Ok(response) if !response.deduplicated))
            {
                self.append_record(&command)?;
            }
            self.inner = preview;
            Ok(response)
        })
    }

    fn flush_cold<'a>(
        &'a mut self,
        request: FlushColdRequest,
        placement: ShardPlacement,
    ) -> GroupFlushColdFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            let command = GroupWriteCommand::from(request);
            let mut preview = self.inner.clone();
            let response = match preview.apply_committed_write(command.clone(), placement)? {
                GroupWriteResponse::FlushCold(response) => response,
                other => {
                    return Err(GroupEngineError::new(format!(
                        "unexpected flush cold write response: {other:?}"
                    )));
                }
            };
            self.append_record(&command)?;
            self.inner = preview;
            Ok(response)
        })
    }

    fn plan_cold_flush<'a>(
        &'a mut self,
        request: PlanColdFlushRequest,
        placement: ShardPlacement,
    ) -> GroupPlanColdFlushFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.inner.plan_cold_flush(request, placement).await
        })
    }

    fn plan_next_cold_flush<'a>(
        &'a mut self,
        request: PlanGroupColdFlushRequest,
        placement: ShardPlacement,
    ) -> GroupPlanNextColdFlushFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.inner.plan_next_cold_flush(request, placement).await
        })
    }

    fn plan_next_cold_flush_batch<'a>(
        &'a mut self,
        request: PlanGroupColdFlushRequest,
        placement: ShardPlacement,
        max_candidates: usize,
    ) -> GroupPlanNextColdFlushBatchFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.inner
                .plan_next_cold_flush_batch(request, placement, max_candidates)
                .await
        })
    }

    fn cold_hot_backlog<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        placement: ShardPlacement,
    ) -> GroupColdHotBacklogFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.inner.cold_hot_backlog(stream_id, placement).await
        })
    }

    fn snapshot<'a>(&'a mut self, placement: ShardPlacement) -> GroupSnapshotFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            self.inner.snapshot(placement).await
        })
    }

    fn install_snapshot<'a>(
        &'a mut self,
        snapshot: GroupSnapshot,
    ) -> GroupInstallSnapshotFuture<'a> {
        Box::pin(async move {
            self.ensure_ready()?;
            let mut preview = self.inner.clone();
            preview.install_snapshot(snapshot.clone()).await?;
            self.append_snapshot_record(&snapshot)?;
            self.inner = preview;
            Ok(())
        })
    }
}

pub(crate) fn group_log_path(root: &Path, placement: ShardPlacement) -> PathBuf {
    root.join(format!("core-{}", placement.core_id.0))
        .join(format!("group-{}.jsonl", placement.raft_group_id.0))
}

fn replay_group_log(log_path: &Path) -> Result<InMemoryGroupEngine, GroupEngineError> {
    let records = journal::replay::<journal::JsonCodec<WalRecord>>(log_path).map_err(|err| {
        GroupEngineError::new(format!("read WAL '{}': {err}", log_path.display()))
    })?;
    let mut inner = InMemoryGroupEngine::default();
    for (index, record) in records.into_iter().enumerate() {
        match record {
            WalRecord::Command { command } => {
                inner
                    .apply_replayed_write_command(*command)
                    .map_err(|err| {
                        GroupEngineError::new(format!(
                            "replay WAL command '{}' record {}: {err}",
                            log_path.display(),
                            index + 1
                        ))
                    })?;
            }
            WalRecord::Snapshot {
                group_commit_index,
                stream_snapshot,
                stream_append_counts,
            } => {
                inner
                    .install_snapshot_parts(
                        group_commit_index,
                        stream_snapshot,
                        stream_append_counts,
                    )
                    .map_err(|err| {
                        GroupEngineError::new(format!(
                            "replay WAL snapshot '{}' record {}: {err}",
                            log_path.display(),
                            index + 1
                        ))
                    })?;
            }
        }
    }
    Ok(inner)
}
