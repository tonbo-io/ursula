use prost::Message;
use ursula_proto as raft_app_proto;
use ursula_runtime::{
    AckColdGcResponse, AppendResponse, CloseStreamResponse, CreateStreamResponse,
    DeleteStreamResponse, FlushColdResponse, ForkRefResponse, GroupAppendBatchResponse,
    GroupEngineError, GroupLeaderHint, GroupWriteCommand, GroupWriteResponse, HeadStreamResponse,
    PublishSnapshotResponse, ReadStreamResponse, StreamErrorCode, StreamIntegritySnapshot,
    TouchStreamAccessResponse,
};
use ursula_shard::BucketStreamId;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;

use crate::raft_internal_proto;
use crate::types::*;

pub(crate) fn placement_to_proto(placement: ShardPlacement) -> raft_app_proto::ShardPlacementV1 {
    raft_app_proto::ShardPlacementV1 {
        core_id: u32::from(placement.core_id.0),
        shard_id: placement.shard_id.0,
        raft_group_id: placement.raft_group_id.0,
    }
}

pub(crate) fn group_write_command_from_proto(
    command: RaftGroupCommand,
) -> Result<GroupWriteCommand, GroupEngineError> {
    use raft_app_proto::raft_group_command_v1::Command;
    let command = required(command.0.command, "raft group command")?;
    match command {
        Command::CreateStream(command) => Ok(GroupWriteCommand::CreateStream {
            stream_id: stream_id_from_proto(command.stream_id, "create_stream.stream_id")?,
            content_type: command.content_type,
            initial_payload: command.initial_payload.into(),
            close_after: command.close_after,
            stream_seq: command.stream_seq,
            producer: command.producer,
            stream_ttl_seconds: command.stream_ttl_seconds,
            stream_expires_at_ms: command.stream_expires_at_ms,
            forked_from: optional_stream_id_from_proto(command.forked_from)?,
            fork_offset: command.fork_offset,
            now_ms: command.now_ms,
        }),
        Command::CreateExternal(command) => Ok(GroupWriteCommand::CreateExternal {
            stream_id: stream_id_from_proto(command.stream_id, "create_external.stream_id")?,
            content_type: command.content_type,
            initial_payload: required(command.initial_payload, "create_external.initial_payload")?,
            close_after: command.close_after,
            stream_seq: command.stream_seq,
            producer: command.producer,
            stream_ttl_seconds: command.stream_ttl_seconds,
            stream_expires_at_ms: command.stream_expires_at_ms,
            forked_from: optional_stream_id_from_proto(command.forked_from)?,
            fork_offset: command.fork_offset,
            now_ms: command.now_ms,
        }),
        Command::Append(command) => Ok(GroupWriteCommand::Append {
            stream_id: stream_id_from_proto(command.stream_id, "append.stream_id")?,
            content_type: command.content_type,
            payload: command.payload.into(),
            close_after: command.close_after,
            stream_seq: command.stream_seq,
            producer: command.producer,
            now_ms: command.now_ms,
        }),
        Command::AppendExternal(command) => Ok(GroupWriteCommand::AppendExternal {
            stream_id: stream_id_from_proto(command.stream_id, "append_external.stream_id")?,
            content_type: command.content_type,
            payload: required(command.payload, "append_external.payload")?,
            close_after: command.close_after,
            stream_seq: command.stream_seq,
            producer: command.producer,
            now_ms: command.now_ms,
        }),
        Command::AppendBatch(command) => Ok(GroupWriteCommand::AppendBatch {
            stream_id: stream_id_from_proto(command.stream_id, "append_batch.stream_id")?,
            content_type: command.content_type,
            payloads: command
                .payloads
                .into_iter()
                .map(Into::into)
                .collect::<Vec<_>>(),
            producer: command.producer,
            now_ms: command.now_ms,
        }),
        Command::PublishSnapshot(command) => Ok(GroupWriteCommand::PublishSnapshot {
            stream_id: stream_id_from_proto(command.stream_id, "publish_snapshot.stream_id")?,
            snapshot_offset: command.snapshot_offset,
            content_type: command.content_type,
            payload: command.payload.into(),
            now_ms: command.now_ms,
        }),
        Command::TouchStreamAccess(command) => Ok(GroupWriteCommand::TouchStreamAccess {
            stream_id: stream_id_from_proto(command.stream_id, "touch_stream_access.stream_id")?,
            now_ms: command.now_ms,
            renew_ttl: command.renew_ttl,
        }),
        Command::AddForkRef(command) => Ok(GroupWriteCommand::AddForkRef {
            stream_id: stream_id_from_proto(command.stream_id, "add_fork_ref.stream_id")?,
            now_ms: command.now_ms,
        }),
        Command::ReleaseForkRef(command) => Ok(GroupWriteCommand::ReleaseForkRef {
            stream_id: stream_id_from_proto(command.stream_id, "release_fork_ref.stream_id")?,
        }),
        Command::FlushCold(command) => Ok(GroupWriteCommand::FlushCold {
            stream_id: stream_id_from_proto(command.stream_id, "flush_cold.stream_id")?,
            chunk: required(command.chunk, "flush_cold.chunk")?,
        }),
        Command::CloseStream(command) => Ok(GroupWriteCommand::CloseStream {
            stream_id: stream_id_from_proto(command.stream_id, "close_stream.stream_id")?,
            stream_seq: command.stream_seq,
            producer: command.producer,
            now_ms: command.now_ms,
        }),
        Command::DeleteStream(command) => Ok(GroupWriteCommand::DeleteStream {
            stream_id: stream_id_from_proto(command.stream_id, "delete_stream.stream_id")?,
        }),
        Command::AckColdGc(command) => Ok(GroupWriteCommand::AckColdGc {
            up_to_seq: command.up_to_seq,
        }),
        Command::Batch(command) => Ok(GroupWriteCommand::Batch {
            commands: command
                .commands
                .into_iter()
                .map(|command| group_write_command_from_proto(RaftGroupCommand(command)))
                .collect::<Result<Vec<_>, _>>()?,
        }),
    }
}

pub(crate) fn stream_id_from_proto(
    stream_id: Option<raft_app_proto::BucketStreamIdV1>,
    field: &str,
) -> Result<BucketStreamId, GroupEngineError> {
    Ok(required(stream_id, field)?.into())
}

pub(crate) fn optional_stream_id_from_proto(
    stream_id: Option<raft_app_proto::BucketStreamIdV1>,
) -> Result<Option<BucketStreamId>, GroupEngineError> {
    Ok(stream_id.map(Into::into))
}

pub(crate) fn placement_from_proto(
    placement: Option<raft_app_proto::ShardPlacementV1>,
    field: &str,
) -> Result<ShardPlacement, GroupEngineError> {
    let placement = required(placement, field)?;
    let core_id = u16::try_from(placement.core_id)
        .map_err(|_| GroupEngineError::new(format!("{field}.core_id does not fit u16")))?;
    Ok(ShardPlacement {
        core_id: CoreId(core_id),
        shard_id: ShardId(placement.shard_id),
        raft_group_id: RaftGroupId(placement.raft_group_id),
    })
}

pub(crate) fn placement_from_parts(
    core_id: u32,
    shard_id: u32,
    raft_group_id: u32,
    field: &str,
) -> Result<ShardPlacement, GroupEngineError> {
    let core_id = u16::try_from(core_id)
        .map_err(|_| GroupEngineError::new(format!("{field}.core_id does not fit u16")))?;
    Ok(ShardPlacement {
        core_id: CoreId(core_id),
        shard_id: ShardId(shard_id),
        raft_group_id: RaftGroupId(raft_group_id),
    })
}

pub(crate) fn required<T>(value: Option<T>, field: &str) -> Result<T, GroupEngineError> {
    value.ok_or_else(|| GroupEngineError::new(format!("protobuf raft payload missing {field}")))
}

pub(crate) fn raft_blank_response() -> RaftGroupResponse {
    RaftGroupResponse(raft_app_proto::RaftGroupResponseV1 {
        response: Some(raft_app_proto::raft_group_response_v1::Response::Blank(
            raft_app_proto::BlankResponseV1 {},
        )),
    })
}

pub(crate) fn raft_membership_response() -> RaftGroupResponse {
    RaftGroupResponse(raft_app_proto::RaftGroupResponseV1 {
        response: Some(
            raft_app_proto::raft_group_response_v1::Response::Membership(
                raft_app_proto::MembershipResponseV1 {},
            ),
        ),
    })
}

pub(crate) fn raft_write_applied_response(response: GroupWriteResponse) -> RaftGroupResponse {
    RaftGroupResponse(raft_app_proto::RaftGroupResponseV1 {
        response: Some(
            raft_app_proto::raft_group_response_v1::Response::WriteApplied(
                write_applied_response_to_proto(response),
            ),
        ),
    })
}

pub(crate) fn raft_write_rejected_response(err: GroupEngineError) -> RaftGroupResponse {
    RaftGroupResponse(raft_app_proto::RaftGroupResponseV1 {
        response: Some(
            raft_app_proto::raft_group_response_v1::Response::WriteRejected(
                group_engine_error_to_proto(err),
            ),
        ),
    })
}

pub(crate) fn write_applied_response_to_proto(
    response: GroupWriteResponse,
) -> raft_app_proto::WriteAppliedResponseV1 {
    use raft_app_proto::write_applied_response_v1::Response;
    let response = match response {
        GroupWriteResponse::CreateStream(response) => {
            Response::CreateStream(create_stream_response_to_proto(response))
        }
        GroupWriteResponse::Append(response) => {
            Response::Append(append_response_to_proto(response))
        }
        GroupWriteResponse::AppendBatch(response) => {
            Response::AppendBatch(append_batch_response_to_proto(response))
        }
        GroupWriteResponse::PublishSnapshot(response) => {
            Response::PublishSnapshot(publish_snapshot_response_to_proto(response))
        }
        GroupWriteResponse::TouchStreamAccess(response) => {
            Response::TouchStreamAccess(touch_stream_access_response_to_proto(response))
        }
        GroupWriteResponse::AddForkRef(response) => {
            Response::AddForkRef(fork_ref_response_to_proto(response))
        }
        GroupWriteResponse::ReleaseForkRef(response) => {
            Response::ReleaseForkRef(fork_ref_response_to_proto(response))
        }
        GroupWriteResponse::FlushCold(response) => {
            Response::FlushCold(flush_cold_response_to_proto(response))
        }
        GroupWriteResponse::CloseStream(response) => {
            Response::CloseStream(close_stream_response_to_proto(response))
        }
        GroupWriteResponse::DeleteStream(response) => {
            Response::DeleteStream(delete_stream_response_to_proto(response))
        }
        GroupWriteResponse::AckColdGc(response) => {
            Response::AckColdGc(ack_cold_gc_response_to_proto(response))
        }
        GroupWriteResponse::Batch(items) => Response::Batch(raft_app_proto::BatchResponseV1 {
            items: items
                .into_iter()
                .map(group_write_result_to_proto)
                .collect::<Vec<_>>(),
        }),
    };
    raft_app_proto::WriteAppliedResponseV1 {
        response: Some(response),
    }
}

pub(crate) fn create_stream_response_to_proto(
    response: CreateStreamResponse,
) -> raft_app_proto::CreateStreamResponseV1 {
    raft_app_proto::CreateStreamResponseV1 {
        placement: Some(placement_to_proto(response.placement)),
        next_offset: response.next_offset,
        closed: response.closed,
        already_exists: response.already_exists,
        group_commit_index: response.group_commit_index,
    }
}

pub(crate) fn append_response_to_proto(
    response: AppendResponse,
) -> raft_app_proto::AppendResponseV1 {
    raft_app_proto::AppendResponseV1 {
        placement: Some(placement_to_proto(response.placement)),
        start_offset: response.start_offset,
        next_offset: response.next_offset,
        stream_append_count: response.stream_append_count,
        group_commit_index: response.group_commit_index,
        closed: response.closed,
        deduplicated: response.deduplicated,
        producer: response.producer,
    }
}

pub(crate) fn append_batch_response_to_proto(
    response: GroupAppendBatchResponse,
) -> raft_app_proto::AppendBatchResponseV1 {
    raft_app_proto::AppendBatchResponseV1 {
        placement: Some(placement_to_proto(response.placement)),
        items: response
            .items
            .into_iter()
            .map(append_result_to_proto)
            .collect::<Vec<_>>(),
    }
}

pub(crate) fn publish_snapshot_response_to_proto(
    response: PublishSnapshotResponse,
) -> raft_app_proto::PublishSnapshotResponseV1 {
    raft_app_proto::PublishSnapshotResponseV1 {
        placement: Some(placement_to_proto(response.placement)),
        snapshot_offset: response.snapshot_offset,
        group_commit_index: response.group_commit_index,
    }
}

pub(crate) fn touch_stream_access_response_to_proto(
    response: TouchStreamAccessResponse,
) -> raft_app_proto::TouchStreamAccessResponseV1 {
    raft_app_proto::TouchStreamAccessResponseV1 {
        placement: Some(placement_to_proto(response.placement)),
        changed: response.changed,
        expired: response.expired,
        group_commit_index: response.group_commit_index,
    }
}

pub(crate) fn fork_ref_response_to_proto(
    response: ForkRefResponse,
) -> raft_app_proto::ForkRefResponseV1 {
    raft_app_proto::ForkRefResponseV1 {
        placement: Some(placement_to_proto(response.placement)),
        fork_ref_count: response.fork_ref_count,
        hard_deleted: response.hard_deleted,
        parent_to_release: response.parent_to_release.map(Into::into),
        group_commit_index: response.group_commit_index,
    }
}

pub(crate) fn flush_cold_response_to_proto(
    response: FlushColdResponse,
) -> raft_app_proto::FlushColdResponseV1 {
    raft_app_proto::FlushColdResponseV1 {
        placement: Some(placement_to_proto(response.placement)),
        hot_start_offset: response.hot_start_offset,
        group_commit_index: response.group_commit_index,
    }
}

pub(crate) fn close_stream_response_to_proto(
    response: CloseStreamResponse,
) -> raft_app_proto::CloseStreamResponseV1 {
    raft_app_proto::CloseStreamResponseV1 {
        placement: Some(placement_to_proto(response.placement)),
        next_offset: response.next_offset,
        group_commit_index: response.group_commit_index,
        deduplicated: response.deduplicated,
    }
}

pub(crate) fn delete_stream_response_to_proto(
    response: DeleteStreamResponse,
) -> raft_app_proto::DeleteStreamResponseV1 {
    raft_app_proto::DeleteStreamResponseV1 {
        placement: Some(placement_to_proto(response.placement)),
        group_commit_index: response.group_commit_index,
        hard_deleted: response.hard_deleted,
        parent_to_release: response.parent_to_release.map(Into::into),
    }
}

pub(crate) fn ack_cold_gc_response_to_proto(
    response: AckColdGcResponse,
) -> raft_app_proto::AckColdGcResponseV1 {
    raft_app_proto::AckColdGcResponseV1 {
        placement: Some(placement_to_proto(response.placement)),
        removed: response.removed,
        group_commit_index: response.group_commit_index,
    }
}

pub(crate) fn append_result_to_proto(
    result: Result<AppendResponse, GroupEngineError>,
) -> raft_app_proto::AppendResultV1 {
    let result = match result {
        Ok(response) => {
            raft_app_proto::append_result_v1::Result::Ok(append_response_to_proto(response))
        }
        Err(err) => raft_app_proto::append_result_v1::Result::Err(group_engine_error_to_proto(err)),
    };
    raft_app_proto::AppendResultV1 {
        result: Some(result),
    }
}

pub(crate) fn group_write_result_to_proto(
    result: Result<GroupWriteResponse, GroupEngineError>,
) -> raft_app_proto::GroupWriteResultV1 {
    let result = match result {
        Ok(response) => raft_app_proto::group_write_result_v1::Result::Ok(
            write_applied_response_to_proto(response),
        ),
        Err(err) => {
            raft_app_proto::group_write_result_v1::Result::Err(group_engine_error_to_proto(err))
        }
    };
    raft_app_proto::GroupWriteResultV1 {
        result: Some(result),
    }
}

pub(crate) fn group_write_result_from_raft_response(
    response: RaftGroupResponse,
) -> Result<Result<GroupWriteResponse, GroupEngineError>, GroupEngineError> {
    match required(response.0.response, "raft group response")? {
        raft_app_proto::raft_group_response_v1::Response::WriteApplied(response) => {
            Ok(Ok(group_write_response_from_proto(response)?))
        }
        raft_app_proto::raft_group_response_v1::Response::WriteRejected(err) => {
            Ok(Err(group_engine_error_from_proto(err)?))
        }
        other => Err(GroupEngineError::new(format!(
            "unexpected OpenRaft write response: {other:?}"
        ))),
    }
}

pub(crate) fn group_write_response_from_proto(
    response: raft_app_proto::WriteAppliedResponseV1,
) -> Result<GroupWriteResponse, GroupEngineError> {
    use raft_app_proto::write_applied_response_v1::Response;
    match required(response.response, "write_applied.response")? {
        Response::CreateStream(response) => Ok(GroupWriteResponse::CreateStream(
            create_stream_response_from_proto(response)?,
        )),
        Response::Append(response) => Ok(GroupWriteResponse::Append(append_response_from_proto(
            response,
        )?)),
        Response::AppendBatch(response) => Ok(GroupWriteResponse::AppendBatch(
            append_batch_response_from_proto(response)?,
        )),
        Response::PublishSnapshot(response) => Ok(GroupWriteResponse::PublishSnapshot(
            publish_snapshot_response_from_proto(response)?,
        )),
        Response::TouchStreamAccess(response) => Ok(GroupWriteResponse::TouchStreamAccess(
            touch_stream_access_response_from_proto(response)?,
        )),
        Response::AddForkRef(response) => Ok(GroupWriteResponse::AddForkRef(
            fork_ref_response_from_proto(response)?,
        )),
        Response::ReleaseForkRef(response) => Ok(GroupWriteResponse::ReleaseForkRef(
            fork_ref_response_from_proto(response)?,
        )),
        Response::FlushCold(response) => Ok(GroupWriteResponse::FlushCold(
            flush_cold_response_from_proto(response)?,
        )),
        Response::CloseStream(response) => Ok(GroupWriteResponse::CloseStream(
            close_stream_response_from_proto(response)?,
        )),
        Response::DeleteStream(response) => Ok(GroupWriteResponse::DeleteStream(
            delete_stream_response_from_proto(response)?,
        )),
        Response::AckColdGc(response) => Ok(GroupWriteResponse::AckColdGc(
            ack_cold_gc_response_from_proto(response)?,
        )),
        Response::Batch(response) => Ok(GroupWriteResponse::Batch(
            response
                .items
                .into_iter()
                .map(group_write_result_from_proto)
                .collect::<Result<Vec<_>, _>>()?,
        )),
    }
}

pub(crate) fn create_stream_response_from_proto(
    response: raft_app_proto::CreateStreamResponseV1,
) -> Result<CreateStreamResponse, GroupEngineError> {
    Ok(CreateStreamResponse {
        placement: placement_from_proto(response.placement, "create_stream_response.placement")?,
        next_offset: response.next_offset,
        closed: response.closed,
        already_exists: response.already_exists,
        group_commit_index: response.group_commit_index,
    })
}

pub(crate) fn append_response_from_proto(
    response: raft_app_proto::AppendResponseV1,
) -> Result<AppendResponse, GroupEngineError> {
    Ok(AppendResponse {
        placement: placement_from_proto(response.placement, "append_response.placement")?,
        start_offset: response.start_offset,
        next_offset: response.next_offset,
        stream_append_count: response.stream_append_count,
        group_commit_index: response.group_commit_index,
        closed: response.closed,
        deduplicated: response.deduplicated,
        producer: response.producer,
    })
}

pub(crate) fn append_batch_response_from_proto(
    response: raft_app_proto::AppendBatchResponseV1,
) -> Result<GroupAppendBatchResponse, GroupEngineError> {
    Ok(GroupAppendBatchResponse {
        placement: placement_from_proto(response.placement, "append_batch_response.placement")?,
        items: response
            .items
            .into_iter()
            .map(append_result_from_proto)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

pub(crate) fn publish_snapshot_response_from_proto(
    response: raft_app_proto::PublishSnapshotResponseV1,
) -> Result<PublishSnapshotResponse, GroupEngineError> {
    Ok(PublishSnapshotResponse {
        placement: placement_from_proto(response.placement, "publish_snapshot_response.placement")?,
        snapshot_offset: response.snapshot_offset,
        group_commit_index: response.group_commit_index,
    })
}

pub(crate) fn touch_stream_access_response_from_proto(
    response: raft_app_proto::TouchStreamAccessResponseV1,
) -> Result<TouchStreamAccessResponse, GroupEngineError> {
    Ok(TouchStreamAccessResponse {
        placement: placement_from_proto(
            response.placement,
            "touch_stream_access_response.placement",
        )?,
        changed: response.changed,
        expired: response.expired,
        group_commit_index: response.group_commit_index,
    })
}

pub(crate) fn fork_ref_response_from_proto(
    response: raft_app_proto::ForkRefResponseV1,
) -> Result<ForkRefResponse, GroupEngineError> {
    Ok(ForkRefResponse {
        placement: placement_from_proto(response.placement, "fork_ref_response.placement")?,
        fork_ref_count: response.fork_ref_count,
        hard_deleted: response.hard_deleted,
        parent_to_release: optional_stream_id_from_proto(response.parent_to_release)?,
        group_commit_index: response.group_commit_index,
    })
}

pub(crate) fn flush_cold_response_from_proto(
    response: raft_app_proto::FlushColdResponseV1,
) -> Result<FlushColdResponse, GroupEngineError> {
    Ok(FlushColdResponse {
        placement: placement_from_proto(response.placement, "flush_cold_response.placement")?,
        hot_start_offset: response.hot_start_offset,
        group_commit_index: response.group_commit_index,
    })
}

pub(crate) fn close_stream_response_from_proto(
    response: raft_app_proto::CloseStreamResponseV1,
) -> Result<CloseStreamResponse, GroupEngineError> {
    Ok(CloseStreamResponse {
        placement: placement_from_proto(response.placement, "close_stream_response.placement")?,
        next_offset: response.next_offset,
        group_commit_index: response.group_commit_index,
        deduplicated: response.deduplicated,
    })
}

pub(crate) fn head_stream_response_to_proto(
    response: HeadStreamResponse,
) -> raft_internal_proto::HeadStreamResponsePayloadV1 {
    raft_internal_proto::HeadStreamResponsePayloadV1 {
        core_id: u32::from(response.placement.core_id.0),
        shard_id: response.placement.shard_id.0,
        raft_group_id: response.placement.raft_group_id.0,
        content_type: response.content_type,
        tail_offset: response.tail_offset,
        closed: response.closed,
        stream_ttl_seconds: response.stream_ttl_seconds,
        stream_expires_at_ms: response.stream_expires_at_ms,
        snapshot_offset: response.snapshot_offset,
        integrity_live_setsum: response.integrity.live_setsum,
        integrity_evicted_setsum: response.integrity.evicted_setsum,
        integrity_total_setsum: response.integrity.total_setsum,
        integrity_live_start_offset: response.integrity.live_start_offset,
        integrity_live_records: response.integrity.live_records,
        integrity_evicted_records: response.integrity.evicted_records,
        integrity_total_records: response.integrity.total_records,
        cold_hot_start_offset: response.cold_hot_start_offset,
    }
}

pub(crate) fn head_stream_response_from_proto(
    response: raft_internal_proto::HeadStreamResponsePayloadV1,
) -> Result<HeadStreamResponse, GroupEngineError> {
    Ok(HeadStreamResponse {
        placement: placement_from_parts(
            response.core_id,
            response.shard_id,
            response.raft_group_id,
            "head_stream_response",
        )?,
        content_type: response.content_type,
        tail_offset: response.tail_offset,
        cold_hot_start_offset: response.cold_hot_start_offset,
        closed: response.closed,
        stream_ttl_seconds: response.stream_ttl_seconds,
        stream_expires_at_ms: response.stream_expires_at_ms,
        snapshot_offset: response.snapshot_offset,
        integrity: StreamIntegritySnapshot {
            live_setsum: response.integrity_live_setsum,
            evicted_setsum: response.integrity_evicted_setsum,
            total_setsum: response.integrity_total_setsum,
            live_start_offset: response.integrity_live_start_offset,
            tail_offset: response.tail_offset,
            live_records: response.integrity_live_records,
            evicted_records: response.integrity_evicted_records,
            total_records: response.integrity_total_records,
            records: Vec::new(),
        },
    })
}

pub(crate) fn read_stream_response_to_proto(
    response: ReadStreamResponse,
) -> raft_internal_proto::ReadStreamResponsePayloadV1 {
    raft_internal_proto::ReadStreamResponsePayloadV1 {
        core_id: u32::from(response.placement.core_id.0),
        shard_id: response.placement.shard_id.0,
        raft_group_id: response.placement.raft_group_id.0,
        offset: response.offset,
        next_offset: response.next_offset,
        content_type: response.content_type,
        payload: response.payload,
        up_to_date: response.up_to_date,
        closed: response.closed,
    }
}

pub(crate) fn read_stream_response_from_proto(
    response: raft_internal_proto::ReadStreamResponsePayloadV1,
) -> Result<ReadStreamResponse, GroupEngineError> {
    Ok(ReadStreamResponse {
        placement: placement_from_parts(
            response.core_id,
            response.shard_id,
            response.raft_group_id,
            "read_stream_response",
        )?,
        offset: response.offset,
        next_offset: response.next_offset,
        content_type: response.content_type,
        payload: response.payload,
        up_to_date: response.up_to_date,
        closed: response.closed,
    })
}

pub(crate) fn delete_stream_response_from_proto(
    response: raft_app_proto::DeleteStreamResponseV1,
) -> Result<DeleteStreamResponse, GroupEngineError> {
    Ok(DeleteStreamResponse {
        placement: placement_from_proto(response.placement, "delete_stream_response.placement")?,
        group_commit_index: response.group_commit_index,
        hard_deleted: response.hard_deleted,
        parent_to_release: optional_stream_id_from_proto(response.parent_to_release)?,
    })
}

pub(crate) fn ack_cold_gc_response_from_proto(
    response: raft_app_proto::AckColdGcResponseV1,
) -> Result<AckColdGcResponse, GroupEngineError> {
    Ok(AckColdGcResponse {
        placement: placement_from_proto(response.placement, "ack_cold_gc_response.placement")?,
        removed: response.removed,
        group_commit_index: response.group_commit_index,
    })
}

pub(crate) fn append_result_from_proto(
    result: raft_app_proto::AppendResultV1,
) -> Result<Result<AppendResponse, GroupEngineError>, GroupEngineError> {
    match required(result.result, "append_result.result")? {
        raft_app_proto::append_result_v1::Result::Ok(response) => {
            Ok(Ok(append_response_from_proto(response)?))
        }
        raft_app_proto::append_result_v1::Result::Err(err) => {
            Ok(Err(group_engine_error_from_proto(err)?))
        }
    }
}

pub(crate) fn group_write_result_from_proto(
    result: raft_app_proto::GroupWriteResultV1,
) -> Result<Result<GroupWriteResponse, GroupEngineError>, GroupEngineError> {
    match required(result.result, "group_write_result.result")? {
        raft_app_proto::group_write_result_v1::Result::Ok(response) => {
            Ok(Ok(group_write_response_from_proto(response)?))
        }
        raft_app_proto::group_write_result_v1::Result::Err(err) => {
            Ok(Err(group_engine_error_from_proto(err)?))
        }
    }
}

pub(crate) fn encode_group_write_result(
    result: Result<GroupWriteResponse, GroupEngineError>,
) -> raft_internal_proto::GroupWriteResultV1 {
    match result {
        Ok(response) => raft_internal_proto::GroupWriteResultV1 {
            ok: true,
            payload: write_applied_response_to_proto(response).encode_to_vec(),
        },
        Err(err) => raft_internal_proto::GroupWriteResultV1 {
            ok: false,
            payload: group_engine_error_to_proto(err).encode_to_vec(),
        },
    }
}

pub(crate) fn group_engine_error_to_proto(
    err: GroupEngineError,
) -> raft_app_proto::GroupEngineErrorV1 {
    raft_app_proto::GroupEngineErrorV1 {
        message: err.message().to_owned(),
        code: err
            .code()
            .map(stream_error_code_to_proto)
            .map(|code| code as i32),
        next_offset: err.next_offset(),
        leader_hint: err.leader_hint().cloned().map(group_leader_hint_to_proto),
    }
}

pub(crate) fn group_engine_error_from_proto(
    err: raft_app_proto::GroupEngineErrorV1,
) -> Result<GroupEngineError, GroupEngineError> {
    Ok(GroupEngineError::from_replicated_parts(
        err.message,
        err.code.map(stream_error_code_from_proto).transpose()?,
        err.next_offset,
        err.leader_hint.map(group_leader_hint_from_proto),
    ))
}

pub(crate) fn group_leader_hint_to_proto(
    hint: GroupLeaderHint,
) -> raft_app_proto::GroupLeaderHintV1 {
    raft_app_proto::GroupLeaderHintV1 {
        node_id: hint.node_id,
        address: hint.address,
    }
}

pub(crate) fn group_leader_hint_from_proto(
    hint: raft_app_proto::GroupLeaderHintV1,
) -> GroupLeaderHint {
    GroupLeaderHint {
        node_id: hint.node_id,
        address: hint.address,
    }
}

pub(crate) fn stream_error_code_to_proto(
    code: StreamErrorCode,
) -> raft_app_proto::StreamErrorCodeV1 {
    match code {
        StreamErrorCode::InvalidBucketId => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidBucketId
        }
        StreamErrorCode::InvalidStreamId => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidStreamId
        }
        StreamErrorCode::BucketNotFound => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeBucketNotFound
        }
        StreamErrorCode::BucketNotEmpty => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeBucketNotEmpty
        }
        StreamErrorCode::StreamNotFound => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeStreamNotFound
        }
        StreamErrorCode::StreamGone => raft_app_proto::StreamErrorCodeV1::StreamErrorCodeStreamGone,
        StreamErrorCode::StreamAlreadyExistsConflict => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeStreamAlreadyExistsConflict
        }
        StreamErrorCode::MissingContentType => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeMissingContentType
        }
        StreamErrorCode::ContentTypeMismatch => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeContentTypeMismatch
        }
        StreamErrorCode::EmptyAppend => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeEmptyAppend
        }
        StreamErrorCode::StreamClosed => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeStreamClosed
        }
        StreamErrorCode::StreamSeqConflict => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeStreamSeqConflict
        }
        StreamErrorCode::InvalidProducer => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidProducer
        }
        StreamErrorCode::ProducerEpochStale => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeProducerEpochStale
        }
        StreamErrorCode::ProducerSeqConflict => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeProducerSeqConflict
        }
        StreamErrorCode::InvalidRetention => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidRetention
        }
        StreamErrorCode::InvalidFork => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidFork
        }
        StreamErrorCode::OffsetOutOfRange => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeOffsetOutOfRange
        }
        StreamErrorCode::InvalidColdFlush => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidColdFlush
        }
        StreamErrorCode::InvalidSnapshot => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidSnapshot
        }
        StreamErrorCode::SnapshotNotFound => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeSnapshotNotFound
        }
        StreamErrorCode::SnapshotConflict => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeSnapshotConflict
        }
    }
}

pub(crate) fn stream_error_code_from_proto(code: i32) -> Result<StreamErrorCode, GroupEngineError> {
    let code = raft_app_proto::StreamErrorCodeV1::try_from(code)
        .map_err(|_| GroupEngineError::new(format!("unknown stream error code: {code}")))?;
    match code {
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeUnspecified => {
            Err(GroupEngineError::new("unspecified stream error code"))
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidBucketId => {
            Ok(StreamErrorCode::InvalidBucketId)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidStreamId => {
            Ok(StreamErrorCode::InvalidStreamId)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeBucketNotFound => {
            Ok(StreamErrorCode::BucketNotFound)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeBucketNotEmpty => {
            Ok(StreamErrorCode::BucketNotEmpty)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeStreamNotFound => {
            Ok(StreamErrorCode::StreamNotFound)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeStreamGone => {
            Ok(StreamErrorCode::StreamGone)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeStreamAlreadyExistsConflict => {
            Ok(StreamErrorCode::StreamAlreadyExistsConflict)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeMissingContentType => {
            Ok(StreamErrorCode::MissingContentType)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeContentTypeMismatch => {
            Ok(StreamErrorCode::ContentTypeMismatch)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeEmptyAppend => {
            Ok(StreamErrorCode::EmptyAppend)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeStreamClosed => {
            Ok(StreamErrorCode::StreamClosed)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeStreamSeqConflict => {
            Ok(StreamErrorCode::StreamSeqConflict)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidProducer => {
            Ok(StreamErrorCode::InvalidProducer)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeProducerEpochStale => {
            Ok(StreamErrorCode::ProducerEpochStale)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeProducerSeqConflict => {
            Ok(StreamErrorCode::ProducerSeqConflict)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidRetention => {
            Ok(StreamErrorCode::InvalidRetention)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidFork => {
            Ok(StreamErrorCode::InvalidFork)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeOffsetOutOfRange => {
            Ok(StreamErrorCode::OffsetOutOfRange)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidColdFlush => {
            Ok(StreamErrorCode::InvalidColdFlush)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidSnapshot => {
            Ok(StreamErrorCode::InvalidSnapshot)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeSnapshotNotFound => {
            Ok(StreamErrorCode::SnapshotNotFound)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeSnapshotConflict => {
            Ok(StreamErrorCode::SnapshotConflict)
        }
    }
}
