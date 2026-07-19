use prost::Message;
use ursula_proto as raft_app_proto;
use ursula_runtime::AckColdGcResponse;
use ursula_runtime::AppendResponse;
use ursula_runtime::CloseStreamResponse;
use ursula_runtime::CreateStreamResponse;
use ursula_runtime::DeleteStreamResponse;
use ursula_runtime::FlushColdResponse;
use ursula_runtime::GetStreamAttrsResponse;
use ursula_runtime::GroupAppendBatchResponse;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupInfraError;
use ursula_runtime::GroupLeaderHint;
use ursula_runtime::GroupWriteCommand;
use ursula_runtime::GroupWriteResponse;
use ursula_runtime::HeadStreamResponse;
use ursula_runtime::PublishSnapshotResponse;
use ursula_runtime::ReadStreamResponse;
use ursula_runtime::StreamAttrs;
use ursula_runtime::StreamErrorCode;
use ursula_runtime::StreamErrorContext;
use ursula_runtime::StreamIntegritySnapshot;
use ursula_runtime::TouchStreamAccessResponse;
use ursula_runtime::UpdateStreamAttrsResponse;
use ursula_shard::BucketStreamId;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;

use crate::raft_internal_proto;
use crate::types::RaftGroupCommand;
use crate::types::RaftGroupResponse;

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
            initial_payload: command.initial_payload,
            close_after: command.close_after,
            stream_seq: command.stream_seq,
            producer: command.producer,
            stream_ttl_seconds: command.stream_ttl_seconds,
            stream_expires_at_ms: command.stream_expires_at_ms,
            attrs: stream_attrs_from_proto(command.attrs_json, "create_stream.attrs_json")?,
            now_ms: command.now_ms,
        }),
        Command::CreateExternal(command) => Ok(GroupWriteCommand::CreateExternal {
            stream_id: stream_id_from_proto(command.stream_id, "create_external.stream_id")?,
            content_type: command.content_type,
            initial_payload: required(command.initial_payload, "create_external.initial_payload")?,
            record_ends: command.record_ends,
            close_after: command.close_after,
            stream_seq: command.stream_seq,
            producer: command.producer,
            stream_ttl_seconds: command.stream_ttl_seconds,
            stream_expires_at_ms: command.stream_expires_at_ms,
            attrs: stream_attrs_from_proto(command.attrs_json, "create_external.attrs_json")?,
            now_ms: command.now_ms,
        }),
        Command::Append(command) => Ok(GroupWriteCommand::Append {
            stream_id: stream_id_from_proto(command.stream_id, "append.stream_id")?,
            content_type: command.content_type,
            payload: command.payload,
            close_after: command.close_after,
            stream_seq: command.stream_seq,
            producer: command.producer,
            now_ms: command.now_ms,
            record_match: command.record_match,
        }),
        Command::AppendExternal(command) => Ok(GroupWriteCommand::AppendExternal {
            stream_id: stream_id_from_proto(command.stream_id, "append_external.stream_id")?,
            content_type: command.content_type,
            payload: required(command.payload, "append_external.payload")?,
            record_ends: command.record_ends,
            close_after: command.close_after,
            stream_seq: command.stream_seq,
            producer: command.producer,
            now_ms: command.now_ms,
            record_match: command.record_match,
        }),
        Command::AppendBatch(command) => Ok(GroupWriteCommand::AppendBatch {
            stream_id: stream_id_from_proto(command.stream_id, "append_batch.stream_id")?,
            content_type: command.content_type,
            payloads: command.payloads,
            producer: command.producer,
            now_ms: command.now_ms,
        }),
        Command::PublishSnapshot(command) => Ok(GroupWriteCommand::PublishSnapshot {
            stream_id: stream_id_from_proto(command.stream_id, "publish_snapshot.stream_id")?,
            snapshot_offset: command.snapshot_offset,
            content_type: command.content_type,
            payload: command.payload,
            now_ms: command.now_ms,
        }),
        Command::TouchStreamAccess(command) => Ok(GroupWriteCommand::TouchStreamAccess {
            stream_id: stream_id_from_proto(command.stream_id, "touch_stream_access.stream_id")?,
            now_ms: command.now_ms,
            renew_ttl: command.renew_ttl,
        }),
        Command::UpdateStreamAttrs(command) => Ok(GroupWriteCommand::UpdateStreamAttrs {
            stream_id: stream_id_from_proto(command.stream_id, "update_stream_attrs.stream_id")?,
            attrs: stream_attrs_from_proto(command.attrs_json, "update_stream_attrs.attrs_json")?,
            now_ms: command.now_ms,
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

fn stream_attrs_from_proto(
    attrs_json: Option<bytes::Bytes>,
    field: &str,
) -> Result<Option<StreamAttrs>, GroupEngineError> {
    let Some(attrs_json) = attrs_json.filter(|bytes| !bytes.is_empty()) else {
        return Ok(None);
    };
    serde_json::from_slice(&attrs_json)
        .map(Some)
        .map_err(|err| GroupEngineError::new(format!("{field} contains invalid JSON: {err}")))
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
    value.ok_or_else(|| GroupEngineError::Infra(GroupInfraError::proto_decode(field)))
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
        GroupWriteResponse::UpdateStreamAttrs(response) => {
            Response::UpdateStreamAttrs(update_stream_attrs_response_to_proto(response))
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
        record_start: response.record_range.map(|range| range.first_record),
        record_next: response.record_range.map(|range| range.next_record),
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
        record_start: response.record_range.map(|range| range.first_record),
        record_next: response.record_range.map(|range| range.next_record),
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
        record_start: response.record_range.map(|range| range.first_record),
        record_next: response.record_range.map(|range| range.next_record),
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

pub(crate) fn update_stream_attrs_response_to_proto(
    response: UpdateStreamAttrsResponse,
) -> raft_app_proto::UpdateStreamAttrsResponseV1 {
    raft_app_proto::UpdateStreamAttrsResponseV1 {
        placement: Some(placement_to_proto(response.placement)),
        changed: response.changed,
        group_commit_index: response.group_commit_index,
    }
}

pub(crate) fn get_stream_attrs_response_to_proto(
    response: GetStreamAttrsResponse,
) -> raft_internal_proto::GetStreamAttrsResponsePayloadV1 {
    raft_internal_proto::GetStreamAttrsResponsePayloadV1 {
        core_id: u32::from(response.placement.core_id.0),
        shard_id: response.placement.shard_id.0,
        raft_group_id: response.placement.raft_group_id.0,
        attrs_json: response.attrs.map(|attrs| {
            serde_json::to_vec(&attrs)
                .expect("stream attrs serialize to JSON")
                .into()
        }),
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
        record_start: response.record_range.map(|range| range.first_record),
        record_next: response.record_range.map(|range| range.next_record),
    }
}

pub(crate) fn delete_stream_response_to_proto(
    response: DeleteStreamResponse,
) -> raft_app_proto::DeleteStreamResponseV1 {
    raft_app_proto::DeleteStreamResponseV1 {
        placement: Some(placement_to_proto(response.placement)),
        group_commit_index: response.group_commit_index,
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
        Response::UpdateStreamAttrs(response) => Ok(GroupWriteResponse::UpdateStreamAttrs(
            update_stream_attrs_response_from_proto(response)?,
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
        record_range: record_range(response.record_start, response.record_next)?,
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
        record_range: record_range(response.record_start, response.record_next)?,
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
        record_range: record_range(response.record_start, response.record_next)?,
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

pub(crate) fn update_stream_attrs_response_from_proto(
    response: raft_app_proto::UpdateStreamAttrsResponseV1,
) -> Result<UpdateStreamAttrsResponse, GroupEngineError> {
    Ok(UpdateStreamAttrsResponse {
        placement: placement_from_proto(
            response.placement,
            "update_stream_attrs_response.placement",
        )?,
        changed: response.changed,
        group_commit_index: response.group_commit_index,
    })
}

pub(crate) fn get_stream_attrs_response_from_proto(
    response: raft_internal_proto::GetStreamAttrsResponsePayloadV1,
) -> Result<GetStreamAttrsResponse, GroupEngineError> {
    Ok(GetStreamAttrsResponse {
        placement: placement_from_parts(
            response.core_id,
            response.shard_id,
            response.raft_group_id,
            "get_stream_attrs_response",
        )?,
        attrs: stream_attrs_from_proto(
            response.attrs_json,
            "get_stream_attrs_response.attrs_json",
        )?,
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
        record_range: record_range(response.record_start, response.record_next)?,
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
        record_first: response.record_range.map(|range| range.first_record),
        record_next: response.record_range.map(|range| range.next_record),
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
        },
        record_range: record_range(response.record_first, response.record_next)?,
    })
}

fn record_range(
    first_record: Option<u64>,
    next_record: Option<u64>,
) -> Result<Option<ursula_stream::StreamRecordRange>, GroupEngineError> {
    match (first_record, next_record) {
        (Some(first_record), Some(next_record)) if first_record <= next_record => {
            Ok(Some(ursula_stream::StreamRecordRange {
                first_record,
                next_record,
            }))
        }
        (None, None) => Ok(None),
        _ => Err(GroupEngineError::new("incomplete record range")),
    }
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
        payload: response.payload.into(),
        up_to_date: response.up_to_date,
        closed: response.closed,
        retained_record_first: response
            .retained_record_range
            .map(|range| range.first_record),
        retained_record_next: response
            .retained_record_range
            .map(|range| range.next_record),
        record_start: response.record_range.map(|range| range.first_record),
        record_next: response.record_range.map(|range| range.next_record),
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
        payload: response.payload.to_vec(),
        up_to_date: response.up_to_date,
        closed: response.closed,
        retained_record_range: record_range(
            response.retained_record_first,
            response.retained_record_next,
        )?,
        record_range: record_range(response.record_start, response.record_next)?,
    })
}

pub(crate) fn delete_stream_response_from_proto(
    response: raft_app_proto::DeleteStreamResponseV1,
) -> Result<DeleteStreamResponse, GroupEngineError> {
    Ok(DeleteStreamResponse {
        placement: placement_from_proto(response.placement, "delete_stream_response.placement")?,
        group_commit_index: response.group_commit_index,
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
            payload: write_applied_response_to_proto(response)
                .encode_to_vec()
                .into(),
        },
        Err(err) => raft_internal_proto::GroupWriteResultV1 {
            ok: false,
            payload: group_engine_error_to_proto(err).encode_to_vec().into(),
        },
    }
}

pub(crate) fn group_engine_error_to_proto(
    err: GroupEngineError,
) -> raft_app_proto::GroupEngineErrorV1 {
    use raft_app_proto::group_engine_error_v1::Error;

    let error = match &err {
        GroupEngineError::Stream(_) => {
            let (message, code, next_offset, context) = err
                .stream_parts()
                .expect("GroupEngineError::stream_parts returns Some for stream errors");
            Error::Stream(raft_app_proto::StreamEngineErrorV1 {
                message: message.to_owned(),
                code: stream_error_code_to_proto(code) as i32,
                next_offset,
                context: context
                    .iter()
                    .cloned()
                    .map(stream_error_context_to_proto)
                    .collect(),
            })
        }
        GroupEngineError::Infra(infra) => Error::Infra(group_infra_error_to_proto(infra)),
        GroupEngineError::ForwardToLeader {
            message,
            leader_hint,
        } => Error::ForwardToLeader(raft_app_proto::ForwardToLeaderErrorV1 {
            message: message.clone(),
            leader_hint: Some(group_leader_hint_to_proto(leader_hint.clone())),
        }),
    };

    raft_app_proto::GroupEngineErrorV1 { error: Some(error) }
}

pub(crate) fn group_engine_error_from_proto(
    err: raft_app_proto::GroupEngineErrorV1,
) -> Result<GroupEngineError, GroupEngineError> {
    use raft_app_proto::group_engine_error_v1::Error;

    match required(err.error, "group_engine_error.error")? {
        Error::Stream(err) => Ok(GroupEngineError::stream_from_replicated(
            err.message,
            stream_error_code_from_proto(err.code)?,
            err.next_offset,
            err.context
                .into_iter()
                .map(stream_error_context_from_proto)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Error::Infra(err) => Ok(GroupEngineError::Infra(group_infra_error_from_proto(err)?)),
        Error::ForwardToLeader(err) => Ok(GroupEngineError::ForwardToLeader {
            message: err.message,
            leader_hint: group_leader_hint_from_proto(required(
                err.leader_hint,
                "group_engine_error.forward_to_leader.leader_hint",
            )?),
        }),
    }
}

pub(crate) fn group_infra_error_to_proto(
    err: &GroupInfraError,
) -> raft_app_proto::GroupInfraErrorV1 {
    use raft_app_proto::group_infra_error_v1::Error;

    let error = match err {
        GroupInfraError::Internal { message } => {
            Error::Internal(raft_app_proto::InternalGroupInfraErrorV1 {
                message: message.clone(),
            })
        }
        GroupInfraError::ProtoDecode { field } => {
            Error::ProtoDecode(raft_app_proto::ProtoDecodeErrorV1 {
                field: field.clone(),
            })
        }
        GroupInfraError::ColdBackpressure {
            stream_id,
            before_group_hot_bytes,
            after_group_hot_bytes,
            limit,
        } => Error::ColdBackpressure(raft_app_proto::ColdBackpressureErrorV1 {
            stream_id: Some(stream_id.into()),
            before_group_hot_bytes: *before_group_hot_bytes,
            after_group_hot_bytes: *after_group_hot_bytes,
            limit: *limit,
        }),
        GroupInfraError::RaftUncommittedBackpressure {
            current,
            incoming,
            limit,
        } => {
            Error::RaftUncommittedBackpressure(raft_app_proto::RaftUncommittedBackpressureErrorV1 {
                current: *current,
                incoming: *incoming,
                limit: *limit,
            })
        }
    };

    raft_app_proto::GroupInfraErrorV1 { error: Some(error) }
}

pub(crate) fn group_infra_error_from_proto(
    err: raft_app_proto::GroupInfraErrorV1,
) -> Result<GroupInfraError, GroupEngineError> {
    use raft_app_proto::group_infra_error_v1::Error;

    match required(err.error, "group_infra_error.error")? {
        Error::Internal(err) => Ok(GroupInfraError::Internal {
            message: err.message,
        }),
        Error::ProtoDecode(err) => Ok(GroupInfraError::ProtoDecode { field: err.field }),
        Error::ColdBackpressure(err) => Ok(GroupInfraError::ColdBackpressure {
            stream_id: stream_id_from_proto(
                err.stream_id,
                "group_infra_error.cold_backpressure.stream_id",
            )?,
            before_group_hot_bytes: err.before_group_hot_bytes,
            after_group_hot_bytes: err.after_group_hot_bytes,
            limit: err.limit,
        }),
        Error::RaftUncommittedBackpressure(err) => {
            Ok(GroupInfraError::RaftUncommittedBackpressure {
                current: err.current,
                incoming: err.incoming,
                limit: err.limit,
            })
        }
    }
}

pub(crate) fn stream_error_context_to_proto(
    context: StreamErrorContext,
) -> raft_app_proto::StreamErrorContextV1 {
    use raft_app_proto::stream_error_context_v1::Context;
    let context = match context {
        StreamErrorContext::StreamClosed => {
            Context::StreamClosed(raft_app_proto::BlankResponseV1 {})
        }
        StreamErrorContext::StaleColdFlushCandidate => {
            Context::StaleColdFlushCandidate(raft_app_proto::BlankResponseV1 {})
        }
        StreamErrorContext::ProducerEpochStale { current_epoch } => {
            Context::ProducerCurrentEpoch(current_epoch)
        }
        StreamErrorContext::ProducerSeqConflict {
            expected_seq,
            received_seq,
        } => Context::ProducerSeqConflict(raft_app_proto::ProducerSeqConflictContextV1 {
            expected_seq,
            received_seq,
        }),
        StreamErrorContext::RecordTailMismatch { current_record } => {
            Context::RecordCurrentTail(current_record)
        }
    };
    raft_app_proto::StreamErrorContextV1 {
        context: Some(context),
    }
}

pub(crate) fn stream_error_context_from_proto(
    context: raft_app_proto::StreamErrorContextV1,
) -> Result<StreamErrorContext, GroupEngineError> {
    use raft_app_proto::stream_error_context_v1::Context;
    match context.context {
        Some(Context::StreamClosed(_)) => Ok(StreamErrorContext::StreamClosed),
        Some(Context::StaleColdFlushCandidate(_)) => {
            Ok(StreamErrorContext::StaleColdFlushCandidate)
        }
        Some(Context::ProducerCurrentEpoch(current_epoch)) => {
            Ok(StreamErrorContext::ProducerEpochStale { current_epoch })
        }
        Some(Context::ProducerSeqConflict(context)) => {
            Ok(StreamErrorContext::ProducerSeqConflict {
                expected_seq: context.expected_seq,
                received_seq: context.received_seq,
            })
        }
        Some(Context::RecordCurrentTail(current_record)) => {
            Ok(StreamErrorContext::RecordTailMismatch { current_record })
        }
        None => Err(GroupEngineError::new(
            "protobuf group engine error: missing context",
        )),
    }
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
        StreamErrorCode::InvalidStreamAttrs => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidStreamAttrs
        }
        StreamErrorCode::InvalidRecordBoundaries => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidRecordBoundaries
        }
        StreamErrorCode::RecordPreconditionFailed => {
            raft_app_proto::StreamErrorCodeV1::StreamErrorCodeRecordPreconditionFailed
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
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidStreamAttrs => {
            Ok(StreamErrorCode::InvalidStreamAttrs)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeInvalidRecordBoundaries => {
            Ok(StreamErrorCode::InvalidRecordBoundaries)
        }
        raft_app_proto::StreamErrorCodeV1::StreamErrorCodeRecordPreconditionFailed => {
            Ok(StreamErrorCode::RecordPreconditionFailed)
        }
    }
}
