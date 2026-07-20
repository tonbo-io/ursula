use std::collections::BTreeMap;
use std::sync::Mutex;

use futures_util::TryStreamExt;
use openraft::BasicNode;
use openraft::Raft;
use openraft::rt::WatchReceiver;
use serde::de::DeserializeOwned;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use ursula_runtime::GetStreamAttrsRequest;
use ursula_runtime::GetStreamAttrsResponse;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupEngineMetrics;
use ursula_runtime::GroupWriteCommand;
use ursula_runtime::GroupWriteResponse;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::HeadStreamResponse;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::ReadStreamResponse;
use ursula_shard::BucketStreamId;
use ursula_shard::ShardPlacement;

use crate::codec::decode_wire;
use crate::grpc::GRPC_LEADER_CHANNELS;
use crate::grpc::RAFT_GRPC_MAX_MESSAGE_BYTES;
use crate::grpc::RaftClient;
use crate::log_store::elapsed_ns;
use crate::raft_internal_proto;
use crate::rt::time::Instant;
use crate::state_machine::RaftGroupStateMachine;
use crate::types::RaftGroupResponse;
use crate::types::UrsulaRaftTypeConfig;

#[tracing::instrument(
    name = "raft.forward_head",
    skip_all,
    fields(group = placement.raft_group_id.0, bucket = %request.stream_id.bucket_id, stream = %request.stream_id.stream_id),
)]
pub(crate) async fn forward_head_stream_to_leader(
    placement: ShardPlacement,
    leader_node: &BasicNode,
    request: HeadStreamRequest,
) -> Result<HeadStreamResponse, GroupEngineError> {
    forward_typed_read_to_leader(
        placement,
        leader_node,
        request.stream_id,
        request.now_ms,
        "head",
        raft_internal_proto::group_read_request_v1::Read::Head(
            raft_internal_proto::HeadStreamReadV1 {},
        ),
    )
    .await
}

#[tracing::instrument(
    name = "raft.forward_read",
    skip_all,
    fields(group = placement.raft_group_id.0, bucket = %request.stream_id.bucket_id, stream = %request.stream_id.stream_id, offset = request.offset),
)]
pub(crate) async fn forward_read_stream_to_leader(
    placement: ShardPlacement,
    leader_node: &BasicNode,
    request: ReadStreamRequest,
) -> Result<ReadStreamResponse, GroupEngineError> {
    let max_len = u64::try_from(request.max_len)
        .map_err(|_| GroupEngineError::new("read max_len does not fit u64"))?;
    forward_typed_read_to_leader(
        placement,
        leader_node,
        request.stream_id,
        request.now_ms,
        "read",
        raft_internal_proto::group_read_request_v1::Read::ReadStream(
            raft_internal_proto::ReadStreamReadV1 {
                offset: request.offset,
                max_len,
                record: request.record,
                max_records: request.max_records,
            },
        ),
    )
    .await
}

#[tracing::instrument(
    name = "raft.forward_get_attrs",
    skip_all,
    fields(group = placement.raft_group_id.0, bucket = %request.stream_id.bucket_id, stream = %request.stream_id.stream_id),
)]
pub(crate) async fn forward_get_stream_attrs_to_leader(
    placement: ShardPlacement,
    leader_node: &BasicNode,
    request: GetStreamAttrsRequest,
) -> Result<GetStreamAttrsResponse, GroupEngineError> {
    forward_typed_read_to_leader(
        placement,
        leader_node,
        request.stream_id,
        request.now_ms,
        "get stream attrs",
        raft_internal_proto::group_read_request_v1::Read::GetStreamAttrs(
            raft_internal_proto::GetStreamAttrsReadV1 {},
        ),
    )
    .await
}

/// Forward one leader-only read to the leader over gRPC and decode the
/// serde-carried response payload into the engine-level response (`T`). The
/// three public forwarders above differ only in the read variant they
/// construct and the decode target, so they all funnel through here.
async fn forward_typed_read_to_leader<T>(
    placement: ShardPlacement,
    leader_node: &BasicNode,
    stream_id: BucketStreamId,
    now_ms: u64,
    what: &str,
    read: raft_internal_proto::group_read_request_v1::Read,
) -> Result<T, GroupEngineError>
where
    T: DeserializeOwned,
{
    let response =
        forward_group_read_to_leader(placement, leader_node, stream_id, now_ms, read).await?;
    if response.ok {
        decode_wire(&response.payload, what)
    } else {
        Err(decode_wire::<GroupEngineError>(&response.payload, what)?)
    }
}

pub(crate) async fn forward_group_read_to_leader(
    placement: ShardPlacement,
    leader_node: &BasicNode,
    stream_id: BucketStreamId,
    now_ms: u64,
    read: raft_internal_proto::group_read_request_v1::Read,
) -> Result<raft_internal_proto::GroupReadResponseV1, GroupEngineError> {
    let channel = grpc_leader_channel(&leader_node.addr).await?;
    let mut client = RaftClient::new(channel)
        .max_decoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES)
        .max_encoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES);
    let mut grpc_request = tonic::Request::new(raft_internal_proto::GroupReadRequestV1 {
        raft_group_id: placement.raft_group_id.0,
        core_id: u32::from(placement.core_id.0),
        shard_id: placement.shard_id.0,
        bucket_id: stream_id.bucket_id,
        stream_id: stream_id.stream_id,
        now_ms,
        read: Some(read),
    });
    // Carry this request's trace context to the leader so the forwarded read
    // joins the originating trace. No-op when no propagator is installed.
    crate::telemetry::inject_current_context(grpc_request.metadata_mut());
    client
        .group_read(grpc_request)
        .await
        .map(|response| response.into_inner())
        .map_err(|err| GroupEngineError::new(format!("forward group read to leader: {err}")))
}

pub(crate) async fn grpc_leader_channel(addr: &str) -> Result<Channel, GroupEngineError> {
    let cache = GRPC_LEADER_CHANNELS.get_or_init(|| Mutex::new(BTreeMap::new()));
    if let Some(channel) = cache
        .lock()
        .map_err(|_| GroupEngineError::new("gRPC leader channel cache mutex poisoned"))?
        .get(addr)
        .cloned()
    {
        return Ok(channel);
    }
    let endpoint = Endpoint::from_shared(addr.to_owned())
        .map_err(|err| GroupEngineError::new(format!("invalid gRPC leader endpoint: {err}")))?;
    let channel = endpoint
        .connect()
        .await
        .map_err(|err| GroupEngineError::new(format!("connect gRPC leader: {err}")))?;
    cache
        .lock()
        .map_err(|_| GroupEngineError::new("gRPC leader channel cache mutex poisoned"))?
        .insert(addr.to_owned(), channel.clone());
    Ok(channel)
}

pub(crate) async fn write_commands_on_raft(
    raft: Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>,
    placement: ShardPlacement,
    metrics: Option<GroupEngineMetrics>,
    commands: Vec<GroupWriteCommand>,
) -> Result<Vec<Result<GroupWriteResponse, GroupEngineError>>, GroupEngineError> {
    if commands.is_empty() {
        return Ok(Vec::new());
    }
    let expected_responses = commands.len();
    let logical_command_count = commands
        .iter()
        .map(logical_group_write_command_count)
        .sum::<usize>();
    let submit_started_at = Instant::now();
    let mut stream = match raft.client_write_many(commands).await {
        Ok(stream) => stream,
        Err(err) => {
            if let Some(metrics) = &metrics {
                metrics.record_raft_write_many(
                    placement,
                    expected_responses,
                    logical_command_count,
                    0,
                    elapsed_ns(submit_started_at),
                    0,
                );
            }
            return Err(GroupEngineError::new(format!(
                "OpenRaft client_write_many: {err}"
            )));
        }
    };
    let submit_ns = elapsed_ns(submit_started_at);
    let response_started_at = Instant::now();
    let mut responses = Vec::with_capacity(expected_responses);
    loop {
        let result = match stream.try_next().await {
            Ok(Some(result)) => result,
            Ok(None) => break,
            Err(err) => {
                if let Some(metrics) = &metrics {
                    metrics.record_raft_write_many(
                        placement,
                        expected_responses,
                        logical_command_count,
                        responses.len(),
                        submit_ns,
                        elapsed_ns(response_started_at),
                    );
                }
                return Err(GroupEngineError::new(format!(
                    "OpenRaft client_write_many response stream: {err}"
                )));
            }
        };
        let response = match result {
            Ok(response) => write_result_from_raft_response(response.response)?,
            Err(err) => Err(group_engine_forward_to_leader_error(
                format!("OpenRaft client_write_many forwarded to leader: {err}"),
                err.leader_id,
                err.leader_node.as_ref(),
                raft.metrics().borrow_watched().id,
            )),
        };
        responses.push(response);
    }
    if let Some(metrics) = &metrics {
        metrics.record_raft_write_many(
            placement,
            expected_responses,
            logical_command_count,
            responses.len(),
            submit_ns,
            elapsed_ns(response_started_at),
        );
    }
    if responses.len() != expected_responses {
        return Err(GroupEngineError::new(format!(
            "OpenRaft client_write_many returned {} responses for {} commands",
            responses.len(),
            expected_responses
        )));
    }
    Ok(responses)
}

pub(crate) fn logical_group_write_command_count(command: &GroupWriteCommand) -> usize {
    match command {
        GroupWriteCommand::Batch { commands } => commands.len(),
        GroupWriteCommand::Stream(_) => 1,
    }
}

/// Unwraps a raft-applied response into the write outcome it carries.
pub(crate) fn write_result_from_raft_response(
    response: RaftGroupResponse,
) -> Result<Result<GroupWriteResponse, GroupEngineError>, GroupEngineError> {
    match response {
        RaftGroupResponse::Write(result) => Ok(result),
        other @ (RaftGroupResponse::Blank | RaftGroupResponse::Membership) => Err(
            GroupEngineError::new(format!("unexpected OpenRaft write response: {other:?}")),
        ),
    }
}

pub(crate) fn group_engine_client_write_error(
    err: openraft::error::RaftError<
        UrsulaRaftTypeConfig,
        openraft::error::ClientWriteError<UrsulaRaftTypeConfig>,
    >,
    self_id: u64,
) -> GroupEngineError {
    if let Some(forward) = err.forward_to_leader() {
        return group_engine_forward_to_leader_error(
            format!("OpenRaft client_write forwarded to leader: {err}"),
            forward.leader_id,
            forward.leader_node.as_ref(),
            self_id,
        );
    }
    GroupEngineError::new(format!("OpenRaft client_write: {err}"))
}

pub(crate) fn group_engine_forward_to_leader_error(
    message: impl Into<String>,
    leader_id: Option<u64>,
    leader_node: Option<&BasicNode>,
    self_id: u64,
) -> GroupEngineError {
    // The write bounced because this node is not the leader. If the reported
    // leader is *this* node, leadership is in a transient step-down/election
    // window: redirecting the client back to ourselves would just loop, so
    // report leader-unknown and let the HTTP layer answer with a retryable 503.
    if leader_id == Some(self_id) {
        return GroupEngineError::forward_to_leader(message, None, None);
    }
    GroupEngineError::forward_to_leader(
        message,
        leader_id,
        leader_node.map(|node| node.addr.clone()),
    )
}
