use std::collections::BTreeMap;
use std::sync::Mutex;

use futures_util::TryStreamExt;
use openraft::BasicNode;
use openraft::Raft;
use openraft::rt::WatchReceiver;
use serde::de::DeserializeOwned;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupWriteCommand;
use ursula_runtime::GroupWriteResponse;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::HeadStreamResponse;
use ursula_runtime::PurgeBucketResponse;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::ReadStreamResponse;
use ursula_shard::BucketStreamId;
use ursula_shard::ShardPlacement;

use crate::codec::decode_wire;
use crate::codec::encode_wire;
use crate::grpc::GRPC_LEADER_CHANNELS;
use crate::grpc::RAFT_GRPC_MAX_MESSAGE_BYTES;
use crate::grpc::RAFT_GRPC_PROTOCOL_VERSION;
use crate::grpc::RaftClient;
use crate::raft_internal_proto;
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
    let head = head_stream_read_v1(&request);
    forward_typed_read_to_leader(
        placement,
        leader_node,
        request.stream_id,
        request.now_ms,
        "head",
        raft_internal_proto::group_read_request_v1::Read::Head(head),
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
    let read = read_stream_read_v1(&request)?;
    forward_typed_read_to_leader(
        placement,
        leader_node,
        request.stream_id,
        request.now_ms,
        "read",
        raft_internal_proto::group_read_request_v1::Read::ReadStream(read),
    )
    .await
}

/// The wire form of a forwarded HEAD: whether the leader linearizes it.
pub(crate) fn head_stream_read_v1(
    request: &HeadStreamRequest,
) -> raft_internal_proto::HeadStreamReadV1 {
    raft_internal_proto::HeadStreamReadV1 {
        applied_state_only: !request.linearizable,
    }
}

/// The wire form of a forwarded read.
pub(crate) fn read_stream_read_v1(
    request: &ReadStreamRequest,
) -> Result<raft_internal_proto::ReadStreamReadV1, GroupEngineError> {
    let max_len = u64::try_from(request.max_len)
        .map_err(|_| GroupEngineError::new("read max_len does not fit u64"))?;
    Ok(raft_internal_proto::ReadStreamReadV1 {
        offset: request.offset,
        max_len,
        leader_only: request.leader_only,
    })
}

/// Forward one leader-only read to the leader over gRPC and decode the
/// serde-carried response payload into the engine-level response (`T`). The
/// two public forwarders above differ only in the read variant they
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
        protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
    });
    // Carry this request's trace context to the leader so the forwarded read
    // joins the originating trace. No-op when no propagator is installed.
    crate::telemetry::inject_current_context(grpc_request.metadata_mut());
    client
        .group_read(grpc_request)
        .await
        .map(|response| response.into_inner())
        .map_err(|err| {
            crate::format_epoch::observe_outbound_status("GroupRead", &err);
            GroupEngineError::new(format!("forward group read to leader: {err}"))
        })
}

/// Forward the cluster-wide administrative bucket purge to the known
/// Raft-group leader. Ordinary client writes intentionally do not use this
/// path: their admission checks must run on the serving leader before
/// replication. PurgeBucket has no per-stream admission step, and constraining
/// this helper to that exact command keeps the exception narrow.
pub(crate) async fn forward_purge_bucket_to_leader(
    placement: ShardPlacement,
    leader_node: &BasicNode,
    bucket_id: String,
) -> Result<PurgeBucketResponse, GroupEngineError> {
    let channel = grpc_leader_channel(&leader_node.addr).await?;
    let mut client = RaftClient::new(channel)
        .max_decoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES)
        .max_encoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES);
    let mut grpc_request = tonic::Request::new(raft_internal_proto::GroupWriteRequestV1 {
        raft_group_id: placement.raft_group_id.0,
        core_id: u32::from(placement.core_id.0),
        shard_id: placement.shard_id.0,
        command_payloads: vec![encode_wire(&GroupWriteCommand::Stream(
            ursula_stream::StreamCommand::PurgeBucket { bucket_id },
        ))],
        protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
    });
    crate::telemetry::inject_current_context(grpc_request.metadata_mut());
    let response = client
        .group_write(grpc_request)
        .await
        .map_err(|err| {
            crate::format_epoch::observe_outbound_status("GroupWrite", &err);
            GroupEngineError::new(format!("forward group write to leader: {err}"))
        })?
        .into_inner();
    let mut results = response.results.into_iter();
    let result = results
        .next()
        .ok_or_else(|| GroupEngineError::new("forward group write returned no result"))?;
    if results.next().is_some() {
        return Err(GroupEngineError::new(
            "forward group write returned more than one result",
        ));
    }
    if result.ok {
        match decode_wire(&result.payload, "bucket purge response")? {
            GroupWriteResponse::PurgeBucket(response) => Ok(response),
            other => Err(GroupEngineError::new(format!(
                "unexpected forwarded bucket purge response: {other:?}"
            ))),
        }
    } else {
        Err(decode_wire::<GroupEngineError>(
            &result.payload,
            "bucket purge error",
        )?)
    }
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
    commands: Vec<GroupWriteCommand>,
) -> Result<Vec<Result<GroupWriteResponse, GroupEngineError>>, GroupEngineError> {
    if commands.is_empty() {
        return Ok(Vec::new());
    }
    let expected_responses = commands.len();
    let mut stream = raft
        .client_write_many(commands)
        .await
        .map_err(|err| GroupEngineError::new(format!("OpenRaft client_write_many: {err}")))?;
    let mut responses = Vec::with_capacity(expected_responses);
    while let Some(result) = stream.try_next().await.map_err(|err| {
        GroupEngineError::new(format!("OpenRaft client_write_many response stream: {err}"))
    })? {
        let response = match result {
            Ok(response) => write_result_from_raft_response(response.response)?,
            Err(err) => Err(group_engine_forward_to_leader_error(
                format!("OpenRaft client_write_many forwarded to leader: {err}"),
                err.leader_id,
                err.leader_node.as_ref(),
                raft.metrics().borrow_watched().id,
                false,
            )),
        };
        responses.push(response);
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
            false,
        );
    }
    GroupEngineError::new(format!("OpenRaft client_write: {err}"))
}

/// Map a failed ReadIndex barrier (`Raft::get_read_linearizer`). Nothing was
/// proposed. A lost leadership forwards to the known leader (503 while it is
/// unknown), and a leader that cannot reach a quorum answers leader-unknown
/// (503) rather than serving a view that may miss a newer leader's
/// acknowledged writes. A fatal error stays internal.
///
/// openraft's error text names internal node addresses (the quorum error
/// lists every member), so the client sees a fixed message and the full
/// error is logged at debug level only.
pub(crate) fn group_engine_linearizable_read_error(
    err: openraft::error::RaftError<
        UrsulaRaftTypeConfig,
        openraft::error::LinearizableReadError<UrsulaRaftTypeConfig>,
    >,
    operation: &str,
    self_id: u64,
) -> GroupEngineError {
    tracing::debug!("OpenRaft {operation} could not confirm leadership: {err}");
    match err.api_error() {
        Some(openraft::error::LinearizableReadError::ForwardToLeader(forward)) => {
            group_engine_forward_to_leader_error(
                format!("OpenRaft {operation} has to forward request to leader"),
                forward.leader_id,
                forward.leader_node.as_ref(),
                self_id,
                true,
            )
        }
        Some(openraft::error::LinearizableReadError::QuorumNotEnough(_)) => {
            group_engine_leader_read_unavailable(
                format!("OpenRaft {operation} could not confirm leadership with a quorum"),
                self_id,
            )
        }
        None => GroupEngineError::new(format!(
            "OpenRaft {operation} could not confirm leadership: {err}"
        )),
    }
}

/// The leader-unknown answer (503 with Retry-After) for a leader read that
/// could not be linearized in time: no quorum confirmed leadership, or the
/// local state machine did not reach the read index. `message` (the 503
/// body) names which, without internal addresses.
pub(crate) fn group_engine_leader_read_unavailable(
    message: String,
    self_id: u64,
) -> GroupEngineError {
    group_engine_forward_to_leader_error(message, None, None, self_id, true)
}

/// A memory-WAL group could not record its "initialized" marker in object
/// storage, so it proposes no client write yet: a retryable 503, nothing was
/// proposed.
pub(crate) fn group_engine_initialized_marker_unavailable(
    raft_group_id: ursula_shard::RaftGroupId,
    err: &str,
    self_id: u64,
) -> GroupEngineError {
    tracing::warn!(
        raft_group_id = raft_group_id.0,
        "memory-WAL group refuses a write until its initialized marker is in object storage: {err}"
    );
    group_engine_forward_to_leader_error(
        format!(
            "raft group {} cannot record its initialized marker in object storage yet",
            raft_group_id.0
        ),
        None,
        None,
        self_id,
        true,
    )
}

/// `before_proposal` is true only for a local leadership check that runs
/// before anything is proposed (RT1); a forward OpenRaft reports after
/// `client_write` is ambiguous, because the entry may already have committed.
pub(crate) fn group_engine_forward_to_leader_error(
    message: impl Into<String>,
    leader_id: Option<u64>,
    leader_node: Option<&BasicNode>,
    self_id: u64,
    before_proposal: bool,
) -> GroupEngineError {
    let build = if before_proposal {
        GroupEngineError::forward_to_leader_before_proposal
    } else {
        GroupEngineError::forward_to_leader
    };
    // The write bounced because this node is not the leader. If the reported
    // leader is *this* node, leadership is in a transient step-down/election
    // window: redirecting the client back to ourselves would just loop, so
    // report leader-unknown and let the HTTP layer answer with a retryable 503.
    if leader_id == Some(self_id) {
        return build(message, None, None);
    }
    build(
        message,
        leader_id,
        leader_node.map(|node| node.addr.clone()),
    )
}
