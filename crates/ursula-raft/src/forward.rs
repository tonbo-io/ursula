use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::time::Duration;

use futures_util::TryStreamExt;
use openraft::BasicNode;
use openraft::Raft;
use openraft::rt::WatchReceiver;
use serde::de::DeserializeOwned;
use tonic::transport::Channel;
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
use crate::format_epoch::observe_outbound_status;
use crate::grpc::RAFT_GRPC_MAX_MESSAGE_BYTES;
use crate::grpc::RAFT_GRPC_PROTOCOL_VERSION;
use crate::grpc::RaftClient;
use crate::peer_channel::peer_endpoint;
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
    leader_node: BasicNode,
    request: HeadStreamRequest,
) -> Result<HeadStreamResponse, GroupEngineError> {
    let head = head_stream_read_v1(&request);
    forward_typed_read_to_leader(
        placement,
        &leader_node,
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
    leader_node: BasicNode,
    request: ReadStreamRequest,
) -> Result<ReadStreamResponse, GroupEngineError> {
    let read = read_stream_read_v1(&request)?;
    forward_typed_read_to_leader(
        placement,
        &leader_node,
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
        .map_err(|_overflow| GroupEngineError::new("read max_len does not fit u64"))?;
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
    let request = raft_internal_proto::GroupReadRequestV1 {
        raft_group_id: placement.raft_group_id.0,
        core_id: u32::from(placement.core_id.0),
        shard_id: placement.shard_id.0,
        bucket_id: stream_id.bucket_id,
        stream_id: stream_id.stream_id,
        now_ms,
        read: Some(read),
        protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
    };
    call_leader(
        placement,
        &leader_node.addr,
        ForwardedRpc::GroupRead,
        request,
        |mut client, request| async move { client.group_read(request).await },
    )
    .await
    .map_err(ForwardError::into_engine_error)
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
    let request = raft_internal_proto::GroupWriteRequestV1 {
        raft_group_id: placement.raft_group_id.0,
        core_id: u32::from(placement.core_id.0),
        shard_id: placement.shard_id.0,
        command_payloads: vec![encode_wire(&GroupWriteCommand::Stream(
            ursula_stream::StreamCommand::PurgeBucket { bucket_id },
        ))],
        protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
    };
    let response = call_leader(
        placement,
        &leader_node.addr,
        ForwardedRpc::GroupWrite,
        request,
        |mut client, request| async move { client.group_write(request).await },
    )
    .await
    .map_err(ForwardError::into_engine_error)?;
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

/// Deadline of one forwarded leader RPC, connect included. It ends a call
/// to a leader that still answers HTTP/2 PINGs but not the call (a silent
/// one is cut off sooner, see [`crate::peer_channel`]). It leaves the
/// leader room for a ReadIndex round (at most `election_timeout_min`) and a
/// cold-tier GET, and stays well below a gateway's 30 s header timeout, so
/// the client gets this node's retryable answer.
const FORWARD_RPC_TIMEOUT: Duration = Duration::from_secs(10);

/// The leader RPCs a follower forwards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ForwardedRpc {
    /// A read or HEAD the leader serves.
    GroupRead,
    /// A bucket purge the leader proposes.
    GroupWrite,
}

impl ForwardedRpc {
    fn route(self) -> &'static str {
        match self {
            Self::GroupRead => "GroupRead",
            Self::GroupWrite => "GroupWrite",
        }
    }
}

impl std::fmt::Display for ForwardedRpc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.route())
    }
}

/// Why a forwarded leader RPC got no answer from the leader's group.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ForwardError {
    #[error("invalid gRPC leader endpoint {address}")]
    InvalidEndpoint {
        address: String,
        #[source]
        source: tonic::transport::Error,
    },
    /// No connection to the leader: nothing was sent.
    #[error("connect to the group leader at {address}")]
    Connect {
        address: String,
        #[source]
        source: tonic::transport::Error,
    },
    /// The call failed at the transport (a broken or silent connection) or
    /// the leader could not serve it right now (`Unavailable`).
    #[error("{rpc} to the group leader at {address} failed at the transport")]
    Transport {
        rpc: ForwardedRpc,
        address: String,
        #[source]
        source: tonic::Status,
    },
    #[error("{rpc} to the group leader at {address} did not complete within {timeout:?}")]
    TimedOut {
        rpc: ForwardedRpc,
        address: String,
        timeout: Duration,
    },
    /// The leader refused the call (protocol mismatch, unknown group, a
    /// malformed request).
    #[error("{rpc} to the group leader at {address} was refused")]
    Refused {
        rpc: ForwardedRpc,
        address: String,
        #[source]
        source: tonic::Status,
    },
}

impl ForwardError {
    /// True when this node could not reach the leader, or the leader could
    /// not serve: retrying once the connection or the election settles may
    /// succeed.
    fn leader_unreachable(&self) -> bool {
        matches!(
            self,
            Self::Connect { .. } | Self::Transport { .. } | Self::TimedOut { .. }
        )
    }

    /// The engine-level answer, decided here only. A leader this node
    /// cannot reach is, to the client, a leader it does not know:
    /// leader-unknown (HTTP 503 with `Retry-After`), retried once the
    /// election or the connection settles. Its message names no internal
    /// address; the full error is logged where it happens.
    fn into_engine_error(self) -> GroupEngineError {
        if self.leader_unreachable() {
            // A purge whose request reached the leader's socket may have
            // committed there.
            let proposal_may_have_started = matches!(
                &self,
                Self::Transport {
                    rpc: ForwardedRpc::GroupWrite,
                    ..
                } | Self::TimedOut {
                    rpc: ForwardedRpc::GroupWrite,
                    ..
                }
            );
            let message = "the raft group leader is unreachable; retry";
            return if proposal_may_have_started {
                GroupEngineError::forward_to_leader(message, None, None)
            } else {
                GroupEngineError::forward_to_leader_before_proposal(message, None, None)
            };
        }
        GroupEngineError::new(ErrorChain(&self).to_string())
    }
}

/// Status codes of a forwarded call that failed at the transport or found
/// the leader unable to serve. Tonic reports this side's failures as
/// `Unavailable` (connect), `Cancelled` (deadline), `Unknown` (a broken
/// connection or a keepalive timeout); a leader answers `Unavailable` while
/// its group owner is stopped.
fn is_transport_failure(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Cancelled
            | tonic::Code::Unknown
    )
}

/// One forwarded leader RPC under [`FORWARD_RPC_TIMEOUT`]. A call that fails
/// at the transport drops its cached channel, so the next one reconnects.
async fn call_leader<Req, Resp, Fut>(
    placement: ShardPlacement,
    address: &str,
    rpc: ForwardedRpc,
    request: Req,
    send: impl FnOnce(RaftClient, tonic::Request<Req>) -> Fut,
) -> Result<Resp, ForwardError>
where
    Fut: Future<Output = Result<tonic::Response<Resp>, tonic::Status>>,
{
    let started = crate::rt::time::Instant::now();
    let timed_out = || ForwardError::TimedOut {
        rpc,
        address: address.to_owned(),
        timeout: FORWARD_RPC_TIMEOUT,
    };
    let connected =
        crate::rt::time::timeout(FORWARD_RPC_TIMEOUT, LEADER_CHANNELS.connect(address)).await;
    let mut dropped_channel = false;
    let error = match connected {
        Err(_elapsed) => timed_out(),
        Ok(Err(error)) => error,
        Ok(Ok(leader)) => {
            let remaining = FORWARD_RPC_TIMEOUT.saturating_sub(started.elapsed());
            let client = RaftClient::new(leader.channel.clone())
                .max_decoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES)
                .max_encoding_message_size(RAFT_GRPC_MAX_MESSAGE_BYTES);
            let mut request = tonic::Request::new(request);
            // The leader stops working on the call when this side gives up.
            request.set_timeout(remaining);
            // Carry this request's trace context to the leader so the
            // forwarded call joins the originating trace. No-op when no
            // propagator is installed.
            crate::telemetry::inject_current_context(request.metadata_mut());
            let error = match crate::rt::time::timeout(remaining, send(client, request)).await {
                Ok(Ok(response)) => return Ok(response.into_inner()),
                Ok(Err(status)) => {
                    observe_outbound_status(rpc.route(), &status);
                    if is_transport_failure(&status) {
                        ForwardError::Transport {
                            rpc,
                            address: address.to_owned(),
                            source: status,
                        }
                    } else {
                        ForwardError::Refused {
                            rpc,
                            address: address.to_owned(),
                            source: status,
                        }
                    }
                }
                Err(_elapsed) => timed_out(),
            };
            dropped_channel =
                error.leader_unreachable() && LEADER_CHANNELS.evict(address, leader.generation);
            error
        }
    };
    if dropped_channel {
        tracing::warn!(
            group = placement.raft_group_id.0,
            leader = address,
            error = %ErrorChain(&error),
            "dropped the forwarding channel to a group leader after a transport failure"
        );
    } else {
        tracing::debug!(
            group = placement.raft_group_id.0,
            leader = address,
            error = %ErrorChain(&error),
            "forwarded leader RPC failed"
        );
    }
    Err(error)
}

/// Renders an error with its sources, for logs.
struct ErrorChain<'a>(&'a (dyn std::error::Error + 'static));

impl std::fmt::Display for ErrorChain<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)?;
        let mut source = self.0.source();
        while let Some(error) = source {
            write!(f, ": {error}")?;
            source = error.source();
        }
        Ok(())
    }
}

/// The forwarding channels of this process, one HTTP/2 channel per leader
/// address shared by every group. A call that fails at the transport drops
/// its channel, so the next call connects afresh (bounded by the peer
/// endpoint's connect timeout) and an address that stopped answering is
/// not kept.
static LEADER_CHANNELS: LeaderChannels = LeaderChannels::new();

struct LeaderChannels(Mutex<LeaderChannelsInner>);

struct LeaderChannelsInner {
    by_address: BTreeMap<String, LeaderChannel>,
    next_generation: u64,
}

/// A cached channel and the generation that tells it apart from a newer
/// channel to the same address.
#[derive(Clone)]
struct LeaderChannel {
    generation: u64,
    channel: Channel,
}

impl LeaderChannels {
    const fn new() -> Self {
        Self(Mutex::new(LeaderChannelsInner {
            by_address: BTreeMap::new(),
            next_generation: 0,
        }))
    }

    fn lock(&self) -> MutexGuard<'_, LeaderChannelsInner> {
        // Every critical section leaves the map consistent.
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The cached channel to `address`, or a new connection to it.
    async fn connect(&self, address: &str) -> Result<LeaderChannel, ForwardError> {
        if let Some(cached) = self.lock().by_address.get(address) {
            return Ok(cached.clone());
        }
        let channel = peer_endpoint(address)
            .map_err(|source| ForwardError::InvalidEndpoint {
                address: address.to_owned(),
                source,
            })?
            .connect()
            .await
            .map_err(|source| ForwardError::Connect {
                address: address.to_owned(),
                source,
            })?;
        let mut inner = self.lock();
        // A concurrent caller may have connected first; share its channel.
        if let Some(cached) = inner.by_address.get(address) {
            return Ok(cached.clone());
        }
        inner.next_generation = inner.next_generation.wrapping_add(1);
        let connected = LeaderChannel {
            generation: inner.next_generation,
            channel,
        };
        inner
            .by_address
            .insert(address.to_owned(), connected.clone());
        Ok(connected)
    }

    /// Drops the channel to `address` unless a newer one replaced it.
    /// Returns whether it dropped one.
    fn evict(&self, address: &str, generation: u64) -> bool {
        let mut inner = self.lock();
        if inner
            .by_address
            .get(address)
            .is_some_and(|cached| cached.generation == generation)
        {
            inner.by_address.remove(address);
            return true;
        }
        false
    }

    #[cfg(test)]
    fn contains(&self, address: &str) -> bool {
        self.lock().by_address.contains_key(address)
    }
}

/// Whether this process holds a forwarding channel to `address`.
#[cfg(test)]
pub(crate) fn has_leader_channel(address: &str) -> bool {
    LEADER_CHANNELS.contains(address)
}

/// Reject commands that the data-group apply dispatcher cannot execute.
pub(crate) fn validate_proposal(command: &GroupWriteCommand) -> Result<(), GroupEngineError> {
    let GroupWriteCommand::Stream(command) = command;
    let mut command = command;
    while let ursula_stream::StreamCommand::IfIncarnation { command: inner, .. } = command {
        command = inner;
    }
    if matches!(command, ursula_stream::StreamCommand::CreateBucket { .. }) {
        return Err(GroupEngineError::Infra(
            ursula_runtime::GroupInfraError::InvalidRaftCommand {
                command: "CreateBucket".to_owned(),
            },
        ));
    }
    Ok(())
}

pub(crate) async fn write_commands_on_raft(
    raft: Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>,
    commands: Vec<GroupWriteCommand>,
) -> Result<Vec<Result<GroupWriteResponse, GroupEngineError>>, GroupEngineError> {
    if commands.is_empty() {
        return Ok(Vec::new());
    }
    for command in &commands {
        validate_proposal(command)?;
    }
    let expected_responses = commands.len();
    let stream = raft.client_write_many(commands).await.map_err(|error| {
        tracing::warn!(%error, "Raft batch submission stopped");
        GroupEngineError::Infra(ursula_runtime::GroupInfraError::OutcomeUnknown)
    })?;
    let responses = stream.map_ok(|result| match result {
        Ok(response) => write_result_from_raft_response(response.response).unwrap_or_else(Err),
        Err(err) => Err(group_engine_forward_to_leader_error(
            format!("OpenRaft client_write_many forwarded to leader: {err}"),
            err.leader_id,
            err.leader_node.as_ref(),
            raft.metrics().borrow_watched().id,
            false,
        )),
    });
    Ok(collect_batch_responses(responses, expected_responses).await)
}

/// Preserve every delivered outcome even when a later responder is lost.
async fn collect_batch_responses<T, E: std::fmt::Display>(
    mut stream: impl futures_util::Stream<Item = Result<Result<T, GroupEngineError>, E>> + Unpin,
    expected: usize,
) -> Vec<Result<T, GroupEngineError>> {
    let mut responses = Vec::with_capacity(expected);
    loop {
        match stream.try_next().await {
            Ok(Some(response)) => responses.push(response),
            Ok(None) => break,
            Err(error) => {
                tracing::warn!(%error, received = responses.len(), expected, "Raft batch response stream stopped");
                break;
            }
        }
    }
    responses.resize_with(expected, || {
        Err(GroupEngineError::Infra(
            ursula_runtime::GroupInfraError::OutcomeUnknown,
        ))
    });
    responses
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
    tracing::warn!(error = %err, "Raft write response lost after submission");
    GroupEngineError::Infra(ursula_runtime::GroupInfraError::OutcomeUnknown)
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
#[cfg(test)]
mod batch_response_tests {
    use ursula_runtime::GroupEngineError;

    use super::collect_batch_responses;
    #[tokio::test]
    async fn fatal_after_applied_prefix_preserves_known_outcomes() {
        for error in [true, false] {
            let mut replies = vec![
                Ok(Ok(7)),
                Ok(Err(GroupEngineError::stream(
                    ursula_stream::StreamErrorCode::StreamNotFound,
                    "absent",
                ))),
            ];
            if error {
                replies.push(Err("raft stopped"));
            }
            let result =
                collect_batch_responses(futures_util::stream::iter(replies.clone()), 4).await;
            assert_eq!(result[0], Ok(7));
            assert_eq!(result[1], replies[1].clone().unwrap());
            assert!(result[2..].iter().all(|item| matches!(
                item,
                Err(GroupEngineError::Infra(
                    ursula_runtime::GroupInfraError::OutcomeUnknown
                ))
            )));
        }
    }
}
