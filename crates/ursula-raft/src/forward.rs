use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::TryStreamExt;
use openraft::Raft;
use openraft::rt::WatchReceiver;
use serde::de::DeserializeOwned;
use tonic::transport::Channel;
use ursula_control::ControlCommand;
use ursula_control::ControlResponse;
use ursula_runtime::AnswerProblem;
use ursula_runtime::CommittedLeader;
use ursula_runtime::ErrorChain;
use ursula_runtime::ErrorStatus;
use ursula_runtime::FollowerRead;
use ursula_runtime::ForwardFailure;
use ursula_runtime::ForwardedRpc;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupInfraError;
use ursula_runtime::GroupWriteResponse;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::HeadStreamResponse;
use ursula_runtime::KnownLeader;
use ursula_runtime::LeadershipReason;
use ursula_runtime::LeadershipRefusal;
use ursula_runtime::Proposal;
use ursula_runtime::PurgeBucketResponse;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::ReadStreamResponse;
use ursula_runtime::ResponseKind;
use ursula_runtime::WriteEffect;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftTarget;
use ursula_shard::ShardPlacement;

use crate::GroupLeadership;
use crate::codec::WireDecodeError;
use crate::codec::decode_wire;
use crate::codec::encode_wire;
use crate::format_epoch::observe_outbound_status;
use crate::grpc::RAFT_GRPC_MAX_MESSAGE_BYTES;
use crate::grpc::RAFT_GRPC_PROTOCOL_VERSION;
use crate::grpc::RaftClient;
use crate::meta::MetaCommand;
use crate::meta::MetaForwardError;
use crate::meta::MetaResponse;
use crate::peer_channel::peer_endpoint;
use crate::peer_cluster::PeerCluster;
use crate::peer_cluster::refusing_cluster;
use crate::raft_internal_proto;
use crate::target::WireTarget;
use crate::types::RaftCommand;
use crate::types::RaftGroupResponse;
use crate::types::UrsulaRaftTypeConfig;

/// Where a follower forwards a leader RPC: the leader, the cluster URL
/// `raft.peers` lists for it, and this node's side of the cluster-id check,
/// which stamps the call and records a refusal.
#[derive(Debug, Clone)]
pub(crate) struct LeaderRoute {
    pub(crate) peer_cluster: PeerCluster,
    pub(crate) leader_id: u64,
    pub(crate) address: String,
}

#[tracing::instrument(
    name = "raft.forward_head",
    skip_all,
    fields(group = placement.raft_group_id.0, bucket = %request.stream_id.bucket_id, stream = %request.stream_id.stream_id),
)]
pub(crate) async fn forward_head_stream_to_leader(
    route: LeaderRoute,
    placement: ShardPlacement,
    request: HeadStreamRequest,
) -> Result<HeadStreamResponse, GroupEngineError> {
    let head = head_stream_read_v1(&request);
    forward_typed_read_to_leader(
        &route,
        placement,
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
    route: LeaderRoute,
    placement: ShardPlacement,
    request: ReadStreamRequest,
) -> Result<ReadStreamResponse, GroupEngineError> {
    let read = read_stream_read_v1(&request);
    forward_typed_read_to_leader(
        &route,
        placement,
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

/// The wire form of a forwarded read. A `max_len` beyond `u64` asks for
/// everything either way, so it saturates.
pub(crate) fn read_stream_read_v1(
    request: &ReadStreamRequest,
) -> raft_internal_proto::ReadStreamReadV1 {
    raft_internal_proto::ReadStreamReadV1 {
        offset: request.offset,
        max_len: u64::try_from(request.max_len).unwrap_or(u64::MAX),
        leader_only: request.leader_only,
        follower: match request.follower {
            FollowerRead::Prefix => raft_internal_proto::FollowerReadV1::Prefix,
            FollowerRead::Immutable => raft_internal_proto::FollowerReadV1::Immutable,
        }
        .into(),
    }
}

/// Forward one leader-only read to the leader over gRPC and decode the
/// serde-carried response payload into the engine-level response (`T`). The
/// two public forwarders above differ only in the read variant they
/// construct and the decode target, so they all funnel through here.
async fn forward_typed_read_to_leader<T>(
    route: &LeaderRoute,
    placement: ShardPlacement,
    stream_id: BucketStreamId,
    now_ms: u64,
    what: &'static str,
    read: raft_internal_proto::group_read_request_v1::Read,
) -> Result<T, GroupEngineError>
where
    T: DeserializeOwned,
{
    let response = forward_group_read_to_leader(route, placement, stream_id, now_ms, read).await?;
    forwarded_answer(
        ForwardedRpc::GroupRead,
        response.ok,
        &response.payload,
        what,
    )
}

/// A forwarded RPC's answer: the leader's response when `ok`, else the
/// leader's error, which keeps its type. An answer that does not decode is
/// [`AnswerProblem::Undecodable`]; the decode error is logged here.
fn forwarded_answer<T: DeserializeOwned>(
    rpc: ForwardedRpc,
    ok: bool,
    payload: &[u8],
    what: &'static str,
) -> Result<T, GroupEngineError> {
    let undecodable = |error: WireDecodeError| {
        tracing::warn!(%rpc, error = %ErrorChain(&error), "forwarded answer does not decode");
        GroupEngineError::Infra(GroupInfraError::ForwardedAnswerInvalid {
            rpc,
            problem: AnswerProblem::Undecodable,
        })
    };
    if ok {
        decode_wire(payload, what).map_err(undecodable)
    } else {
        Err(decode_wire::<GroupEngineError>(payload, what).map_err(undecodable)?)
    }
}

pub(crate) async fn forward_group_read_to_leader(
    route: &LeaderRoute,
    placement: ShardPlacement,
    stream_id: BucketStreamId,
    now_ms: u64,
    read: raft_internal_proto::group_read_request_v1::Read,
) -> Result<raft_internal_proto::GroupReadResponseV1, GroupEngineError> {
    let mut request = raft_internal_proto::GroupReadRequestV1 {
        core_id: u32::from(placement.core_id.0),
        shard_id: placement.shard_id.0,
        bucket_id: stream_id.bucket_id,
        stream_id: stream_id.stream_id,
        now_ms,
        read: Some(read),
        protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
        ..Default::default()
    };
    let target = RaftTarget::Data(placement.raft_group_id);
    target.stamp(route.leader_id, &mut request);
    call_leader(
        route,
        target,
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
    route: &LeaderRoute,
    placement: ShardPlacement,
    bucket_id: String,
) -> Result<PurgeBucketResponse, GroupEngineError> {
    let mut request = raft_internal_proto::GroupWriteRequestV1 {
        core_id: u32::from(placement.core_id.0),
        shard_id: placement.shard_id.0,
        command_payloads: vec![encode_wire(&RaftCommand::Stream(
            ursula_stream::StreamCommand::PurgeBucket { bucket_id },
        ))],
        protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
        ..Default::default()
    };
    let target = RaftTarget::Data(placement.raft_group_id);
    target.stamp(route.leader_id, &mut request);
    let response = call_leader(
        route,
        target,
        ForwardedRpc::GroupWrite,
        request,
        |mut client, request| async move { client.group_write(request).await },
    )
    .await
    .map_err(ForwardError::into_engine_error)?;
    let result = single_write_answer(response.results)?;
    let response: GroupWriteResponse = forwarded_answer(
        ForwardedRpc::GroupWrite,
        result.ok,
        &result.payload,
        "bucket purge answer",
    )?;
    PurgeBucketResponse::try_from(response)
}

/// Forwards one control command to the meta leader `route` names: a
/// `GroupWrite` of the meta plane, with core and shard 0 and the command as
/// its one `RaftCommand::Meta(Control(_))`. The meta leader answers it as a
/// data leader answers a forwarded write, and refuses (`INVALID_ARGUMENT`,
/// before anything is proposed) a `RegisterNode` naming another node than
/// this one, whose admitted id the call carries.
pub(crate) async fn forward_control_to_leader(
    route: &LeaderRoute,
    command: ControlCommand,
) -> Result<ControlResponse, MetaForwardError> {
    let mut request = raft_internal_proto::GroupWriteRequestV1 {
        core_id: 0,
        shard_id: 0,
        command_payloads: vec![encode_wire(&RaftCommand::Meta(MetaCommand::Control(
            command,
        )))],
        protocol_version: RAFT_GRPC_PROTOCOL_VERSION,
        ..Default::default()
    };
    RaftTarget::Meta.stamp(route.leader_id, &mut request);
    let response = call_leader(
        route,
        RaftTarget::Meta,
        ForwardedRpc::GroupWrite,
        request,
        |mut client, request| async move { client.group_write(request).await },
    )
    .await
    .map_err(MetaForwardError::Transport)?;
    let result = single_write_answer(response.results).map_err(MetaForwardError::Leader)?;
    forwarded_answer(
        ForwardedRpc::GroupWrite,
        result.ok,
        &result.payload,
        "control answer",
    )
    .map_err(MetaForwardError::Leader)
}

/// The one answer of a forwarded write of one command, or the refusal of an
/// answer with another count.
fn single_write_answer<T>(results: Vec<T>) -> Result<T, GroupEngineError> {
    let [result] = <[_; 1]>::try_from(results).map_err(|results| {
        GroupEngineError::Infra(GroupInfraError::ForwardedAnswerInvalid {
            rpc: ForwardedRpc::GroupWrite,
            problem: AnswerProblem::ResultCount {
                results: u64::try_from(results.len()).unwrap_or(u64::MAX),
            },
        })
    })?;
    Ok(result)
}

/// [`single_write_answer`] of a meta write answered in this process, as
/// [`forward_control_to_leader`] reads the same answer off the wire: the
/// leader's answer, or its refusal ([`MetaForwardError::Leader`]).
pub(crate) fn single_control_answer(
    answers: Vec<Result<ControlResponse, GroupEngineError>>,
) -> Result<ControlResponse, GroupEngineError> {
    single_write_answer(answers).and_then(|answer| answer)
}

/// Deadline of one forwarded leader RPC, connect included. It ends a call
/// to a leader that still answers HTTP/2 PINGs but not the call (a silent
/// one is cut off sooner, see [`crate::peer_channel`]). It leaves the
/// leader room for a ReadIndex round (at most `election_timeout_min`) and a
/// cold-tier GET, and stays well below a gateway's 30 s header timeout, so
/// the client gets this node's retryable answer.
const FORWARD_RPC_TIMEOUT: Duration = Duration::from_secs(10);

/// Why a forwarded leader RPC got no answer from the leader's group. Never
/// encoded: the forwarder classifies it ([`Self::write_effect`],
/// [`Self::status`]), and a data-plane forward answers its client with
/// [`Self::into_engine_error`].
#[derive(Debug, thiserror::Error)]
pub enum ForwardError {
    /// This replica runs without the gRPC transport, so it has no identity
    /// to stamp on the call: nothing was sent.
    #[error("{rpc} cannot be forwarded: this replica has no Raft gRPC identity")]
    NoIdentity { rpc: ForwardedRpc },
    /// The leader is in another cluster (a misconfigured peer, or a node
    /// replaced at the leader's address): nothing ran there.
    #[error("{rpc} to the group leader at {address} was refused: it is in cluster {cluster_id}")]
    OtherCluster {
        rpc: ForwardedRpc,
        address: String,
        cluster_id: ursula_headers::ClusterId,
    },
    /// The leader's address serves another node now, which refused the call
    /// as meant for another node: nothing ran there.
    #[error("{rpc} to the group leader at {address} was refused: the address serves node {node}")]
    OtherNode {
        rpc: ForwardedRpc,
        address: String,
        node: u64,
    },
    #[error("{rpc} cannot be forwarded: invalid gRPC leader endpoint {address}")]
    InvalidEndpoint {
        rpc: ForwardedRpc,
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
    /// Whether the leader refused the request itself as one it refuses
    /// again: `INVALID_ARGUMENT`, which a leader answers only before it
    /// proposes anything (a malformed request, or a forwarded
    /// `RegisterNode` naming another node than the sender). Retrying it is
    /// pointless.
    fn refused_for_good(&self) -> bool {
        matches!(self, Self::Refused { source, .. } if source.code() == tonic::Code::InvalidArgument)
    }

    /// What the forwarded write did: nothing for a request the leader
    /// refused for good, else what the engine-level answer
    /// ([`Self::into_engine_error`]) says.
    pub fn write_effect(&self) -> WriteEffect {
        if self.refused_for_good() {
            return WriteEffect::NotProposed;
        }
        self.engine_error().write_effect()
    }

    /// Whether retrying may succeed: permanent for a request the leader
    /// refused for good, else what the engine-level answer says.
    pub fn status(&self) -> ErrorStatus {
        if self.refused_for_good() {
            return ErrorStatus::Permanent;
        }
        self.engine_error().status()
    }

    /// True when this node could not reach the leader, or the leader could
    /// not serve: retrying once the connection or the election settles may
    /// succeed.
    fn leader_unreachable(&self) -> bool {
        matches!(
            self,
            Self::Connect { .. } | Self::Transport { .. } | Self::TimedOut { .. }
        )
    }

    /// True when the cached channel to the leader's address is of no more
    /// use: the leader is unreachable over it, or the address serves
    /// another node now, which `raft.peers` or a moved address can fix
    /// without a restart. The next call connects afresh.
    fn drops_channel(&self) -> bool {
        self.leader_unreachable() || matches!(self, Self::OtherNode { .. })
    }

    /// The engine-level answer, decided here only. A leader this node
    /// cannot reach is, to the client, a leader it does not know:
    /// leader-unknown (HTTP 503 with `Retry-After`), retried once the
    /// election or the connection settles. The native error is logged here
    /// and stays out of the answer, which names no internal address.
    pub(crate) fn into_engine_error(self) -> GroupEngineError {
        let error = self.engine_error();
        if matches!(error, GroupEngineError::Infra(_)) {
            tracing::warn!(error = %ErrorChain(&self), "forwarded leader RPC failed");
        }
        error
    }

    /// [`Self::into_engine_error`], without the log.
    fn engine_error(&self) -> GroupEngineError {
        let (rpc, failure) = match self {
            // Nothing ran on the refusing node. To the client it is a leader
            // this node cannot use, so it answers 503 with Retry-After while
            // the group elects a leader of this cluster.
            Self::OtherCluster { .. } => {
                return unknown_leader(
                    LeadershipReason::LeaderInOtherCluster,
                    Proposal::NotProposed,
                );
            }
            // Nothing ran on the node at the leader's address, which is
            // another node now: to the client, a leader this node cannot
            // reach until its view of the group's leader moves on.
            Self::OtherNode { .. } => {
                return unknown_leader(LeadershipReason::LeaderUnreachable, Proposal::NotProposed);
            }
            // A write (a purge, a control command) whose request reached the
            // leader's socket may have committed there.
            Self::Transport {
                rpc: ForwardedRpc::GroupWrite,
                ..
            }
            | Self::TimedOut {
                rpc: ForwardedRpc::GroupWrite,
                ..
            } => {
                return unknown_leader(
                    LeadershipReason::LeaderUnreachable,
                    Proposal::MayHaveCommitted,
                );
            }
            Self::Connect { .. } | Self::Transport { .. } | Self::TimedOut { .. } => {
                return unknown_leader(LeadershipReason::LeaderUnreachable, Proposal::NotProposed);
            }
            Self::NoIdentity { rpc } => (*rpc, ForwardFailure::NoIdentity),
            Self::InvalidEndpoint { rpc, .. } => (*rpc, ForwardFailure::InvalidEndpoint),
            Self::Refused { rpc, source, .. } => (*rpc, ForwardFailure::Refused {
                grpc_code: i32::from(source.code()),
            }),
        };
        GroupEngineError::Infra(GroupInfraError::ForwardFailed { rpc, failure })
    }
}

/// Status codes of a forwarded call that failed at the transport or found
/// the leader unable to serve. Tonic reports this side's failures as
/// `Unavailable` (connect), `Cancelled` (deadline), `Unknown` (a broken
/// connection or a keepalive timeout); a leader answers `Unavailable` while
/// its group owner is stopped.
pub(crate) fn is_transport_failure(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Cancelled
            | tonic::Code::Unknown
    )
}

/// One forwarded leader RPC under [`FORWARD_RPC_TIMEOUT`]. A call that fails
/// at the transport, or that the leader's address refused as meant for
/// another node, drops its cached channel, so the next one reconnects.
async fn call_leader<Req, Resp, Fut>(
    route: &LeaderRoute,
    target: RaftTarget,
    rpc: ForwardedRpc,
    request: Req,
    send: impl FnOnce(RaftClient, tonic::Request<Req>) -> Fut,
) -> Result<Resp, ForwardError>
where
    Fut: Future<Output = Result<tonic::Response<Resp>, tonic::Status>>,
{
    let address = route.address.as_str();
    let peer_cluster = &route.peer_cluster;
    let Some(identity) = peer_cluster.identity() else {
        return Err(ForwardError::NoIdentity { rpc });
    };
    let started = crate::rt::time::Instant::now();
    let timed_out = || ForwardError::TimedOut {
        rpc,
        address: address.to_owned(),
        timeout: FORWARD_RPC_TIMEOUT,
    };
    let connected =
        crate::rt::time::timeout(FORWARD_RPC_TIMEOUT, LEADER_CHANNELS.connect(address, rpc)).await;
    let mut dropped_channel = false;
    let error = match connected {
        Err(_elapsed) => timed_out(),
        Ok(Err(error)) => error,
        Ok(Ok(leader)) => {
            let remaining = FORWARD_RPC_TIMEOUT.saturating_sub(started.elapsed());
            let client =
                raft_internal_proto::raft_internal_client::RaftInternalClient::with_interceptor(
                    leader.channel.clone(),
                    identity,
                )
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
                    if let Some(node) = crate::target::refusing_node(&status, route.leader_id) {
                        peer_cluster.observe_misdelivery(route.leader_id, address, node);
                        ForwardError::OtherNode {
                            rpc,
                            address: address.to_owned(),
                            node,
                        }
                    } else if peer_cluster.observe_refusal(
                        route.leader_id,
                        address,
                        &status,
                        crate::rt::time::Instant::now(),
                    ) && let Some(cluster_id) = refusing_cluster(&status)
                    {
                        ForwardError::OtherCluster {
                            rpc,
                            address: address.to_owned(),
                            cluster_id,
                        }
                    } else if is_transport_failure(&status) {
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
                error.drops_channel() && LEADER_CHANNELS.evict(address, leader.generation);
            error
        }
    };
    // A leader unreachable over its channel warns on each drop. An address
    // that serves another node drops its channel at debug:
    // `observe_misdelivery` already warned once for that change, and every
    // later forward to the same address repeats the drop.
    if dropped_channel && error.leader_unreachable() {
        tracing::warn!(
            plane = ?target.plane(),
            group = target.wire_group_id(),
            leader = address,
            error = %ErrorChain(&error),
            "dropped the forwarding channel to a group leader: the leader is unreachable over it"
        );
    } else if dropped_channel {
        tracing::debug!(
            plane = ?target.plane(),
            group = target.wire_group_id(),
            leader = address,
            error = %ErrorChain(&error),
            "dropped the forwarding channel to a group leader: its address serves another node now"
        );
    } else {
        tracing::debug!(
            plane = ?target.plane(),
            group = target.wire_group_id(),
            leader = address,
            error = %ErrorChain(&error),
            "forwarded leader RPC failed"
        );
    }
    Err(error)
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

    /// The cached channel to `address`, or a new connection to it for
    /// `rpc`.
    async fn connect(
        &self,
        address: &str,
        rpc: ForwardedRpc,
    ) -> Result<LeaderChannel, ForwardError> {
        if let Some(cached) = self.lock().by_address.get(address) {
            return Ok(cached.clone());
        }
        let channel = peer_endpoint(address)
            .map_err(|source| ForwardError::InvalidEndpoint {
                rpc,
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

    #[cfg(all(test, not(madsim)))]
    fn contains(&self, address: &str) -> bool {
        self.lock().by_address.contains_key(address)
    }
}

/// Whether this process holds a forwarding channel to `address`.
#[cfg(all(test, not(madsim)))]
pub(crate) fn has_leader_channel(address: &str) -> bool {
    LEADER_CHANNELS.contains(address)
}

/// Reject commands that the apply dispatcher of their plane cannot execute,
/// the one proposal check of both planes. Only a leader's history driver
/// proposes a history event, straight to its Raft core (#581 section 4.3),
/// on either plane.
pub(crate) fn validate_proposal(command: &RaftCommand) -> Result<(), GroupEngineError> {
    let unproposable = match command {
        RaftCommand::Stream(command) => {
            let mut command = command;
            while let ursula_stream::StreamCommand::IfIncarnation { command: inner, .. } = command {
                command = inner;
            }
            match command {
                ursula_stream::StreamCommand::CreateBucket { .. } => {
                    ursula_runtime::UnproposableCommand::CreateBucket
                }
                ursula_stream::StreamCommand::NewHistory { .. } => {
                    ursula_runtime::UnproposableCommand::NewHistory
                }
                _ => return Ok(()),
            }
        }
        RaftCommand::Meta(MetaCommand::NewHistory { .. }) => {
            ursula_runtime::UnproposableCommand::NewHistory
        }
        RaftCommand::Meta(MetaCommand::Control(_)) => return Ok(()),
    };
    Err(invalid_raft_command(unproposable))
}

/// The refusal of a command that may not be proposed: nothing was written.
pub(crate) fn invalid_raft_command(
    command: ursula_runtime::UnproposableCommand,
) -> GroupEngineError {
    GroupEngineError::Infra(ursula_runtime::GroupInfraError::InvalidRaftCommand { command })
}

/// The leadership a proposal must still be appended under (F14j).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProposalFence {
    /// Any leadership: the command names no object this node uploaded.
    Unfenced,
    /// The committed leader the proposal's upload, or its pass, was
    /// planned under. OpenRaft appends the proposal only while it is the
    /// group's committed leader.
    Leadership(CommittedLeader),
}

/// The committed leader OpenRaft must still find when it takes `command`
/// under `fence`, if any. A command that names as new an object this node
/// uploaded ([`ursula_runtime::uploaded_object`]) but carries no fence is a
/// bug of its proposer: it is refused before proposal and logged, so its
/// uploader deletes the object rather than commit it under another
/// leadership. Both the single write and the batch path ask here.
pub(crate) fn proposal_leader(
    command: &RaftCommand,
    fence: ProposalFence,
) -> Result<Option<CommittedLeader>, GroupEngineError> {
    // A negative control turns the fence off: every upload proposes under
    // any leadership.
    if ursula_stream::sim_controls::off(ursula_stream::sim_controls::Defence::UploadFence) {
        return Ok(None);
    }
    // A meta command names no object.
    let uploaded = match command {
        RaftCommand::Stream(command) => ursula_runtime::uploaded_object(command),
        RaftCommand::Meta(_) => None,
    };
    match (fence, uploaded) {
        (ProposalFence::Leadership(leader), _) => Ok(Some(leader)),
        (ProposalFence::Unfenced, None) => Ok(None),
        (ProposalFence::Unfenced, Some(object)) => {
            tracing::error!(
                object,
                "an upload command reached the Raft engine without its leadership fence and \
                 was refused before proposal"
            );
            Err(unknown_leader(
                LeadershipReason::Follower,
                Proposal::NotProposed,
            ))
        }
    }
}

/// Proposes `commands`, which a forwarded `GroupWrite` of a data group
/// carried, as one batch. A command of the meta plane refuses the batch
/// before anything of it is proposed. `history` is the group's history on
/// this replica: a leader whose history is pending admits none of them
/// (#581 section 4.5).
pub(crate) async fn write_commands_on_raft<SM: crate::replica::ReplicaStateMachine>(
    raft: Raft<UrsulaRaftTypeConfig, SM>,
    fence: &ursula_runtime::NodeFence,
    history: &crate::history::GroupHistory,
    commands: Vec<RaftCommand>,
) -> Result<Vec<Result<SM::Answer, GroupEngineError>>, GroupEngineError> {
    if commands.is_empty() {
        return Ok(Vec::new());
    }
    let target = history.target();
    for command in &commands {
        if !target.admits(command) {
            return Err(invalid_raft_command(
                ursula_runtime::UnproposableCommand::OtherPlane,
            ));
        }
        history.admit_proposal(&raft, command).await?;
    }
    let expected_responses = commands.len();
    let expected_kinds: Vec<ResponseKind> = commands.iter().map(answer_kind).collect();
    await_first_quorum_ack(&raft).await;
    // A replica that lost the leadership during the admits refuses every
    // command here, before proposing any, as `write` does.
    if let Err(refused) = refuse_unless_leader(&raft, fence) {
        return Ok((0..expected_responses)
            .map(|_| Err(refused.clone()))
            .collect());
    }
    // A batch carries no fence, so an upload command in it refuses the
    // batch before anything of it is proposed.
    for command in &commands {
        proposal_leader(command, ProposalFence::Unfenced)?;
    }
    // The submission fails only when RaftCore is gone and hands the batch
    // back unsent: nothing of it was proposed, as for a single write.
    let stream = raft
        .client_write_many(commands)
        .await
        .map_err(crate::openraft_error::write_not_submitted)?;
    let responses =
        stream
            .zip(futures_util::stream::iter(expected_kinds))
            .map(|(result, expected)| {
                result.map(|result| {
                    crate::openraft_error::batch_write_result::<SM>(result, expected, || {
                        GroupLeadership::of(&raft.metrics().borrow_watched(), fence)
                    })
                })
            });
    Ok(collect_batch_responses(responses, expected_responses).await)
}

/// Waits, at most `election_timeout_min`, until this leader holds a quorum
/// acknowledgement, or until the replica no longer leads. OpenRaft refuses a
/// proposal while no quorum acknowledged the leader within its lease, as for
/// a new leader until its first follower answers. Waiting here turns that
/// round trip after each election into latency instead of a `503`. The wait
/// watches the metrics, so a leader that steps down meanwhile goes on at
/// once. A leader that no quorum acknowledges in time, or a replica that does
/// not lead, goes on to be refused before proposal.
pub(crate) async fn await_first_quorum_ack<
    SM: openraft::storage::RaftStateMachine<UrsulaRaftTypeConfig>,
>(
    raft: &Raft<UrsulaRaftTypeConfig, SM>,
) {
    let budget = Duration::from_millis(raft.config().election_timeout_min);
    await_quorum_ack_on(raft.metrics(), budget).await;
}

/// [`await_first_quorum_ack`] on the metrics watch `metrics`, bounded by
/// `budget`.
pub(crate) async fn await_quorum_ack_on(
    metrics: openraft::type_config::alias::WatchReceiverOf<
        UrsulaRaftTypeConfig,
        openraft::RaftMetrics<UrsulaRaftTypeConfig>,
    >,
    budget: Duration,
) {
    fn settled(metrics: &openraft::RaftMetrics<UrsulaRaftTypeConfig>) -> bool {
        metrics.state != openraft::ServerState::Leader || metrics.last_quorum_acked.is_some()
    }
    if settled(&metrics.borrow_watched()) {
        return;
    }
    let wait = openraft::metrics::Wait {
        timeout: Some(budget),
        rx: metrics,
    };
    if let Err(error) = wait
        .metrics(settled, "a new leader's first quorum acknowledgement")
        .await
    {
        tracing::debug!(%error, "a write found no quorum acknowledgement of this leader");
    }
}

/// The one check, right before commands go to OpenRaft, that this replica
/// leads the group, for a single write ([`RaftGroupEngine::write`]) and a
/// batch ([`write_commands_on_raft`]) alike. A replica that does not lead
/// refuses here, before proposing, so its refusal says that nothing applied
/// (a redirect or `NO_LEADER`). OpenRaft checks again when it takes the
/// command ([`client_write_refusal`]). One view of the metrics decides and
/// names the leader.
///
/// [`RaftGroupEngine::write`]: crate::engine::RaftGroupEngine::write
/// [`client_write_refusal`]: crate::openraft_error::client_write_refusal
pub(crate) fn refuse_unless_leader<
    SM: openraft::storage::RaftStateMachine<UrsulaRaftTypeConfig>,
>(
    raft: &Raft<UrsulaRaftTypeConfig, SM>,
    fence: &ursula_runtime::NodeFence,
) -> Result<(), GroupEngineError> {
    let receiver = raft.metrics();
    let metrics = receiver.borrow_watched();
    let leadership = GroupLeadership::of(&metrics, fence);
    if leadership.leads() {
        return Ok(());
    }
    Err(leadership_refusal(
        LeadershipReason::Follower,
        Proposal::NotProposed,
        metrics.current_leader,
        &leadership,
    ))
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

/// The kind of response the state machine of `command`'s plane answers it
/// with when it applies it, as [`write_result_from_raft_response`] names
/// what it got instead.
pub(crate) fn answer_kind(command: &RaftCommand) -> ResponseKind {
    match command {
        RaftCommand::Stream(command) => ResponseKind::answering(command),
        RaftCommand::Meta(MetaCommand::NewHistory { .. }) => ResponseKind::NewHistory,
        RaftCommand::Meta(MetaCommand::Control(_)) => ResponseKind::Control,
    }
}

/// Unwraps a raft-applied response into the write outcome it carries. The
/// command's response is of the kind `expected`
/// ([`ResponseKind::answering`]); OpenRaft's answer to a blank or membership
/// entry instead breaks an invariant.
pub(crate) fn write_result_from_raft_response(
    response: RaftGroupResponse,
    expected: ResponseKind,
) -> Result<Result<GroupWriteResponse, GroupEngineError>, GroupEngineError> {
    match response {
        RaftGroupResponse::Write(result) => Ok(result),
        other => Err(unexpected_response(expected, &other)),
    }
}

/// Unwraps the meta group's applied response into the control answer it
/// carries, as [`write_result_from_raft_response`] does a data write's.
pub(crate) fn control_answer_from_raft_response(
    response: RaftGroupResponse,
    expected: ResponseKind,
) -> Result<Result<ursula_control::ControlResponse, GroupEngineError>, GroupEngineError> {
    match response {
        RaftGroupResponse::Meta(MetaResponse::Control(answer)) => Ok(Ok(answer)),
        other => Err(unexpected_response(expected, &other)),
    }
}

/// The refusal of a response of another kind than `expected`.
fn unexpected_response(expected: ResponseKind, response: &RaftGroupResponse) -> GroupEngineError {
    let actual = match response {
        RaftGroupResponse::Write(Ok(response)) => response.kind(),
        RaftGroupResponse::Write(Err(_)) => ResponseKind::StreamError,
        RaftGroupResponse::Blank => ResponseKind::RaftBlank,
        RaftGroupResponse::Membership => ResponseKind::RaftMembership,
        RaftGroupResponse::Meta(MetaResponse::History | MetaResponse::HistoryMismatch { .. }) => {
            ResponseKind::NewHistory
        }
        RaftGroupResponse::Meta(MetaResponse::Control(_)) => ResponseKind::Control,
    };
    GroupEngineError::Infra(GroupInfraError::UnexpectedResponse { expected, actual })
}

/// A leadership refusal that names no leader: the answer is leader-unknown
/// (HTTP 503 with `Retry-After`), for a read whose leader could not be
/// linearized in time (no quorum confirmed it, or the local state machine
/// did not reach the read index) and for a leader this node cannot use.
pub(crate) fn unknown_leader(reason: LeadershipReason, proposal: Proposal) -> GroupEngineError {
    GroupEngineError::NotLeader(LeadershipRefusal {
        leader: None,
        proposal,
        reason,
    })
}

/// A leadership refusal naming the leader `leader_id` that this replica's
/// view `leadership` knows with its term. `proposal` says whether the
/// command may have been appended (RT1): only an entry OpenRaft appended
/// and then discarded may have.
pub(crate) fn leadership_refusal(
    reason: LeadershipReason,
    proposal: Proposal,
    leader_id: Option<u64>,
    leadership: &GroupLeadership,
) -> GroupEngineError {
    // If the reported leader is *this* node, leadership is in a transient
    // step-down/election window: redirecting the client back to ourselves
    // would just loop, so report leader-unknown and let the HTTP layer
    // answer with a retryable 503. The refusal names a leader only with the
    // term this replica's view knows it in: a redirect carries the leader
    // and its term together.
    let leader = leader_id
        .filter(|&leader| leader != leadership.node_id)
        .and_then(|node_id| {
            Some(KnownLeader {
                node_id,
                term: leadership.term_of(node_id)?,
            })
        });
    GroupEngineError::NotLeader(LeadershipRefusal {
        leader,
        proposal,
        reason,
    })
}

#[cfg(test)]
mod forwarded_answer_tests {
    use ursula_runtime::AnswerProblem;
    use ursula_runtime::ForwardedRpc;
    use ursula_runtime::GroupEngineError;
    use ursula_runtime::GroupInfraError;
    use ursula_runtime::GroupWriteResponse;
    use ursula_shard::RaftGroupId;

    use super::forwarded_answer;
    use crate::codec::encode_wire;

    /// A leader's refusal keeps its type through a forward: a purge the
    /// leader refuses while its history is pending answers `503` on the
    /// follower as on the leader.
    #[test]
    fn a_forwarded_refusal_keeps_the_leaders_type() {
        let pending = GroupEngineError::Infra(GroupInfraError::RecoveryPending {
            group: ursula_shard::RaftTarget::Data(RaftGroupId(7)),
        });
        let answer = forwarded_answer::<GroupWriteResponse>(
            ForwardedRpc::GroupWrite,
            false,
            &encode_wire(&pending),
            "bucket purge answer",
        );
        assert_eq!(answer, Err(pending));
    }

    #[test]
    fn an_undecodable_answer_is_an_invalid_forwarded_answer() {
        for ok in [true, false] {
            let answer = forwarded_answer::<GroupWriteResponse>(
                ForwardedRpc::GroupRead,
                ok,
                &[0xc1],
                "read",
            );
            assert_eq!(
                answer,
                Err(GroupEngineError::Infra(
                    GroupInfraError::ForwardedAnswerInvalid {
                        rpc: ForwardedRpc::GroupRead,
                        problem: AnswerProblem::Undecodable,
                    }
                ))
            );
        }
    }
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

#[cfg(test)]
mod leadership_refusal_tests {
    use openraft::EmptyNode;
    use ursula_runtime::GroupEngineError;
    use ursula_runtime::KnownLeader;
    use ursula_runtime::LeadershipReason;
    use ursula_runtime::LeadershipRefusal;
    use ursula_runtime::Proposal;

    use super::leadership_refusal;
    use crate::GroupLeadership;
    use crate::leadership::LeaderRole;
    use crate::openraft_error::client_write_refusal;

    /// Node 2's view in term 7, following `leader`.
    fn following(leader: u64) -> GroupLeadership {
        GroupLeadership {
            node_id: 2,
            term: 7,
            role: LeaderRole::Follows { leader_id: leader },
        }
    }

    fn leader_of(error: GroupEngineError) -> Option<KnownLeader> {
        match error {
            GroupEngineError::NotLeader(LeadershipRefusal {
                leader,
                proposal: Proposal::NotProposed,
                reason: LeadershipReason::Follower,
            }) => leader,
            other => panic!("expected a leadership refusal, got {other:?}"),
        }
    }

    fn refusal(leader_id: Option<u64>, leadership: &GroupLeadership) -> Option<KnownLeader> {
        leader_of(leadership_refusal(
            LeadershipReason::Follower,
            Proposal::NotProposed,
            leader_id,
            leadership,
        ))
    }

    /// A refusal naming the refusing node would redirect the client back to
    /// it: it names no leader.
    #[test]
    fn the_refusing_node_naming_itself_names_no_leader() {
        assert_eq!(refusal(Some(2), &following(2)), None);
    }

    /// A leader whose term this view does not know is not named: a redirect
    /// carries the leader and its term together.
    #[test]
    fn a_leader_of_an_unknown_term_is_not_named() {
        assert_eq!(refusal(Some(3), &following(4)), None);
        assert_eq!(
            refusal(Some(3), &GroupLeadership {
                node_id: 2,
                term: 7,
                role: LeaderRole::Unknown,
            }),
            None
        );
        assert_eq!(refusal(None, &following(3)), None);
    }

    #[test]
    fn a_leader_of_a_known_term_is_named_with_it() {
        assert_eq!(
            refusal(Some(3), &following(3)),
            Some(KnownLeader {
                node_id: 3,
                term: 7,
            })
        );
    }

    /// OpenRaft forwards a write only before it appends the entry, so each
    /// forward reason answers that nothing applied. Only a discarded entry
    /// may still commit.
    #[test]
    fn client_write_refusals_say_whether_openraft_appended() {
        use openraft::errors::ClientWriteError;
        use openraft::errors::ForwardReason;
        use openraft::errors::ForwardToLeader;

        let to_node_3 = |reason| ForwardToLeader::new(3, EmptyNode {}).with_reason(reason);
        // Node 2 while it still leads in term 7.
        let leading = GroupLeadership {
            node_id: 2,
            term: 7,
            role: LeaderRole::Leads,
        };
        let refused = |error, view: &GroupLeadership| match client_write_refusal(&error, view) {
            GroupEngineError::NotLeader(refusal) => refusal,
            other => panic!("expected a leadership refusal, got {other:?}"),
        };
        // A replica that stepped down follows the leader OpenRaft names.
        let stepped_down = refused(
            ClientWriteError::ForwardToLeader(to_node_3(ForwardReason::NotLeader)),
            &following(3),
        );
        assert_eq!(stepped_down.proposal, Proposal::NotProposed);
        assert_eq!(stepped_down.reason, LeadershipReason::SteppedDown);
        assert_eq!(stepped_down.leader.map(|leader| leader.node_id), Some(3));
        // A leader past its lease names no leader.
        let lease = refused(
            ClientWriteError::ForwardToLeader(
                ForwardToLeader::empty().with_reason(ForwardReason::LeaseExpired),
            ),
            &leading,
        );
        assert_eq!(lease.proposal, Proposal::NotProposed);
        assert_eq!(lease.reason, LeadershipReason::QuorumUnconfirmed);
        assert_eq!(lease.leader, None);
        // A transfer's target does not lead yet: no term, so no leader.
        let transfer = refused(
            ClientWriteError::ForwardToLeader(to_node_3(ForwardReason::LeadershipTransfer)),
            &leading,
        );
        assert_eq!(transfer.proposal, Proposal::NotProposed);
        assert_eq!(transfer.reason, LeadershipReason::LeadershipTransfer);
        assert_eq!(transfer.leader, None);
        let discarded = refused(
            ClientWriteError::LogEntryDiscarded(to_node_3(ForwardReason::NotLeader)),
            &following(3),
        );
        assert_eq!(discarded.proposal, Proposal::MayHaveCommitted);
        assert_eq!(discarded.reason, LeadershipReason::SteppedDown);
    }
}
