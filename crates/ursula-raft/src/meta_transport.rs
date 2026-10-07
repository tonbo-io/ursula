//! Dedicated meta-Raft replication and leader forwarding over gRPC.

use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;

use openraft::BasicNode;
use openraft::OptionalSend;
use openraft::RaftNetworkFactory;
use openraft::RaftNetworkV2;
use openraft::alias::SnapshotMetaOf;
use openraft::alias::SnapshotOf;
use openraft::alias::VoteOf;
use openraft::error::RPCError;
use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::error::Unreachable;
use openraft::network::RPCOption;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::TransferLeaderRequest;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::rt::WatchReceiver;
use openraft::vote::RaftLeaderId;
use serde::Deserialize;
use serde::Serialize;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::transport::Endpoint;
use ursula_control::ControlCommand;
use ursula_control::ControlPlaneState;
use ursula_control::ControlResponse;

use crate::MetaRaftError;
use crate::MetaRaftHandle;
use crate::MetaRaftTypeConfig;
use crate::raft_internal_proto as pb;

const MAX_BYTES: usize = 64 * 1024 * 1024;
const VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaRecoveryStatus {
    pub initialized: bool,
    pub vote: Option<VoteOf<MetaRaftTypeConfig>>,
    pub nonce: Option<ursula_proto::admin::ProcessIncarnation>,
}

#[derive(Debug, Serialize, Deserialize)]
enum MetaRequest {
    AuthenticatedReplication {
        identity: Option<ursula_control::ProcessIdentity>,
        request: Box<MetaRequest>,
    },
    Append(AppendEntriesRequest<MetaRaftTypeConfig>),
    Vote(VoteRequest<MetaRaftTypeConfig>),
    Snapshot {
        vote: VoteOf<MetaRaftTypeConfig>,
        meta: SnapshotMetaOf<MetaRaftTypeConfig>,
        bytes: Vec<u8>,
    },
    Transfer(TransferLeaderRequest<MetaRaftTypeConfig>),
    Read,
    Processes,
    Topology,
    Initialized,
    RecoveryStatus,
    RecoveryFloor {
        node_id: u64,
    },
    AuthorizeGenesis(ursula_proto::admin::ProcessIncarnation),
    Write(ControlCommand),
    ReconcileMembership {
        token: ursula_control::OperationToken,
        restore: bool,
    },
}

#[derive(Debug, Serialize, Deserialize)]
enum MetaResponse {
    Append(AppendEntriesResponse<MetaRaftTypeConfig>),
    Vote(VoteResponse<MetaRaftTypeConfig>),
    Snapshot(SnapshotResponse<MetaRaftTypeConfig>),
    Transferred,
    Initialized(bool),
    RecoveryStatus(MetaRecoveryStatus),
    RecoveryFloor(VoteOf<MetaRaftTypeConfig>),
    GenesisAuthorized,
    State(Box<ControlPlaneState>),
    Processes(crate::meta::ProcessEpochs),
    Topology {
        state: Box<ControlPlaneState>,
        voters: std::collections::BTreeSet<u64>,
    },
    Written(ControlResponse),
    NotLeader(Option<String>),
    MembershipReconciled,
}

#[derive(Clone)]
pub struct MetaGrpcService {
    handle: MetaRaftHandle,
}

impl MetaGrpcService {
    pub fn new(handle: MetaRaftHandle) -> pb::meta_internal_server::MetaInternalServer<Self> {
        pb::meta_internal_server::MetaInternalServer::new(Self { handle })
            .max_decoding_message_size(MAX_BYTES)
            .max_encoding_message_size(MAX_BYTES)
    }
}

#[tonic::async_trait]
impl pb::meta_internal_server::MetaInternal for MetaGrpcService {
    async fn exchange(
        &self,
        request: Request<pb::MetaRpcEnvelopeV1>,
    ) -> Result<Response<pb::MetaRpcEnvelopeV1>, Status> {
        let envelope = request.into_inner();
        if envelope.protocol_version != VERSION {
            return Err(Status::failed_precondition(
                "meta protocol version mismatch",
            ));
        }
        let request: MetaRequest = rmp_serde::from_slice(&envelope.payload)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let (identity, request) = match request {
            MetaRequest::AuthenticatedReplication { identity, request } => (identity, *request),
            request => (None, request),
        };
        if matches!(request, MetaRequest::AuthenticatedReplication { .. }) {
            return Err(Status::invalid_argument("nested meta identity envelope"));
        }
        if !self.handle.replication_enabled()
            && matches!(
                request,
                MetaRequest::Append(_)
                    | MetaRequest::Vote(_)
                    | MetaRequest::Snapshot { .. }
                    | MetaRequest::Transfer(_)
            )
        {
            return Err(Status::failed_precondition(
                "meta replica awaits durable survivor vote floor",
            ));
        }
        // This check intentionally uses applied committed state, not a recursive
        // ReadIndex. Once a quorum applies decommission, the removed process can
        // no longer advance that quorum's term using its stale membership.
        if is_removed_sender(&self.handle.committed_state().borrow(), &request)
            || is_fenced_sender(
                &self.handle.committed_state().borrow(),
                &request,
                identity.as_ref(),
            )
        {
            return Err(Status::failed_precondition(
                "meta sender process was retired or superseded",
            ));
        }
        let raft = self.handle.raft_handle();
        let response = match request {
            MetaRequest::AuthenticatedReplication { .. } => {
                return Err(Status::invalid_argument("nested meta identity envelope"));
            }
            MetaRequest::ReconcileMembership { token, restore } => {
                self.handle
                    .reconcile_operation_membership_local(token, restore)
                    .await
                    .map_err(unavailable)?;
                MetaResponse::MembershipReconciled
            }
            MetaRequest::Append(request) => {
                MetaResponse::Append(raft.append_entries(request).await.map_err(unavailable)?)
            }
            MetaRequest::Vote(request) => {
                MetaResponse::Vote(raft.vote(request).await.map_err(unavailable)?)
            }
            MetaRequest::Snapshot { vote, meta, bytes } => MetaResponse::Snapshot(
                raft.install_full_snapshot(vote, SnapshotOf::<MetaRaftTypeConfig> {
                    meta,
                    snapshot: Cursor::new(bytes),
                })
                .await
                .map_err(unavailable)?,
            ),
            MetaRequest::Transfer(request) => {
                raft.handle_transfer_leader(request)
                    .await
                    .map_err(unavailable)?;
                MetaResponse::Transferred
            }
            MetaRequest::RecoveryFloor { node_id } => {
                let before = raft.metrics().borrow_watched().clone();
                if self
                    .handle
                    .committed_state()
                    .borrow()
                    .nodes
                    .get(&before.id)
                    .is_some_and(|node| node.state == ursula_control::NodeState::Removed)
                {
                    return Err(Status::failed_precondition(
                        "removed meta replica cannot authorize recovery",
                    ));
                }
                let snapshot = self
                    .handle
                    .read_linearizable_local()
                    .await
                    .map_err(unavailable)?;
                let authorized = snapshot.operations.active.as_ref().is_some_and(|operation|
                    operation.phase == ursula_control::OperationPhase::Retired
                    && matches!(operation.kind, ursula_control::OperationKind::RebuildReplica { node_id: source } if source == node_id))
                    && matches!(snapshot.operations.processes.get(&node_id), Some(ursula_control::ProcessState::Retired { reason: ursula_control::RetirementReason::Rebuild, .. }));
                let membership = before.membership_config.membership();
                if !authorized
                    || membership.voter_ids().any(|voter| voter == node_id)
                    || membership.get_node(&node_id).is_none()
                {
                    return Err(Status::failed_precondition(
                        "fresh meta disk requires an authorized retired rebuild learner",
                    ));
                }
                raft.trigger()
                    .allow_next_revert(&node_id, true)
                    .await
                    .map_err(unavailable)?
                    .map_err(unavailable)?;
                let vote = self
                    .handle
                    .recovery_status()
                    .await
                    .map_err(unavailable)?
                    .vote
                    .ok_or_else(|| Status::unavailable("meta leader lacks durable vote"))?;
                let after = raft.metrics().borrow_watched().clone();
                if !vote.is_committed()
                    || vote != before.vote
                    || after.vote != vote
                    || after.current_leader != Some(after.id)
                {
                    return Err(Status::unavailable(
                        "meta leadership changed during recovery confirmation",
                    ));
                }
                MetaResponse::RecoveryFloor(vote)
            }
            MetaRequest::RecoveryStatus => MetaResponse::RecoveryStatus(
                self.handle.recovery_status().await.map_err(unavailable)?,
            ),
            MetaRequest::AuthorizeGenesis(nonce) => {
                self.handle
                    .authorize_genesis(&nonce)
                    .await
                    .map_err(unavailable)?;
                MetaResponse::GenesisAuthorized
            }
            MetaRequest::Initialized => {
                MetaResponse::Initialized(raft.is_initialized().await.map_err(unavailable)?)
            }
            MetaRequest::Processes => MetaResponse::Processes(
                (*self
                    .handle
                    .read_linearizable_processes_local()
                    .await
                    .map_err(unavailable)?)
                .clone(),
            ),
            MetaRequest::Topology => {
                let (state, voters) = self
                    .handle
                    .read_linearizable_topology_local()
                    .await
                    .map_err(unavailable)?;
                MetaResponse::Topology {
                    state: Box::new(state),
                    voters,
                }
            }
            MetaRequest::Read => MetaResponse::State(Box::new(
                self.handle
                    .read_linearizable_local()
                    .await
                    .map_err(unavailable)?,
            )),
            MetaRequest::Write(command) => match self.handle.write_local(command).await {
                Ok(response) => MetaResponse::Written(response),
                Err(error) => match error.redirect() {
                    crate::meta::MetaRedirect::NotLeader(endpoint) => {
                        MetaResponse::NotLeader(endpoint.clone())
                    }
                    crate::meta::MetaRedirect::None => return Err(unavailable(error)),
                },
            },
        };
        Ok(Response::new(pb::MetaRpcEnvelopeV1 {
            protocol_version: VERSION,
            payload: rmp_serde::to_vec_named(&response)
                .map_err(|error| Status::internal(error.to_string()))?
                .into(),
        }))
    }
}

fn is_removed_sender(state: &ControlPlaneState, request: &MetaRequest) -> bool {
    let vote = match request {
        MetaRequest::Append(request) => &request.vote,
        MetaRequest::Vote(request) => &request.vote,
        MetaRequest::Snapshot { vote, .. } => vote,
        MetaRequest::Transfer(request) => request.from_leader(),
        MetaRequest::AuthenticatedReplication { .. }
        | MetaRequest::Read
        | MetaRequest::Processes
        | MetaRequest::Topology
        | MetaRequest::Initialized
        | MetaRequest::Write(_)
        | MetaRequest::ReconcileMembership { .. }
        | MetaRequest::RecoveryStatus
        | MetaRequest::RecoveryFloor { .. }
        | MetaRequest::AuthorizeGenesis(_) => return false,
    };
    let sender = vote.leader_id().node_id();
    state
        .nodes
        .get(sender)
        .is_some_and(|node| node.state == ursula_control::NodeState::Removed)
}

fn is_fenced_sender(
    state: &ControlPlaneState,
    request: &MetaRequest,
    identity: Option<&ursula_control::ProcessIdentity>,
) -> bool {
    let vote = match request {
        MetaRequest::Append(request) => &request.vote,
        MetaRequest::Vote(request) => &request.vote,
        MetaRequest::Snapshot { vote, .. } => vote,
        MetaRequest::Transfer(request) => request.from_leader(),
        _ => return false,
    };
    // Process epochs are a monotonic floor, not an equality barrier: a lagging
    // receiver must accept the newer process carrying its own ClaimProcess log.
    // Peer RPCs are trusted/non-Byzantine; identities are pinned from committed
    // claims by startup, never adopted from a watch. Once the claim/retirement
    // applies on a quorum, the old process cannot advance that quorum's term.
    match state.operations.processes.get(vote.leader_id().node_id()) {
        Some(ursula_control::ProcessState::Active(current)) => identity.is_none_or(|sender| {
            sender.epoch < current.epoch || (sender.epoch == current.epoch && sender != current)
        }),
        Some(ursula_control::ProcessState::Retired { epoch, .. }) => {
            identity.is_none_or(|sender| sender.epoch <= *epoch)
        }
        None => false,
    }
}

fn unavailable(error: impl std::fmt::Display) -> Status {
    Status::unavailable(error.to_string())
}

async fn exchange(endpoint: &str, request: MetaRequest) -> Result<MetaResponse, MetaRaftError> {
    static CHANNELS: OnceLock<Mutex<BTreeMap<String, tonic::transport::Channel>>> = OnceLock::new();
    let channel = {
        let mut channels = CHANNELS
            .get_or_init(Mutex::default)
            .lock()
            .map_err(|_poisoned| MetaRaftError::new("meta channel cache", "lock poisoned"))?;
        if let Some(channel) = channels.get(endpoint) {
            channel.clone()
        } else {
            let channel = Endpoint::from_shared(endpoint.to_owned())
                .map_err(|error| MetaRaftError::with_source("meta endpoint", error))?
                .connect_timeout(Duration::from_secs(2))
                .timeout(Duration::from_secs(10))
                .connect_lazy();
            channels.insert(endpoint.to_owned(), channel.clone());
            channel
        }
    };
    let mut client = pb::meta_internal_client::MetaInternalClient::new(channel)
        .max_decoding_message_size(MAX_BYTES)
        .max_encoding_message_size(MAX_BYTES);
    let payload = rmp_serde::to_vec_named(&request)
        .map_err(|error| MetaRaftError::with_source("encode meta request", error))?;
    let response = client
        .exchange(pb::MetaRpcEnvelopeV1 {
            protocol_version: VERSION,
            payload: payload.into(),
        })
        .await
        .map_err(|error| MetaRaftError::with_source("meta RPC", error))?
        .into_inner();
    if response.protocol_version != VERSION {
        return Err(MetaRaftError::new("meta RPC", "protocol version mismatch"));
    }
    rmp_serde::from_slice(&response.payload)
        .map_err(|error| MetaRaftError::with_source("decode meta response", error))
}

fn mismatch() -> MetaRaftError {
    MetaRaftError::new("meta RPC", "response kind does not match request")
}

/// A successful fresh leader quorum confirmation dominates every previously
/// quorum-established term, including when the voter configuration changed.
pub async fn meta_peer_recovery_floor(
    endpoint: &str,
    node_id: u64,
) -> Result<VoteOf<MetaRaftTypeConfig>, MetaRaftError> {
    match exchange(endpoint, MetaRequest::RecoveryFloor { node_id }).await? {
        MetaResponse::RecoveryFloor(vote) => Ok(vote),
        _ => Err(mismatch()),
    }
}

pub async fn meta_peer_recovery_status(
    endpoint: &str,
) -> Result<MetaRecoveryStatus, MetaRaftError> {
    match exchange(endpoint, MetaRequest::RecoveryStatus).await? {
        MetaResponse::RecoveryStatus(status) => Ok(status),
        _ => Err(mismatch()),
    }
}

pub async fn meta_authorize_genesis(
    endpoint: &str,
    nonce: ursula_proto::admin::ProcessIncarnation,
) -> Result<(), MetaRaftError> {
    match exchange(endpoint, MetaRequest::AuthorizeGenesis(nonce)).await? {
        MetaResponse::GenesisAuthorized => Ok(()),
        _ => Err(mismatch()),
    }
}

pub async fn meta_peer_initialized(endpoint: &str) -> Result<bool, MetaRaftError> {
    match exchange(endpoint, MetaRequest::Initialized).await? {
        MetaResponse::Initialized(initialized) => Ok(initialized),
        _ => Err(mismatch()),
    }
}

pub(crate) async fn forward_membership(
    endpoint: &str,
    token: ursula_control::OperationToken,
    restore: bool,
) -> Result<(), MetaRaftError> {
    match exchange(endpoint, MetaRequest::ReconcileMembership {
        token,
        restore,
    })
    .await?
    {
        MetaResponse::MembershipReconciled => Ok(()),
        _ => Err(mismatch()),
    }
}

pub(crate) async fn forward_processes(
    endpoint: &str,
) -> Result<crate::meta::ProcessEpochs, MetaRaftError> {
    match exchange(endpoint, MetaRequest::Processes).await? {
        MetaResponse::Processes(processes) => Ok(processes),
        _ => Err(mismatch()),
    }
}

pub(crate) async fn forward_topology(
    endpoint: &str,
) -> Result<(ControlPlaneState, std::collections::BTreeSet<u64>), MetaRaftError> {
    match exchange(endpoint, MetaRequest::Topology).await? {
        MetaResponse::Topology { state, voters } => Ok((*state, voters)),
        _ => Err(mismatch()),
    }
}

pub(crate) async fn forward_read(endpoint: &str) -> Result<ControlPlaneState, MetaRaftError> {
    match exchange(endpoint, MetaRequest::Read).await? {
        MetaResponse::State(state) => Ok(*state),
        _ => Err(mismatch()),
    }
}
pub(crate) async fn forward_write(
    endpoint: &str,
    command: ControlCommand,
) -> Result<ControlResponse, MetaRaftError> {
    match exchange(endpoint, MetaRequest::Write(command)).await? {
        MetaResponse::Written(response) => Ok(response),
        MetaResponse::NotLeader(endpoint) => Err(MetaRaftError::not_leader(endpoint)),
        _ => Err(mismatch()),
    }
}

#[derive(Clone, Default)]
pub struct MetaGrpcNetworkFactory {
    pub(crate) identity: Arc<Mutex<Option<ursula_control::ProcessIdentity>>>,
}
pub struct MetaGrpcNetwork {
    endpoint: String,
    identity: Arc<Mutex<Option<ursula_control::ProcessIdentity>>>,
}
impl RaftNetworkFactory<MetaRaftTypeConfig> for MetaGrpcNetworkFactory {
    type Network = MetaGrpcNetwork;
    async fn new_client(&mut self, _target: u64, node: &BasicNode) -> Self::Network {
        MetaGrpcNetwork {
            endpoint: node.addr.clone(),
            identity: self.identity.clone(),
        }
    }
}
impl MetaGrpcNetwork {
    async fn exchange(&self, request: MetaRequest) -> Result<MetaResponse, MetaRaftError> {
        let identity = self
            .identity
            .lock()
            .map_err(|_poisoned| MetaRaftError::new("meta sender identity", "lock poisoned"))?
            .clone();
        exchange(&self.endpoint, MetaRequest::AuthenticatedReplication {
            identity,
            request: Box::new(request),
        })
        .await
    }
}
fn rpc_error(error: MetaRaftError) -> RPCError<MetaRaftTypeConfig> {
    RPCError::Unreachable(Unreachable::new(&error))
}
impl RaftNetworkV2<MetaRaftTypeConfig> for MetaGrpcNetwork {
    async fn append_entries(
        &mut self,
        request: AppendEntriesRequest<MetaRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<MetaRaftTypeConfig>, RPCError<MetaRaftTypeConfig>> {
        match self
            .exchange(MetaRequest::Append(request))
            .await
            .map_err(rpc_error)?
        {
            MetaResponse::Append(response) => Ok(response),
            _ => Err(rpc_error(mismatch())),
        }
    }
    async fn vote(
        &mut self,
        request: VoteRequest<MetaRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<MetaRaftTypeConfig>, RPCError<MetaRaftTypeConfig>> {
        match self
            .exchange(MetaRequest::Vote(request))
            .await
            .map_err(rpc_error)?
        {
            MetaResponse::Vote(response) => Ok(response),
            _ => Err(rpc_error(mismatch())),
        }
    }
    async fn full_snapshot(
        &mut self,
        vote: VoteOf<MetaRaftTypeConfig>,
        snapshot: SnapshotOf<MetaRaftTypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<MetaRaftTypeConfig>, StreamingError<MetaRaftTypeConfig>> {
        match self
            .exchange(MetaRequest::Snapshot {
                vote,
                meta: snapshot.meta,
                bytes: snapshot.snapshot.into_inner(),
            })
            .await
            .map_err(|error| StreamingError::Unreachable(Unreachable::new(&error)))?
        {
            MetaResponse::Snapshot(response) => Ok(response),
            _ => Err(StreamingError::Unreachable(Unreachable::new(
                &io::Error::other("meta snapshot response mismatch"),
            ))),
        }
    }
    async fn transfer_leader(
        &mut self,
        request: TransferLeaderRequest<MetaRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<(), RPCError<MetaRaftTypeConfig>> {
        match self
            .exchange(MetaRequest::Transfer(request))
            .await
            .map_err(rpc_error)?
        {
            MetaResponse::Transferred => Ok(()),
            _ => Err(rpc_error(mismatch())),
        }
    }
}

#[cfg(all(test, not(madsim)))]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use openraft::BasicNode;
    use openraft::raft::AppendEntriesRequest;
    use openraft::raft::TransferLeaderRequest;
    use openraft::raft::VoteRequest;
    use openraft::rt::WatchReceiver;
    use tonic::Request;
    use ursula_control::ControlCommand;

    use super::MetaGrpcService;
    use super::MetaRequest;
    use super::VERSION;
    use super::pb;
    use crate::MetaRaftHandle;
    use crate::raft_internal_proto::meta_internal_server::MetaInternal;

    #[tokio::test]
    async fn fresh_meta_replica_rejects_rpc_until_durable_vote_floor_and_rejects_stale_genesis() {
        let root = tempfile::tempdir().unwrap();
        let nonce = ursula_proto::admin::ProcessIncarnation::from_bits(77);
        let handle = MetaRaftHandle::new_durable_recovering(
            2,
            root.path().to_owned(),
            std::sync::Arc::new(openraft::Config::default()),
            nonce,
        )
        .await
        .unwrap();
        let service = MetaGrpcService {
            handle: handle.clone(),
        };
        let request = MetaRequest::Vote(VoteRequest {
            vote: openraft::Vote::new(1, 1),
            last_log_id: None,
        });
        let rejected = service
            .exchange(Request::new(pb::MetaRpcEnvelopeV1 {
                protocol_version: VERSION,
                payload: rmp_serde::to_vec_named(&request).unwrap().into(),
            }))
            .await
            .expect_err("fresh node must not grant an old term vote");
        assert_eq!(rejected.code(), tonic::Code::FailedPrecondition);
        handle
            .authorize_genesis(&ursula_proto::admin::ProcessIncarnation::from_bits(76))
            .await
            .expect_err("delayed permit for previous boot rejected");
        assert!(!handle.replication_enabled());
        let floor = openraft::Vote::new_committed(50, 3);
        handle.install_recovery_vote_floor(floor).await.unwrap();
        assert!(handle.replication_enabled());
        assert_eq!(handle.recovery_status().await.unwrap().vote, Some(floor));
        let response = handle
            .raft_handle()
            .vote(VoteRequest {
                vote: openraft::Vote::new(49, 1),
                last_log_id: None,
            })
            .await
            .unwrap();
        assert!(!response.vote_granted);
        assert_eq!(response.vote, floor);
        handle.shutdown().await.unwrap();
        drop(service);
        drop(handle);
        let reopened = MetaRaftHandle::new_durable_recovering(
            2,
            root.path().to_owned(),
            std::sync::Arc::new(openraft::Config::default()),
            ursula_proto::admin::ProcessIncarnation::from_bits(78),
        )
        .await
        .unwrap();
        assert_eq!(reopened.recovery_status().await.unwrap().vote, Some(floor));
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn superseded_meta_process_cannot_raise_survivor_term() {
        let root = tempfile::tempdir().unwrap();
        let handle = MetaRaftHandle::new_durable(
            1,
            root.path().to_owned(),
            std::sync::Arc::new(openraft::Config::default()),
        )
        .await
        .unwrap();
        handle
            .initialize_membership(BTreeMap::from([(1, BasicNode::new("http://unused"))]))
            .await
            .unwrap();
        handle
            .wait_for_current_leader(1, Duration::from_secs(3))
            .await
            .unwrap();
        handle
            .register_node(
                crate::MetaNodeRegistration::new(2, "http://old", "http://old"),
                0,
            )
            .await
            .unwrap();
        let mut identities = Vec::new();
        for epoch in 0..2 {
            let response = handle
                .write(ControlCommand::Operation {
                    command: ursula_control::OperationCommand::ClaimProcess {
                        node_id: 2,
                        expected_epoch: epoch,
                        incarnation: ursula_proto::admin::ProcessIncarnation::from_bits(
                            u128::from(epoch) + 10,
                        ),
                    },
                    now_ms: epoch,
                })
                .await
                .unwrap();
            let ursula_control::ControlResponse::Operation(Ok(
                ursula_control::OperationOutcome::ProcessClaimed(identity),
            )) = response
            else {
                panic!("claim rejected: {response:?}")
            };
            identities.push(identity);
        }
        let term = handle.raft_handle().metrics().borrow_watched().current_term;
        let service = MetaGrpcService {
            handle: handle.clone(),
        };
        for identity in [None, Some(identities[0].clone())] {
            let request = MetaRequest::AuthenticatedReplication {
                identity,
                request: Box::new(MetaRequest::Vote(VoteRequest {
                    vote: openraft::Vote::new(term.saturating_add(100), 2),
                    last_log_id: None,
                })),
            };
            let error = service
                .exchange(Request::new(pb::MetaRpcEnvelopeV1 {
                    protocol_version: VERSION,
                    payload: rmp_serde::to_vec_named(&request).unwrap().into(),
                }))
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
            assert_eq!(
                handle.raft_handle().metrics().borrow_watched().current_term,
                term
            );
        }
        let request = MetaRequest::Vote(VoteRequest {
            vote: openraft::Vote::new(term.saturating_add(100), 2),
            last_log_id: None,
        });
        assert!(!super::is_fenced_sender(
            &handle.committed_state().borrow(),
            &request,
            Some(&identities[1])
        ));
        handle.shutdown().await.unwrap();
    }

    #[test]
    fn retirement_epoch_fences_old_process_but_allows_a_committed_rebuild_to_catch_up() {
        let mut state = ursula_control::ControlPlaneState::default();
        state
            .operations
            .processes
            .insert(2, ursula_control::ProcessState::Retired {
                epoch: 7,
                reason: ursula_control::RetirementReason::Rebuild,
            });
        let request = MetaRequest::Vote(VoteRequest {
            vote: openraft::Vote::new(100, 2),
            last_log_id: None,
        });
        assert!(super::is_fenced_sender(&state, &request, None));
        for epoch in [6, 7] {
            let identity = ursula_control::ProcessIdentity {
                epoch,
                incarnation: ursula_proto::admin::ProcessIncarnation::from_bits(10),
            };
            assert!(super::is_fenced_sender(&state, &request, Some(&identity)));
        }
        let rebuilt = ursula_control::ProcessIdentity {
            epoch: 8,
            incarnation: ursula_proto::admin::ProcessIncarnation::from_bits(11),
        };
        assert!(!super::is_fenced_sender(&state, &request, Some(&rebuilt)));
    }

    #[tokio::test]
    async fn newer_process_can_deliver_its_missing_claim_to_a_lagging_meta_replica() {
        use openraft::RaftTypeConfig;
        use openraft::entry::RaftEntry;
        use openraft::vote::RaftLeaderId;
        let root = tempfile::tempdir().unwrap();
        let handle = MetaRaftHandle::new_durable(
            1,
            root.path().to_owned(),
            std::sync::Arc::new(openraft::Config::default()),
        )
        .await
        .unwrap();
        handle
            .initialize_membership(BTreeMap::from([(1, BasicNode::new("http://unused"))]))
            .await
            .unwrap();
        handle
            .wait_for_current_leader(1, Duration::from_secs(3))
            .await
            .unwrap();
        handle
            .register_node(
                crate::MetaNodeRegistration::new(2, "http://peer", "http://peer"),
                0,
            )
            .await
            .unwrap();
        handle
            .write(ControlCommand::Operation {
                command: ursula_control::OperationCommand::ClaimProcess {
                    node_id: 2,
                    expected_epoch: 0,
                    incarnation: ursula_proto::admin::ProcessIncarnation::from_bits(10),
                },
                now_ms: 0,
            })
            .await
            .unwrap();
        let metrics = handle.raft_handle().metrics().borrow_watched().clone();
        let prev = metrics.last_applied.unwrap();
        let term = metrics.current_term.saturating_add(100);
        type Config = crate::MetaRaftTypeConfig;
        let next = openraft::LogId {
            leader_id: <Config as RaftTypeConfig>::LeaderId::new(term, 2),
            index: prev.index.saturating_add(1),
        };
        let identity = ursula_control::ProcessIdentity {
            epoch: 2,
            incarnation: ursula_proto::admin::ProcessIncarnation::from_bits(11),
        };
        let entry = <Config as RaftTypeConfig>::Entry::new(
            next,
            openraft::EntryPayload::Normal(ControlCommand::Operation {
                command: ursula_control::OperationCommand::ClaimProcess {
                    node_id: 2,
                    expected_epoch: 1,
                    incarnation: identity.incarnation.clone(),
                },
                now_ms: 1,
            }),
        );
        let request = MetaRequest::AuthenticatedReplication {
            identity: Some(identity.clone()),
            request: Box::new(MetaRequest::Append(AppendEntriesRequest {
                vote: openraft::Vote::new_committed(term, 2),
                prev_log_id: Some(prev),
                entries: vec![entry],
                leader_commit: Some(next),
            })),
        };
        let service = MetaGrpcService {
            handle: handle.clone(),
        };
        service
            .exchange(Request::new(pb::MetaRpcEnvelopeV1 {
                protocol_version: VERSION,
                payload: rmp_serde::to_vec_named(&request).unwrap().into(),
            }))
            .await
            .unwrap();
        handle
            .raft_handle()
            .wait(Some(Duration::from_secs(3)))
            .applied_index_at_least(Some(next.index), "new claim catches up")
            .await
            .unwrap();
        assert!(
            handle
                .committed_state()
                .borrow()
                .operations
                .accepts_process(2, &identity)
        );
        let stale = ursula_control::ProcessIdentity {
            epoch: 1,
            incarnation: ursula_proto::admin::ProcessIncarnation::from_bits(10),
        };
        assert!(super::is_fenced_sender(
            &handle.committed_state().borrow(),
            &MetaRequest::Vote(VoteRequest {
                vote: openraft::Vote::new(term.saturating_add(1), 2),
                last_log_id: Some(next)
            }),
            Some(&stale)
        ));
        handle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn removed_meta_sender_cannot_raise_survivor_term() {
        let root = tempfile::tempdir().unwrap();
        let handle = MetaRaftHandle::new_durable(
            1,
            root.path().to_owned(),
            std::sync::Arc::new(openraft::Config::default()),
        )
        .await
        .unwrap();
        handle
            .initialize_membership(BTreeMap::from([(1, BasicNode::new("http://unused"))]))
            .await
            .unwrap();
        handle
            .wait_for_current_leader(1, Duration::from_secs(3))
            .await
            .unwrap();
        handle
            .register_node(
                crate::MetaNodeRegistration::new(2, "http://removed", "http://removed"),
                0,
            )
            .await
            .unwrap();
        handle
            .write(ControlCommand::SetNodeState {
                node_id: 2,
                state: ursula_control::NodeState::Removed,
                now_ms: 1,
            })
            .await
            .unwrap();
        let term = handle.raft_handle().metrics().borrow_watched().current_term;
        let service = MetaGrpcService {
            handle: handle.clone(),
        };
        let vote = openraft::Vote::new_committed(term.saturating_add(100), 2);
        for request in [
            MetaRequest::Vote(VoteRequest {
                vote,
                last_log_id: None,
            }),
            MetaRequest::Append(AppendEntriesRequest {
                vote,
                prev_log_id: None,
                entries: vec![],
                leader_commit: None,
            }),
            MetaRequest::Transfer(TransferLeaderRequest::new(vote, 1, None)),
        ] {
            let error = service
                .exchange(Request::new(pb::MetaRpcEnvelopeV1 {
                    protocol_version: VERSION,
                    payload: rmp_serde::to_vec_named(&request).unwrap().into(),
                }))
                .await
                .expect_err("removed sender rejected before Raft");
            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
            assert_eq!(
                handle.raft_handle().metrics().borrow_watched().current_term,
                term
            );
        }
        handle
            .register_node(
                crate::MetaNodeRegistration::new(1, "http://local", "http://local"),
                1,
            )
            .await
            .unwrap();
        handle
            .write(ControlCommand::SetNodeState {
                node_id: 1,
                state: ursula_control::NodeState::Removed,
                now_ms: 2,
            })
            .await
            .unwrap();
        let error = service
            .exchange(Request::new(pb::MetaRpcEnvelopeV1 {
                protocol_version: VERSION,
                payload: rmp_serde::to_vec_named(&MetaRequest::RecoveryFloor { node_id: 2 })
                    .unwrap()
                    .into(),
            }))
            .await
            .expect_err("removed peer cannot authorize a fresh disk");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        handle.shutdown().await.unwrap();
    }
}
