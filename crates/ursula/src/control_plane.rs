//! Meta-Raft operation API and server-collected replication evidence.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

use axum::Json;
use axum::extract::Path;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use serde::Deserialize;
use serde::Serialize;
use ursula_control::ActionRejection;
use ursula_control::ControlCommand;
use ursula_control::ControlPlaneState;
use ursula_control::ControlResponse;
use ursula_control::OperationCommand;
use ursula_control::OperationError;
use ursula_control::OperationKind;
use ursula_control::OperationPhase;
use ursula_control::OperationRequest;
use ursula_control::OperationToken;
use ursula_control::PrefixEvidence;
use ursula_control::ProcessIdentity;
use ursula_control::ProcessState;
use ursula_control::ReplicaEvidence;
use ursula_proto::admin::PROCESS_INCARNATION_HEADER;
use ursula_proto::admin::QuorumPrefix;
use ursula_proto::admin::RaftGroupMetrics;
use ursula_shard::RaftGroupId;

use crate::HttpState;

pub(crate) const OPERATION_PATH: &str = "/__ursula/control/operation";
pub(crate) const STATE_PATH: &str = "/__ursula/control/state";
pub(crate) const EVIDENCE_PATH: &str = "/__ursula/control/group/{group}/evidence";

#[derive(Debug, thiserror::Error)]
enum ControlHttpError {
    #[error("meta Raft is not configured")]
    Unavailable,
    #[error("no data leader with a fresh quorum proof for group {group:?}")]
    NoDataLeader { group: RaftGroupId },
    #[error("meta Raft request failed: {0}")]
    Meta(#[from] ursula_raft::MetaRaftError),
    #[error("prepare replica failed: {0}")]
    Runtime(#[from] ursula_runtime::RuntimeError),
    #[error("quorum prefix confirmation failed: {0}")]
    Quorum(#[from] ursula_raft::QuorumProofError),
    #[error("{0}")]
    Operation(#[from] OperationError),
    #[error("node {node_id} evidence request failed: {source}")]
    Peer {
        node_id: u64,
        #[source]
        source: reqwest::Error,
    },
    #[error("remote data leadership changed: {detail}")]
    RemoteLeadershipChanged { detail: String },
    #[error("action receipt is drained")]
    ActionDrained,
    #[error("data leadership changed: {0}")]
    LeadershipChanged(#[source] ursula_raft::MetaRaftError),
    #[error("node {node_id} action {action:?} returned {status}: {body}")]
    ActionRejected {
        node_id: u64,
        action: ursula_control::MembershipAction,
        status: StatusCode,
        body: String,
    },
    #[error(
        "group {group:?} evidence rejected: leader={leader}, voters={voters:?}, prefix={index}, floor={floor:?}, observed_ms={observed_ms}, command_ms={command_ms}: {source}"
    )]
    EvidenceRejected {
        group: RaftGroupId,
        leader: u64,
        voters: BTreeSet<u64>,
        index: u64,
        floor: Option<u64>,
        observed_ms: u64,
        command_ms: u64,
        #[source]
        source: OperationError,
    },
    #[error("node {node_id} has no current process or group proof")]
    MissingPeer { node_id: u64 },
}

impl IntoResponse for ControlHttpError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::Operation(_) | Self::MissingPeer { .. } | Self::EvidenceRejected { .. } => {
                StatusCode::CONFLICT
            }
            _ => StatusCode::SERVICE_UNAVAILABLE,
        };
        if matches!(self, Self::ActionDrained) {
            return (StatusCode::CONFLICT, Json(ActionRejection::Drained)).into_response();
        }
        if let Self::LeadershipChanged(source) = self {
            return (
                StatusCode::CONFLICT,
                Json(ActionRejection::LeadershipChanged {
                    detail: source.to_string(),
                }),
            )
                .into_response();
        }
        (status, self.to_string()).into_response()
    }
}

fn action_raft_error(
    operation: &'static str,
    source: openraft::error::RaftError<
        ursula_raft::UrsulaRaftTypeConfig,
        openraft::error::ClientWriteError<ursula_raft::UrsulaRaftTypeConfig>,
    >,
) -> ControlHttpError {
    let forwarded = source.forward_to_leader().is_some();
    let error = ursula_raft::MetaRaftError::with_source(operation, source);
    if forwarded {
        ControlHttpError::LeadershipChanged(error)
    } else {
        ControlHttpError::Meta(error)
    }
}

async fn linear_state(state: &HttpState) -> Result<ControlPlaneState, ControlHttpError> {
    let meta = state
        .meta_control
        .as_ref()
        .ok_or(ControlHttpError::Unavailable)?;
    Ok(meta.read_linearizable_state().await?)
}

pub(crate) async fn state(State(state): State<HttpState>) -> Response {
    match linear_state(&state).await {
        Ok(snapshot) => Json(snapshot).into_response(),
        Err(error) => error.into_response(),
    }
}

pub(crate) async fn operation(
    State(state): State<HttpState>,
    Json(request): Json<OperationRequest>,
) -> Response {
    match apply_request(&state, request).await {
        Ok(response) => (
            if response.is_rejected() {
                StatusCode::CONFLICT
            } else {
                StatusCode::OK
            },
            Json(response),
        )
            .into_response(),
        Err(error) => error.into_response(),
    }
}

async fn apply_request(
    state: &HttpState,
    request: OperationRequest,
) -> Result<ControlResponse, ControlHttpError> {
    let snapshot = linear_state(state).await?;
    let meta = state
        .meta_control
        .as_ref()
        .ok_or(ControlHttpError::Unavailable)?;
    let command = match request {
        OperationRequest::JoinNode {
            node_id,
            client_url,
            cluster_url,
            meta_url,
        } => {
            return Ok(meta
                .join_node(
                    ursula_raft::MetaNodeRegistration::new(node_id, client_url, cluster_url),
                    meta_url,
                    state.wall_clock.unix_time_ms(),
                )
                .await?);
        }

        OperationRequest::RecoverAction { token } => {
            let bound = snapshot
                .operations
                .active
                .as_ref()
                .filter(|operation| operation.token == token)
                .and_then(|operation| operation.pending_action.as_ref())
                .ok_or(OperationError::InvalidTransition)?;
            let proofs =
                collect_evidence(state, &snapshot, &token, Some(bound.leader), None).await?;
            for evidence in proofs {
                let response = meta
                    .write(ControlCommand::Operation {
                        command: OperationCommand::Observe {
                            token: token.clone(),
                            evidence,
                        },
                        now_ms: state.wall_clock.unix_time_ms(),
                    })
                    .await?;
                if response.is_rejected() {
                    return Ok(response);
                }
            }
            let node_id = bound.leader;
            let node = snapshot
                .nodes
                .get(&node_id)
                .ok_or(ControlHttpError::MissingPeer { node_id })?;
            let drained: ursula_control::OperationAction = reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|source| ControlHttpError::Peer { node_id, source })?
                .post(format!(
                    "{}{}",
                    node.cluster_url.trim_end_matches('/'),
                    ACTION_DRAIN_PATH
                ))
                .header(
                    PROCESS_INCARNATION_HEADER,
                    bound.process.incarnation.as_str(),
                )
                .json(&ursula_control::ActionRequest {
                    token: token.clone(),
                    action: bound.clone(),
                })
                .send()
                .await
                .and_then(reqwest::Response::error_for_status)
                .map_err(|source| ControlHttpError::Peer { node_id, source })?
                .json()
                .await
                .map_err(|source| ControlHttpError::Peer { node_id, source })?;
            if &drained != bound {
                return Err(OperationError::InvalidTransition.into());
            }
            OperationCommand::RetireActionExecutor { token, drained }
        }
        OperationRequest::Reconcile { token } => return reconcile(state, snapshot, token).await,
        OperationRequest::Begin { kind, executor } => {
            let (current, meta_voters) = meta.read_linearizable_topology().await?;
            let participants = participants(&current, &kind)?;
            OperationCommand::Begin {
                kind,
                executor,
                participants,
                meta_voters,
            }
        }
        OperationRequest::TakeOver { expected, executor } => {
            OperationCommand::TakeOver { expected, executor }
        }
        OperationRequest::RetireSource { token } => {
            let command = OperationCommand::RetireSource {
                token: token.clone(),
            };
            let mut preview = snapshot.clone();
            let response = preview.apply(ControlCommand::Operation {
                command: command.clone(),
                now_ms: state.wall_clock.unix_time_ms(),
            });
            let resuming_retirement = matches!(
                response,
                ControlResponse::Operation(Err(OperationError::Busy))
            ) && snapshot
                .operations
                .active
                .as_ref()
                .and_then(|op| op.pending_action.as_ref())
                .is_some_and(|action| {
                    matches!(
                        action.action,
                        ursula_control::MembershipAction::RetireReplica
                    )
                });
            if response.is_rejected() && !resuming_retirement {
                return Ok(response);
            }
            if matches!(
                snapshot.operations.active.as_ref().map(|op| &op.kind),
                Some(OperationKind::RebuildReplica { .. })
            ) {
                retire_data_replica(state, snapshot.clone(), &token).await?;
            }
            meta.reconcile_operation_membership(token, false).await?;
            command
        }
        OperationRequest::Complete { token } => OperationCommand::Complete { token },
        OperationRequest::CollectEvidence { token } => {
            let proofs = collect_evidence_with_retry(state, &snapshot, &token, None, None).await?;
            for evidence in proofs {
                let response = meta
                    .write(ControlCommand::Operation {
                        command: OperationCommand::Observe {
                            token: token.clone(),
                            evidence,
                        },
                        now_ms: state.wall_clock.unix_time_ms(),
                    })
                    .await?;
                if response.is_rejected() {
                    return Ok(response);
                }
            }
            return Ok(ControlResponse::Operation(Ok(
                ursula_control::OperationOutcome::EvidenceRecorded,
            )));
        }
    };
    Ok(meta
        .write(ControlCommand::Operation {
            command,
            now_ms: state.wall_clock.unix_time_ms(),
        })
        .await?)
}

fn participants(
    snapshot: &ControlPlaneState,
    kind: &OperationKind,
) -> Result<BTreeMap<u64, ProcessIdentity>, ControlHttpError> {
    let (source, selected) = match kind {
        OperationKind::MoveReplicas { source, groups, .. } => (*source, Some(groups)),
        OperationKind::RebuildReplica { node_id }
        | OperationKind::DecommissionNode { node_id, .. } => (*node_id, None),
    };
    let mut required = snapshot
        .placements
        .iter()
        .filter(|(group, placement)| {
            placement.voters.contains(&source)
                && selected.is_none_or(|selected| selected.contains(group))
        })
        .flat_map(|(_, placement)| placement.voters.iter().copied())
        .collect::<BTreeSet<_>>();
    required.insert(source);
    match kind {
        OperationKind::MoveReplicas { target, .. } => {
            required.insert(*target);
        }
        OperationKind::DecommissionNode { replacements, .. } => {
            required.extend(replacements.values().copied())
        }
        OperationKind::RebuildReplica { .. } => {}
    }
    required
        .into_iter()
        .map(
            |node_id| match snapshot.operations.processes.get(&node_id) {
                Some(ProcessState::Active(identity)) => Ok((node_id, identity.clone())),
                _ => Err(ControlHttpError::MissingPeer { node_id }),
            },
        )
        .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GroupObservation {
    process: ProcessIdentity,
    metrics: RaftGroupMetrics,
    quorum: Option<QuorumPrefix>,
}

/// Private cluster-plane evidence. The request must name this exact boot.
/// Callers select its endpoint and identity from committed meta state.
pub(crate) async fn evidence(
    State(state): State<HttpState>,
    Path(group): Path<u32>,
    headers: HeaderMap,
) -> Response {
    if let Some(response) = crate::reject_admin_incarnation(&state, &headers) {
        return response;
    }
    match local_observation(&state, RaftGroupId(group)).await {
        Ok(observation) => Json(observation).into_response(),
        Err(error) => error.into_response(),
    }
}

async fn local_observation(
    state: &HttpState,
    group: RaftGroupId,
) -> Result<GroupObservation, ControlHttpError> {
    let node_id = state
        .configured_node_id
        .ok_or(ControlHttpError::Unavailable)?;
    let meta = state
        .meta_control
        .as_ref()
        .ok_or(ControlHttpError::Unavailable)?;
    let snapshot = meta.read_state(Clone::clone).await?;
    let Some(ProcessState::Active(process)) = snapshot.operations.processes.get(&node_id) else {
        return Err(ControlHttpError::MissingPeer { node_id });
    };
    if process.incarnation != state.process_incarnation {
        return Err(ControlHttpError::MissingPeer { node_id });
    }
    let registry = state.raft_registry().ok_or(ControlHttpError::Unavailable)?;
    let metrics = registry
        .metrics_snapshot()
        .into_iter()
        .find(|metrics| metrics.raft_group_id == group.0)
        .ok_or(ControlHttpError::MissingPeer { node_id })?;
    let quorum = if metrics.current_leader == Some(node_id) {
        Some(
            crate::http_time::timeout(
                Duration::from_secs(10),
                registry.confirm_quorum_prefix(group),
            )
            .await
            .map_err(|_timeout| ControlHttpError::MissingPeer { node_id })??,
        )
    } else {
        None
    };
    Ok(GroupObservation {
        process: process.clone(),
        metrics: crate::render::raft_group_metrics(&metrics),
        quorum,
    })
}

async fn peer_observation(
    client: &reqwest::Client,
    snapshot: &ControlPlaneState,
    node_id: u64,
    group: RaftGroupId,
) -> Result<GroupObservation, ControlHttpError> {
    let node = snapshot
        .nodes
        .get(&node_id)
        .ok_or(ControlHttpError::MissingPeer { node_id })?;
    let Some(ProcessState::Active(identity)) = snapshot.operations.processes.get(&node_id) else {
        return Err(ControlHttpError::MissingPeer { node_id });
    };
    let url = format!(
        "{}/__ursula/control/group/{}/evidence",
        node.cluster_url.trim_end_matches('/'),
        group.0
    );
    let observation: GroupObservation = client
        .get(url)
        .header(PROCESS_INCARNATION_HEADER, identity.incarnation.as_str())
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|source| ControlHttpError::Peer { node_id, source })?
        .json()
        .await
        .map_err(|source| ControlHttpError::Peer { node_id, source })?;
    if &observation.process != identity
        || observation.metrics.node_id != node_id
        || observation.metrics.raft_group_id != u64::from(group.0)
    {
        return Err(ControlHttpError::MissingPeer { node_id });
    }
    Ok(observation)
}

async fn collect_evidence_with_retry(
    state: &HttpState,
    _snapshot: &ControlPlaneState,
    token: &OperationToken,
    exclude: Option<u64>,
    only_group: Option<RaftGroupId>,
) -> Result<Vec<PrefixEvidence>, ControlHttpError> {
    let started = tokio::time::Instant::now();
    loop {
        let snapshot = linear_state(state).await?;
        match collect_evidence(state, &snapshot, token, exclude, only_group).await {
            Ok(evidence) => return Ok(evidence),
            Err(error) => {
                if started.elapsed() >= Duration::from_secs(30)
                    || matches!(
                        error,
                        ControlHttpError::Operation(
                            OperationError::StaleExecutor
                                | OperationError::InventoryMismatch
                                | OperationError::InvalidTransition
                        )
                    )
                {
                    return Err(error);
                }
                // Remote membership convergence has no local watch channel.
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

async fn collect_evidence(
    state: &HttpState,
    snapshot: &ControlPlaneState,
    token: &OperationToken,
    exclude: Option<u64>,
    only_group: Option<RaftGroupId>,
) -> Result<Vec<PrefixEvidence>, ControlHttpError> {
    let operation = snapshot
        .operations
        .active
        .as_ref()
        .filter(|operation| &operation.token == token)
        .ok_or(OperationError::StaleExecutor)?;
    let installing_fence = operation.pending_action.as_ref().is_some_and(|action| {
        matches!(
            action.action,
            ursula_control::MembershipAction::InstallReplicaIdentity { .. }
        )
    });
    let retiring = exclude.is_some()
        || (installing_fence && matches!(operation.kind, OperationKind::RebuildReplica { .. }))
        || (operation.phase == OperationPhase::Preparing
            && matches!(operation.kind, OperationKind::RebuildReplica { .. }));
    let source = match operation.kind {
        OperationKind::MoveReplicas { source, .. } => source,
        OperationKind::RebuildReplica { node_id }
        | OperationKind::DecommissionNode { node_id, .. } => node_id,
    };
    let source = exclude.unwrap_or(source);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(12))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| ControlHttpError::Peer {
            node_id: source,
            source: error,
        })?;
    let mut proofs = Vec::new();
    for (group, desired) in operation
        .desired
        .iter()
        .filter(|(group, _)| only_group.is_none_or(|selected| selected == **group))
    {
        let voters = if retiring || installing_fence {
            operation
                .previous
                .get(group)
                .ok_or(OperationError::InventoryMismatch)?
        } else {
            desired
        };
        let required = voters
            .iter()
            .filter(|id| !retiring || **id != source)
            .copied()
            .collect::<BTreeSet<_>>();
        let mut observed_leader = None;
        for node_id in &required {
            if let Ok(observation) = peer_observation(&client, snapshot, *node_id, *group).await
                && let Some(leader) = observation.metrics.current_leader
            {
                observed_leader = Some(leader);
                break;
            }
        }
        let leader = observed_leader.ok_or(ControlHttpError::Unavailable)?;
        let observed_at_ms = state.wall_clock.unix_time_ms();
        let leader_observation = peer_observation(&client, snapshot, leader, *group).await?;
        let prefix = leader_observation
            .quorum
            .ok_or(ControlHttpError::MissingPeer { node_id: leader })?;
        if prefix.raft_group_id != group.0 || prefix.leader_id != leader {
            return Err(ControlHttpError::MissingPeer { node_id: leader });
        }
        let observed_voters = leader_observation
            .metrics
            .voter_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let voters = if exclude.is_some() {
            let previous = operation
                .previous
                .get(group)
                .ok_or(OperationError::InventoryMismatch)?;
            if &observed_voters != previous && &observed_voters != desired {
                return Err(OperationError::InventoryMismatch.into());
            }
            &observed_voters
        } else if retiring && matches!(operation.kind, OperationKind::RebuildReplica { .. }) {
            if &observed_voters != voters && observed_voters != required {
                return Err(OperationError::InventoryMismatch.into());
            }
            &observed_voters
        } else {
            voters
        };
        // A just-removed leader can remain in follower leader hints while the
        // new configuration elects its successor. Never submit that transient
        // hint as evidence for a voter set that no longer contains it.
        if !voters.contains(&leader) {
            return Err(ControlHttpError::MissingPeer { node_id: leader });
        }
        let required = voters
            .iter()
            .filter(|id| !retiring || **id != source)
            .copied()
            .collect::<BTreeSet<_>>();
        let mut replicas = BTreeMap::new();
        for node_id in &required {
            let observation = peer_observation(&client, snapshot, *node_id, *group).await?;
            let health = &observation.metrics.maintenance;
            if let Some(ursula_control::OperationAction {
                action:
                    ursula_control::MembershipAction::InstallReplicaIdentity { node_id, identity },
                ..
            }) = operation.pending_action.as_ref()
                && observation
                    .metrics
                    .installed_replica_identities
                    .get(node_id)
                    != Some(identity)
            {
                return Err(ControlHttpError::MissingPeer { node_id: *node_id });
            }
            if observation.metrics.current_leader != Some(leader)
                || observation.metrics.current_term != Some(prefix.leader_term)
                || observation
                    .metrics
                    .voter_ids
                    .iter()
                    .copied()
                    .collect::<BTreeSet<_>>()
                    != *voters
                || !health.running
                || !health.recovery_ready
                || health.membership_joint
                || health.stopped_for_operator
                || health.membership_log_index.is_none()
                || observation.metrics.last_applied_index < health.membership_log_index
                || observation.metrics.last_applied_index < Some(prefix.required_applied_index)
            {
                return Err(ControlHttpError::MissingPeer { node_id: *node_id });
            }
            replicas.insert(*node_id, ReplicaEvidence {
                process: observation.process,
                installed_replica_identities: observation
                    .metrics
                    .installed_replica_identities
                    .clone(),
                applied_index: observation
                    .metrics
                    .last_applied_index
                    .ok_or(ControlHttpError::MissingPeer { node_id: *node_id })?,
            });
        }
        proofs.push(PrefixEvidence {
            raft_group_id: *group,
            leader,
            term: prefix.leader_term,
            committed_index: prefix.required_applied_index,
            voters: voters.clone(),
            joint: false,
            replicas,
            observed_at_ms,
        });
    }
    Ok(proofs)
}

pub(crate) const ACTION_PATH: &str = "/__ursula/control/action";
pub(crate) const ACTION_DRAIN_PATH: &str = "/__ursula/control/action/drain";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LocalActionState {
    Idle,
    Applied {
        operation_id: u64,
        sequence: u64,
        fence_index: Option<u64>,
    },
    Fenced {
        operation_id: u64,
        sequence: u64,
    },
}

pub(crate) async fn drain_action(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(request): Json<ursula_control::ActionRequest>,
) -> Response {
    if let Some(response) = crate::reject_admin_incarnation(&state, &headers) {
        return response;
    }
    match fence_action(&state, request).await {
        Ok(receipt) => Json(receipt).into_response(),
        Err(error) => error.into_response(),
    }
}

async fn fence_action(
    state: &HttpState,
    request: ursula_control::ActionRequest,
) -> Result<ursula_control::OperationAction, ControlHttpError> {
    let mut gate = state.control_action_gate.lock().await;
    let snapshot = linear_state(state).await?;
    let operation = snapshot
        .operations
        .active
        .as_ref()
        .filter(|operation| operation.token == request.token)
        .ok_or(OperationError::StaleExecutor)?;
    if operation.pending_action.as_ref() != Some(&request.action)
        || state.configured_node_id != Some(request.action.leader)
        || state.process_incarnation != request.action.process.incarnation
    {
        return Err(OperationError::StaleExecutor.into());
    }
    *gate = LocalActionState::Fenced {
        operation_id: request.token.operation_id,
        sequence: request.action.sequence,
    };
    Ok(request.action)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActionResult {
    fence_index: Option<u64>,
}

pub(crate) async fn action(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(request): Json<ursula_control::ActionRequest>,
) -> Response {
    if let Some(response) = crate::reject_admin_incarnation(&state, &headers) {
        return response;
    }
    // An admitted effect survives a disconnected executor. The mutex orders
    // delayed duplicates before authorization is checked again.
    match tokio::spawn(async move { execute_action(&state, request).await }).await {
        Ok(Ok(result)) => Json(result).into_response(),
        Ok(Err(error)) => error.into_response(),
        Err(error) => {
            tracing::error!(%error, "control action task failed");
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}

/// Reactivate only a certified, currently nonvoting target. A removed replica
/// may retain a WAL from before another member's replacement, so normal replay
/// cannot even authenticate the leader carrying its missing fence entries.
async fn prepare_replica(
    state: &HttpState,
    snapshot: &ControlPlaneState,
    request: &ursula_control::ActionRequest,
) -> Result<(), ControlHttpError> {
    let group = request.action.group;
    let target = request.action.leader;
    let operation = snapshot
        .operations
        .active
        .as_ref()
        .ok_or(OperationError::StaleExecutor)?;
    let desired = operation
        .desired
        .get(&group)
        .ok_or(OperationError::InventoryMismatch)?;
    if !desired.contains(&target) {
        return Err(OperationError::InvalidTransition.into());
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|source| ControlHttpError::Peer {
            node_id: target,
            source,
        })?;
    let candidates = operation
        .previous
        .get(&group)
        .into_iter()
        .flatten()
        .chain(desired)
        .copied()
        .collect::<BTreeSet<_>>();
    let mut leader = None;
    for node_id in candidates {
        if let Ok(observed) = peer_observation(&client, snapshot, node_id, group).await
            && let Some(proof) = &observed.quorum
            && proof.leader_id == node_id
            && observed.metrics.current_leader == Some(node_id)
            && observed.metrics.current_term == Some(proof.leader_term)
            && !observed.metrics.maintenance.membership_joint
        {
            leader = Some(observed);
            break;
        }
    }
    let leader = leader.ok_or(ControlHttpError::NoDataLeader { group })?;
    // A retry after successful promotion must not stop an active voter.
    if leader.metrics.voter_ids.contains(&target) {
        let registry = state.raft_registry().ok_or(ControlHttpError::Unavailable)?;
        registry.set_replica_reactivation(
            group,
            request.token.operation_id,
            BTreeSet::new(),
            BTreeMap::new(),
            0,
        );
        state.runtime.warm_group(group).await?;
        return Ok(());
    }
    let (identities, required_index) =
        replica_reactivation_certificate(snapshot, group, target, &leader)?;
    let refreshed = linear_state(state).await?;
    if refreshed
        .operations
        .active
        .as_ref()
        .filter(|active| active.token == request.token)
        .and_then(|active| active.pending_action.as_ref())
        != Some(&request.action)
    {
        return Err(OperationError::StaleExecutor.into());
    }
    let registry = state.raft_registry().ok_or(ControlHttpError::Unavailable)?;
    // Set the pre-open certificate first so any concurrent warm also receives
    // the election gate. Shutdown drains owned snapshot and metadata work.
    registry.set_replica_reactivation(
        group,
        request.token.operation_id,
        leader.metrics.voter_ids.iter().copied().collect(),
        identities,
        required_index,
    );
    state.runtime.shutdown_group(group).await?;
    registry.allow_dynamic_group_hosting(group);
    state.runtime.warm_group(group).await?;
    Ok(())
}

fn replica_reactivation_certificate(
    snapshot: &ControlPlaneState,
    group: RaftGroupId,
    target: u64,
    leader: &GroupObservation,
) -> Result<(BTreeMap<u64, ursula_proto::admin::ReplicaIdentity>, u64), ControlHttpError> {
    if leader.metrics.voter_ids.contains(&target) || leader.metrics.maintenance.membership_joint {
        return Err(OperationError::InvalidTransition.into());
    }
    let mut identities = BTreeMap::new();
    let mut required_index = 0;
    for (node_id, replica) in &snapshot.operations.replicas {
        if let ursula_control::ReplicaState::Active {
            identity,
            installed_groups,
        } = replica
            && let Some(index) = installed_groups
                .get(&group)
                .copied()
                .filter(|index| *index > 0)
        {
            required_index = required_index.max(index);
            identities.insert(*node_id, identity.clone());
        }
    }
    // Require an explicit target admission. Also import the current survivor
    // prefix below: on repeated moves its admission may predate a later removal.
    if !identities.contains_key(&target)
        || leader
            .quorum
            .as_ref()
            .is_none_or(|proof| proof.required_applied_index < required_index)
        || identities.iter().any(|(node, identity)| {
            leader.metrics.installed_replica_identities.get(node) != Some(identity)
        })
    {
        return Err(OperationError::MissingEvidence {
            raft_group_id: group,
        }
        .into());
    }
    let proof = leader
        .quorum
        .as_ref()
        .ok_or(ControlHttpError::NoDataLeader { group })?;
    if leader
        .metrics
        .maintenance
        .membership_log_index
        .is_none_or(|index| index > proof.required_applied_index)
    {
        return Err(OperationError::MissingEvidence {
            raft_group_id: group,
        }
        .into());
    }
    required_index = required_index.max(proof.required_applied_index);
    Ok((identities, required_index))
}

async fn execute_action(
    state: &HttpState,
    request: ursula_control::ActionRequest,
) -> Result<ActionResult, ControlHttpError> {
    let mut completed = state.control_action_gate.lock().await;
    let snapshot = linear_state(state).await?;
    let operation = snapshot
        .operations
        .active
        .as_ref()
        .filter(|operation| operation.token == request.token)
        .ok_or(OperationError::StaleExecutor)?;
    if operation.pending_action.as_ref() != Some(&request.action)
        || state.configured_node_id != Some(request.action.leader)
        || request.action.process.incarnation != state.process_incarnation
        || !snapshot
            .operations
            .accepts_process(request.action.leader, &request.action.process)
    {
        return Err(OperationError::StaleExecutor.into());
    }
    let applied = LocalActionState::Applied {
        operation_id: request.token.operation_id,
        sequence: request.action.sequence,
        fence_index: None,
    };
    let fenced = LocalActionState::Fenced {
        operation_id: request.token.operation_id,
        sequence: request.action.sequence,
    };
    if *completed == fenced {
        return Err(ControlHttpError::ActionDrained);
    }
    if let LocalActionState::Applied {
        operation_id,
        sequence,
        fence_index,
    } = &*completed
        && *operation_id == request.token.operation_id
        && *sequence == request.action.sequence
    {
        return Ok(ActionResult {
            fence_index: *fence_index,
        });
    }
    if matches!(
        request.action.action,
        ursula_control::MembershipAction::PrepareReplica
    ) {
        prepare_replica(state, &snapshot, &request).await?;
        *completed = applied;
        return Ok(ActionResult { fence_index: None });
    }
    let raft = state
        .raft_registry()
        .and_then(|registry| registry.get(request.action.group))
        .ok_or(ControlHttpError::Unavailable)?;
    match request.action.action {
        ursula_control::MembershipAction::PrepareReplica => {
            return Err(OperationError::InvalidTransition.into());
        }
        ursula_control::MembershipAction::AddLearner { node_id } => {
            let node = snapshot
                .nodes
                .get(&node_id)
                .ok_or(ControlHttpError::MissingPeer { node_id })?;
            raft.add_learner(
                node_id,
                openraft::BasicNode::new(node.cluster_url.clone()),
                false,
            )
            .await
            .map_err(|source| action_raft_error("operation add learner", source))?;
        }
        ursula_control::MembershipAction::InstallReplicaIdentity {
            node_id,
            ref identity,
        } => {
            let expected = match snapshot.operations.replicas.get(&node_id) {
                Some(ursula_control::ReplicaState::Pending {
                    previous,
                    replacement,
                    ..
                }) if replacement == identity => Some(previous.clone()),
                Some(ursula_control::ReplicaState::Active {
                    identity: active, ..
                }) if active == identity => None,
                _ => return Err(OperationError::InvalidTransition.into()),
            };
            let registry = state.raft_registry().ok_or(ControlHttpError::Unavailable)?;
            let fence_index = registry
                .install_replica_identity(request.action.group, node_id, expected, identity.clone())
                .await
                .map_err(|source| {
                    let forwarded = source.leader_hint().is_some();
                    let error =
                        ursula_raft::MetaRaftError::with_source("install replica fence", source);
                    if forwarded {
                        ControlHttpError::LeadershipChanged(error)
                    } else {
                        ControlHttpError::Meta(error)
                    }
                })?;
            *completed = LocalActionState::Applied {
                operation_id: request.token.operation_id,
                sequence: request.action.sequence,
                fence_index: Some(fence_index),
            };
            return Ok(ActionResult {
                fence_index: Some(fence_index),
            });
        }
        ursula_control::MembershipAction::RetireReplica => {
            let OperationKind::RebuildReplica { node_id } = operation.kind else {
                return Err(OperationError::InvalidTransition.into());
            };
            let mut voters = operation
                .previous
                .get(&request.action.group)
                .ok_or(OperationError::InventoryMismatch)?
                .clone();
            voters.remove(&node_id);
            let successor = voters.iter().next().copied();
            raft.change_membership(voters, true)
                .await
                .map_err(|source| action_raft_error("retire data replica", source))?;
            // OpenRaft can retain a demoted leader as a learner while it still
            // sends heartbeats. Explicitly hand off after the committed change
            // so survivor evidence can certify a voter leader. The action gate
            // remains held through this handoff; a retry retains its receipt.
            if request.action.leader == node_id
                && let Some(successor) = successor
            {
                match state
                    .raft_registry()
                    .ok_or(ControlHttpError::Unavailable)?
                    .transfer_leader(request.action.group, successor)
                    .await
                {
                    Ok(()) | Err(ursula_raft::LeadershipTransferError::NotLeader { .. }) => {}
                    Err(source) => {
                        return Err(ursula_raft::MetaRaftError::with_source(
                            "handoff retired data leader",
                            source,
                        )
                        .into());
                    }
                }
            }
        }
        ursula_control::MembershipAction::ChangeVoters => {
            let voters = operation
                .desired
                .get(&request.action.group)
                .ok_or(OperationError::InventoryMismatch)?
                .clone();
            raft.change_membership(voters, false)
                .await
                .map_err(|source| action_raft_error("operation change voters", source))?;
        }
    }
    *completed = applied;
    Ok(ActionResult { fence_index: None })
}

async fn retire_data_replica(
    state: &HttpState,
    mut snapshot: ControlPlaneState,
    token: &OperationToken,
) -> Result<(), ControlHttpError> {
    let meta = state
        .meta_control
        .as_ref()
        .ok_or(ControlHttpError::Unavailable)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|source| ControlHttpError::Peer { node_id: 0, source })?;
    let operation = snapshot
        .operations
        .active
        .as_ref()
        .ok_or(OperationError::StaleExecutor)?
        .clone();
    if let Some(pending) = &operation.pending_action {
        run_action(state, &client, &snapshot, token, pending.clone()).await?;
        snapshot = linear_state(state).await?;
    }
    for group in operation.previous.keys() {
        let proofs =
            collect_evidence_with_retry(state, &snapshot, token, None, Some(*group)).await?;
        let proof = proofs
            .iter()
            .find(|proof| proof.raft_group_id == *group)
            .ok_or(OperationError::MissingEvidence {
                raft_group_id: *group,
            })?;
        let OperationKind::RebuildReplica { node_id: source } = operation.kind else {
            return Err(OperationError::InvalidTransition.into());
        };
        if !proof.voters.contains(&source) {
            continue;
        }
        let response = meta
            .write(ControlCommand::Operation {
                command: OperationCommand::PrepareAction {
                    token: token.clone(),
                    group: *group,
                    leader: proof.leader,
                    action: ursula_control::MembershipAction::RetireReplica,
                },
                now_ms: state.wall_clock.unix_time_ms(),
            })
            .await?;
        let ControlResponse::Operation(result) = response else {
            return Err(ControlHttpError::Unavailable);
        };
        let ursula_control::OperationOutcome::ActionPrepared(receipt) = result? else {
            return Err(ControlHttpError::Unavailable);
        };
        run_action(state, &client, &snapshot, token, receipt).await?;
        snapshot = linear_state(state).await?;
    }
    for evidence in collect_evidence_with_retry(state, &snapshot, token, None, None).await? {
        let command_ms = state.wall_clock.unix_time_ms();
        let response = meta
            .write(ControlCommand::Operation {
                command: OperationCommand::Observe {
                    token: token.clone(),
                    evidence: evidence.clone(),
                },
                now_ms: command_ms,
            })
            .await?;
        match response {
            ControlResponse::Operation(Ok(_)) => {}
            ControlResponse::Operation(Err(source)) => {
                return Err(ControlHttpError::EvidenceRejected {
                    group: evidence.raft_group_id,
                    leader: evidence.leader,
                    voters: evidence.voters,
                    index: evidence.committed_index,
                    observed_ms: evidence.observed_at_ms,
                    command_ms,
                    floor: operation.prefix_floor.get(&evidence.raft_group_id).copied(),
                    source,
                });
            }
            _ => return Err(ControlHttpError::Unavailable),
        }
    }
    Ok(())
}

async fn reconcile(
    state: &HttpState,
    mut snapshot: ControlPlaneState,
    token: OperationToken,
) -> Result<ControlResponse, ControlHttpError> {
    let meta = state
        .meta_control
        .as_ref()
        .ok_or(ControlHttpError::Unavailable)?;
    let operation = snapshot
        .operations
        .active
        .as_ref()
        .filter(|operation| operation.token == token)
        .ok_or(OperationError::StaleExecutor)?
        .clone();
    if operation.phase == OperationPhase::Preparing
        && matches!(operation.kind, OperationKind::RebuildReplica { .. })
    {
        return Err(OperationError::InvalidTransition.into());
    }
    if matches!(operation.kind, OperationKind::RebuildReplica { .. }) {
        meta.reconcile_operation_membership(token.clone(), true)
            .await?;
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|source| ControlHttpError::Peer { node_id: 0, source })?;
    if let Some(pending) = &operation.pending_action {
        let receipt = if snapshot
            .operations
            .accepts_process(pending.leader, &pending.process)
        {
            pending.clone()
        } else {
            let leader = if matches!(
                pending.action,
                ursula_control::MembershipAction::PrepareReplica
            ) {
                pending.leader
            } else {
                let mut leader = None;
                for node_id in operation.participants.keys() {
                    if let Ok(observation) =
                        peer_observation(&client, &snapshot, *node_id, pending.group).await
                        && let Some(current) = observation.metrics.current_leader
                    {
                        leader = Some(current);
                        break;
                    }
                }
                leader.ok_or(ControlHttpError::Unavailable)?
            };
            let response = meta
                .write(ControlCommand::Operation {
                    command: OperationCommand::ReassignAction {
                        drained: None,
                        token: token.clone(),
                        leader,
                    },
                    now_ms: state.wall_clock.unix_time_ms(),
                })
                .await?;
            let ControlResponse::Operation(Ok(ursula_control::OperationOutcome::ActionPrepared(
                receipt,
            ))) = response
            else {
                return Ok(response);
            };
            receipt
        };
        run_action(state, &client, &snapshot, &token, receipt).await?;
        snapshot = linear_state(state).await?;
    }
    // A replacement process waits in meta-only bootstrap. Order and durably
    // certify every survivor fence before activating its new data identity;
    // only then can the existing learner preparation/catch-up path proceed.
    install_replacement_fences(state, &client, &mut snapshot, &token).await?;
    install_new_target_fences(state, &client, &mut snapshot, &token).await?;
    for (group, desired) in &operation.desired {
        snapshot = linear_state(state).await?;
        let previous = operation
            .previous
            .get(group)
            .ok_or(OperationError::InventoryMismatch)?;
        let candidates = previous
            .iter()
            .chain(desired)
            .copied()
            .collect::<BTreeSet<_>>();
        let mut leader_observation = None;
        for node_id in candidates {
            if let Ok(observation) = peer_observation(&client, &snapshot, node_id, *group).await
                && let Some(leader) = observation.metrics.current_leader
            {
                leader_observation =
                    Some(peer_observation(&client, &snapshot, leader, *group).await?);
                break;
            }
        }
        let observation =
            leader_observation.ok_or(ControlHttpError::NoDataLeader { group: *group })?;
        let leader = observation.metrics.node_id;
        let mut targets = desired
            .difference(previous)
            .copied()
            .collect::<BTreeSet<_>>();
        if let OperationKind::RebuildReplica { node_id } = operation.kind {
            targets.insert(node_id);
        }
        for target in &targets {
            wait_target_process(state, &client, &token, *target).await?;
            let response = meta
                .write(ControlCommand::Operation {
                    command: OperationCommand::PrepareAction {
                        token: token.clone(),
                        group: *group,
                        leader: *target,
                        action: ursula_control::MembershipAction::PrepareReplica,
                    },
                    now_ms: state.wall_clock.unix_time_ms(),
                })
                .await?;
            let ControlResponse::Operation(Ok(ursula_control::OperationOutcome::ActionPrepared(
                receipt,
            ))) = response
            else {
                return Ok(response);
            };
            snapshot = linear_state(state).await?;
            run_action(state, &client, &snapshot, &token, receipt).await?;

            // A retained learner may have just lost its entire local WAL.
            // Registration completes independently from bounded catch-up below.
            if observation.metrics.voter_ids.contains(target) {
                continue;
            }
            let response = meta
                .write(ControlCommand::Operation {
                    command: OperationCommand::PrepareAction {
                        token: token.clone(),
                        group: *group,
                        leader,
                        action: ursula_control::MembershipAction::AddLearner { node_id: *target },
                    },
                    now_ms: state.wall_clock.unix_time_ms(),
                })
                .await?;
            let ControlResponse::Operation(Ok(ursula_control::OperationOutcome::ActionPrepared(
                receipt,
            ))) = response
            else {
                return Ok(response);
            };
            snapshot = linear_state(state).await?;
            run_action(state, &client, &snapshot, &token, receipt).await?;
        }
        if !targets.is_empty() {
            wait_replica_prefix(Some(state), &client, &snapshot, *group, desired).await?;
        }
        if observation
            .metrics
            .voter_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            != *desired
            || observation.metrics.maintenance.membership_joint
        {
            let response = meta
                .write(ControlCommand::Operation {
                    command: OperationCommand::PrepareAction {
                        token: token.clone(),
                        group: *group,
                        leader,
                        action: ursula_control::MembershipAction::ChangeVoters,
                    },
                    now_ms: state.wall_clock.unix_time_ms(),
                })
                .await?;
            let ControlResponse::Operation(Ok(ursula_control::OperationOutcome::ActionPrepared(
                receipt,
            ))) = response
            else {
                return Ok(response);
            };
            snapshot = linear_state(state).await?;
            run_action(state, &client, &snapshot, &token, receipt).await?;
        }
    }
    Ok(ControlResponse::Operation(Ok(
        ursula_control::OperationOutcome::ActionFinished,
    )))
}

async fn wait_target_process(
    state: &HttpState,
    client: &reqwest::Client,
    token: &OperationToken,
    node_id: u64,
) -> Result<(), ControlHttpError> {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let snapshot = linear_state(state).await?;
            if !snapshot
                .operations
                .active
                .as_ref()
                .is_some_and(|op| &op.token == token)
            {
                return Err(OperationError::StaleExecutor.into());
            }
            let node = snapshot
                .nodes
                .get(&node_id)
                .ok_or(ControlHttpError::MissingPeer { node_id })?;
            let Some(ProcessState::Active(process)) = snapshot.operations.processes.get(&node_id)
            else {
                return Err(OperationError::ProcessChanged { node_id }.into());
            };
            if let Ok(response) = client
                .get(format!(
                    "{}/__ursula/metrics",
                    node.cluster_url.trim_end_matches('/')
                ))
                .send()
                .await
                && response.status().is_success()
                && let Ok(metrics) = response.json::<ursula_proto::admin::NodeMetrics>().await
                && metrics.process_node_id == Some(node_id)
                && metrics.process_incarnation.as_ref() == Some(&process.incarnation)
            {
                return Ok(());
            }
            // A draining old listener can outlive the committed new boot.
            // Keep observing; never accept its metrics as the new process.
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_elapsed| ControlHttpError::MissingPeer { node_id })?
}

async fn install_new_target_fences(
    state: &HttpState,
    client: &reqwest::Client,
    snapshot: &mut ControlPlaneState,
    token: &OperationToken,
) -> Result<(), ControlHttpError> {
    let operation = snapshot
        .operations
        .active
        .as_ref()
        .filter(|operation| &operation.token == token)
        .ok_or(OperationError::StaleExecutor)?
        .clone();
    if matches!(operation.kind, OperationKind::RebuildReplica { .. }) {
        return Ok(());
    }
    let meta = state
        .meta_control
        .as_ref()
        .ok_or(ControlHttpError::Unavailable)?;
    for (group, desired) in &operation.desired {
        let previous = operation
            .previous
            .get(group)
            .ok_or(OperationError::InventoryMismatch)?;
        for target in desired.difference(previous) {
            *snapshot = linear_state(state).await?;
            let Some(ursula_control::ReplicaState::Active {
                identity,
                installed_groups,
            }) = snapshot.operations.replicas.get(target)
            else {
                return Err(OperationError::ReplicaChanged { node_id: *target }.into());
            };
            if installed_groups.contains_key(group) {
                continue;
            }
            let identity = identity.clone();
            let mut leader = None;
            for voter in previous {
                match peer_observation(client, snapshot, *voter, *group).await {
                    Ok(observation)
                        if observation
                            .quorum
                            .as_ref()
                            .is_some_and(|proof| proof.leader_id == *voter) =>
                    {
                        leader = Some(*voter);
                        break;
                    }
                    Ok(observation) => {
                        tracing::warn!(voter, ?group, leader = ?observation.metrics.current_leader, "replica admission candidate has no leader proof")
                    }
                    Err(error) => {
                        tracing::warn!(voter, ?group, %error, "replica admission leader evidence unavailable")
                    }
                }
            }
            let response = meta
                .write(ControlCommand::Operation {
                    command: OperationCommand::PrepareAction {
                        token: token.clone(),
                        group: *group,
                        leader: leader.ok_or(ControlHttpError::NoDataLeader { group: *group })?,
                        action: ursula_control::MembershipAction::InstallReplicaIdentity {
                            node_id: *target,
                            identity,
                        },
                    },
                    now_ms: state.wall_clock.unix_time_ms(),
                })
                .await?;
            let receipt = match response {
                ControlResponse::Operation(Ok(
                    ursula_control::OperationOutcome::ActionPrepared(receipt),
                )) => receipt,
                ControlResponse::Operation(Err(error)) => return Err(error.into()),
                _ => return Err(ControlHttpError::Unavailable),
            };
            *snapshot = linear_state(state).await?;
            run_action(state, client, snapshot, token, receipt).await?;
            *snapshot = linear_state(state).await?;
        }
    }
    Ok(())
}

async fn install_replacement_fences(
    state: &HttpState,
    client: &reqwest::Client,
    snapshot: &mut ControlPlaneState,
    token: &OperationToken,
) -> Result<(), ControlHttpError> {
    let operation = snapshot
        .operations
        .active
        .as_ref()
        .filter(|operation| &operation.token == token)
        .ok_or(OperationError::StaleExecutor)?
        .clone();
    let OperationKind::RebuildReplica { node_id } = operation.kind else {
        return Ok(());
    };
    let meta = state
        .meta_control
        .as_ref()
        .ok_or(ControlHttpError::Unavailable)?;
    let identity = match snapshot.operations.replicas.get(&node_id) {
        Some(ursula_control::ReplicaState::Active { .. }) => return Ok(()),
        Some(ursula_control::ReplicaState::Pending { replacement, .. }) => replacement.clone(),
        _ => return Err(OperationError::InvalidTransition.into()),
    };
    for (group, previous) in &operation.previous {
        if matches!(snapshot.operations.replicas.get(&node_id), Some(ursula_control::ReplicaState::Pending { installed_groups, .. }) if installed_groups.contains_key(group))
        {
            continue;
        }
        let mut leader = None;
        for voter in previous.iter().filter(|voter| **voter != node_id) {
            if let Ok(observation) = peer_observation(client, snapshot, *voter, *group).await
                && observation
                    .quorum
                    .as_ref()
                    .is_some_and(|proof| proof.leader_id == *voter)
                && observation.metrics.voter_ids.contains(voter)
            {
                leader = Some(*voter);
                break;
            }
        }
        let leader = leader.ok_or(ControlHttpError::NoDataLeader { group: *group })?;
        let response = meta
            .write(ControlCommand::Operation {
                command: OperationCommand::PrepareAction {
                    token: token.clone(),
                    group: *group,
                    leader,
                    action: ursula_control::MembershipAction::InstallReplicaIdentity {
                        node_id,
                        identity: identity.clone(),
                    },
                },
                now_ms: state.wall_clock.unix_time_ms(),
            })
            .await?;
        let receipt = match response {
            ControlResponse::Operation(Ok(ursula_control::OperationOutcome::ActionPrepared(
                receipt,
            ))) => receipt,
            ControlResponse::Operation(Err(error)) => return Err(error.into()),
            _ => return Err(ControlHttpError::Unavailable),
        };
        *snapshot = linear_state(state).await?;
        run_action(state, client, snapshot, token, receipt).await?;
        *snapshot = linear_state(state).await?;
    }
    match meta
        .write(ControlCommand::Operation {
            command: OperationCommand::ActivateReplica {
                token: token.clone(),
                node_id,
                identity,
            },
            now_ms: state.wall_clock.unix_time_ms(),
        })
        .await?
    {
        ControlResponse::Operation(Ok(_)) => {}
        ControlResponse::Operation(Err(error)) => return Err(error.into()),
        _ => return Err(ControlHttpError::Unavailable),
    }
    *snapshot = linear_state(state).await?;
    Ok(())
}

async fn wait_replica_prefix(
    state: Option<&HttpState>,
    client: &reqwest::Client,
    snapshot: &ControlPlaneState,
    group: RaftGroupId,
    targets: &BTreeSet<u64>,
) -> Result<(), ControlHttpError> {
    tokio::time::timeout(
        Duration::from_secs(30),
        wait_replica_prefix_inner(state, client, snapshot, group, targets),
    )
    .await
    .map_err(|_elapsed| OperationError::MissingEvidence {
        raft_group_id: group,
    })?
}

async fn wait_replica_prefix_inner(
    state: Option<&HttpState>,
    client: &reqwest::Client,
    snapshot: &ControlPlaneState,
    group: RaftGroupId,
    targets: &BTreeSet<u64>,
) -> Result<(), ControlHttpError> {
    let deadline = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(30))
        .ok_or(ControlHttpError::Unavailable)?;
    let token = snapshot
        .operations
        .active
        .as_ref()
        .map(|op| op.token.clone());
    let mut prefix = None;
    loop {
        let refreshed;
        let snapshot = if let Some(state) = state {
            refreshed = linear_state(state).await?;
            if refreshed.operations.active.as_ref().map(|op| &op.token) != token.as_ref() {
                return Err(OperationError::StaleExecutor.into());
            }
            &refreshed
        } else {
            snapshot
        };
        if prefix.is_none() {
            for target in targets {
                if let Ok(observed) = peer_observation(client, snapshot, *target, group).await {
                    if let Some(proof) = observed.quorum {
                        prefix = Some(proof);
                        break;
                    }
                    // The leader may be the source that this move will remove.
                    // Confirm it through committed inventory, even though it is
                    // absent from the eventual target voter set.
                    if let Some(leader) = observed.metrics.current_leader
                        && let Ok(leader_observation) =
                            peer_observation(client, snapshot, leader, group).await
                        && let Some(proof) = leader_observation.quorum
                    {
                        prefix = Some(proof);
                        break;
                    }
                }
            }
        }
        if let Some(proof) = &prefix {
            let mut ready = true;
            let mut leader_changed = false;
            for target in targets {
                match peer_observation(client, snapshot, *target, group).await {
                    Ok(observed) => {
                        let metrics = &observed.metrics;
                        leader_changed |= metrics.current_leader != Some(proof.leader_id)
                            || metrics.current_term != Some(proof.leader_term);
                        ready &= metrics.last_applied_index >= Some(proof.required_applied_index)
                            && metrics.maintenance.running
                            && metrics.maintenance.recovery_ready;
                    }
                    Err(_unavailable) => ready = false,
                }
            }
            if ready && !leader_changed {
                return Ok(());
            }
            if leader_changed {
                prefix = None;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(OperationError::MissingEvidence {
                raft_group_id: group,
            }
            .into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn run_action(
    state: &HttpState,
    client: &reqwest::Client,
    initial: &ControlPlaneState,
    token: &OperationToken,
    mut action: ursula_control::OperationAction,
) -> Result<(), ControlHttpError> {
    let deadline = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(30))
        .ok_or(ControlHttpError::Unavailable)?;
    let mut snapshot = initial.clone();
    loop {
        match run_action_once(state, client, &snapshot, token, action.clone()).await {
            Ok(()) => return Ok(()),
            Err(ControlHttpError::RemoteLeadershipChanged { detail }) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(ControlHttpError::RemoteLeadershipChanged { detail });
                }
            }
            Err(error) => return Err(error),
        }
        // ForwardToLeader can follow a partially committed membership change.
        // Fence this exact receipt at the old executor before any reassignment.
        let node_id = action.leader;
        let node = snapshot
            .nodes
            .get(&node_id)
            .ok_or(ControlHttpError::MissingPeer { node_id })?;
        let drained = client
            .post(format!(
                "{}{}",
                node.cluster_url.trim_end_matches('/'),
                ACTION_DRAIN_PATH
            ))
            .header(
                PROCESS_INCARNATION_HEADER,
                action.process.incarnation.as_str(),
            )
            .json(&ursula_control::ActionRequest {
                token: token.clone(),
                action: action.clone(),
            })
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|source| ControlHttpError::Peer { node_id, source })?
            .json::<ursula_control::OperationAction>()
            .await
            .map_err(|source| ControlHttpError::Peer { node_id, source })?;
        if drained != action {
            return Err(OperationError::InvalidTransition.into());
        }
        let leader = loop {
            snapshot = linear_state(state).await?;
            let operation = snapshot
                .operations
                .active
                .as_ref()
                .filter(|op| &op.token == token)
                .ok_or(OperationError::StaleExecutor)?;
            let mut confirmed = None;
            for candidate in operation.participants.keys() {
                if let Ok(observation) =
                    peer_observation(client, &snapshot, *candidate, action.group).await
                    && let Some(prefix) = observation.quorum
                    && prefix.leader_id == *candidate
                    && prefix.raft_group_id == action.group.0
                {
                    confirmed = Some(*candidate);
                    break;
                }
            }
            if let Some(leader) = confirmed {
                break leader;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ControlHttpError::Unavailable);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        let response = state
            .meta_control
            .as_ref()
            .ok_or(ControlHttpError::Unavailable)?
            .write(ControlCommand::Operation {
                command: OperationCommand::ReassignAction {
                    token: token.clone(),
                    leader,
                    drained: Some(drained),
                },
                now_ms: state.wall_clock.unix_time_ms(),
            })
            .await?;
        match response {
            ControlResponse::Operation(Ok(ursula_control::OperationOutcome::ActionPrepared(
                receipt,
            ))) => action = receipt,
            ControlResponse::Operation(Err(error)) => return Err(error.into()),
            _ => return Err(ControlHttpError::Unavailable),
        }
    }
}

async fn run_action_once(
    state: &HttpState,
    client: &reqwest::Client,
    snapshot: &ControlPlaneState,
    token: &OperationToken,
    action: ursula_control::OperationAction,
) -> Result<(), ControlHttpError> {
    let node_id = action.leader;
    let node = snapshot
        .nodes
        .get(&node_id)
        .ok_or(ControlHttpError::MissingPeer { node_id })?;
    let request = ursula_control::ActionRequest {
        token: token.clone(),
        action: action.clone(),
    };
    let peer_response = client
        .post(format!(
            "{}{}",
            node.cluster_url.trim_end_matches('/'),
            ACTION_PATH
        ))
        .header(
            PROCESS_INCARNATION_HEADER,
            action.process.incarnation.as_str(),
        )
        .json(&request)
        .send()
        .await
        .map_err(|source| ControlHttpError::Peer { node_id, source })?;
    let status = peer_response.status();
    if !status.is_success() {
        let body = peer_response
            .text()
            .await
            .map_err(|source| ControlHttpError::Peer { node_id, source })?;
        if let Ok(rejection) = serde_json::from_str::<ActionRejection>(&body) {
            let detail = match rejection {
                ActionRejection::LeadershipChanged { detail } => detail,
                ActionRejection::Drained => "executor already drained this receipt".to_owned(),
            };
            return Err(ControlHttpError::RemoteLeadershipChanged { detail });
        }
        return Err(ControlHttpError::ActionRejected {
            node_id,
            action: action.action.clone(),
            status,
            body,
        });
    }
    let result: ActionResult = peer_response
        .json()
        .await
        .map_err(|source| ControlHttpError::Peer { node_id, source })?;
    let command = if matches!(
        action.action,
        ursula_control::MembershipAction::InstallReplicaIdentity { .. }
    ) {
        let committed_index = result.fence_index.ok_or(ControlHttpError::Unavailable)?;
        let snapshot = linear_state(state).await?;
        let proofs =
            collect_evidence_with_retry(state, &snapshot, token, None, Some(action.group)).await?;
        let proof = proofs
            .into_iter()
            .next()
            .ok_or(ControlHttpError::Unavailable)?;
        if proof.committed_index < committed_index {
            return Err(OperationError::MissingEvidence {
                raft_group_id: action.group,
            }
            .into());
        }
        let observed = state
            .meta_control
            .as_ref()
            .ok_or(ControlHttpError::Unavailable)?
            .write(ControlCommand::Operation {
                command: OperationCommand::Observe {
                    token: token.clone(),
                    evidence: proof,
                },
                now_ms: state.wall_clock.unix_time_ms(),
            })
            .await?;
        match observed {
            ControlResponse::Operation(Ok(_)) => {}
            ControlResponse::Operation(Err(error)) => return Err(error.into()),
            _ => return Err(ControlHttpError::Unavailable),
        }
        OperationCommand::FinishReplicaFence {
            token: token.clone(),
            sequence: action.sequence,
            committed_index,
        }
    } else {
        OperationCommand::FinishAction {
            token: token.clone(),
            sequence: action.sequence,
        }
    };
    let response = state
        .meta_control
        .as_ref()
        .ok_or(ControlHttpError::Unavailable)?
        .write(ControlCommand::Operation {
            command,
            now_ms: state.wall_clock.unix_time_ms(),
        })
        .await?;
    match response {
        ControlResponse::Operation(Ok(_)) => Ok(()),
        ControlResponse::Operation(Err(error)) => Err(error.into()),
        _ => Err(ControlHttpError::Unavailable),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topology() -> ControlPlaneState {
        let mut state = ControlPlaneState::default();
        for node_id in 1..=4 {
            assert_eq!(
                state.apply(ControlCommand::RegisterNode {
                    node_id,
                    client_url: format!("http://client-{node_id}"),
                    cluster_url: format!("http://cluster-{node_id}"),
                    labels: BTreeMap::new(),
                    now_ms: 0,
                }),
                ControlResponse::Ok
            );
            let response = state.apply(ControlCommand::Operation {
                command: OperationCommand::ClaimProcess {
                    node_id,
                    expected_epoch: 0,
                    incarnation: ursula_proto::admin::ProcessIncarnation::from_bits(u128::from(
                        node_id,
                    )),
                },
                now_ms: 0,
            });
            assert!(matches!(response, ControlResponse::Operation(Ok(_))));
        }
        assert_eq!(
            state.apply(ControlCommand::SeedPlacement {
                raft_group_id: RaftGroupId(0),
                voters: BTreeSet::from([1, 2]),
                now_ms: 0
            }),
            ControlResponse::Ok
        );
        state
    }

    #[test]
    fn reactivation_certificate_covers_later_removal_and_excludes_uncertified_identities() {
        let mut state = topology();
        let group = RaftGroupId(0);
        let replica = |bits| ursula_proto::admin::ReplicaIdentity {
            generation: 1,
            incarnation: ursula_proto::admin::ProcessIncarnation::from_bits(bits),
        };
        state
            .operations
            .replicas
            .insert(3, ursula_control::ReplicaState::Active {
                identity: replica(3),
                installed_groups: BTreeMap::from([(group, 17)]),
            });
        state
            .operations
            .replicas
            .insert(4, ursula_control::ReplicaState::Active {
                identity: replica(4),
                installed_groups: BTreeMap::new(),
            });
        let mut leader = GroupObservation {
            process: ProcessIdentity {
                epoch: 1,
                incarnation: replica(2).incarnation,
            },
            metrics: RaftGroupMetrics {
                apply_failure: None,
                installed_replica_identities: BTreeMap::from([(3, replica(3))]),
                raft_group_id: 0,
                node_id: 2,
                current_term: Some(2),
                current_leader: Some(2),
                committed_index: Some(25),
                last_applied_index: Some(25),
                voter_ids: vec![1, 2, 4],
                learner_ids: vec![3],
                maintenance: ursula_proto::admin::RaftGroupMaintenanceState {
                    running: true,
                    recovery_ready: true,
                    membership_log_index: Some(24),
                    ..Default::default()
                },
                last_log_index: Some(25),
                committed_term: Some(2),
                last_applied_term: Some(2),
                snapshot_term: None,
                snapshot_index: None,
                purged_term: None,
                purged_index: None,
                log_bytes_since_snapshot: 0,
                log_entries_since_snapshot: 0,
                last_snapshot_bytes: 0,
                has_snapshot: false,
            },
            quorum: Some(QuorumPrefix {
                raft_group_id: 0,
                leader_id: 2,
                leader_term: 2,
                required_applied_index: 25,
            }),
        };
        let (identities, prefix) =
            replica_reactivation_certificate(&state, group, 3, &leader).unwrap();
        assert_eq!(identities, BTreeMap::from([(3, replica(3))]));
        assert_eq!(prefix, 25, "admission17 must not bypass later removal24");
        leader.metrics.voter_ids.push(3);
        assert!(matches!(
            replica_reactivation_certificate(&state, group, 3, &leader),
            Err(ControlHttpError::Operation(
                OperationError::InvalidTransition
            ))
        ));
        leader.metrics.voter_ids.pop();
        leader.metrics.maintenance.membership_joint = true;
        assert!(matches!(
            replica_reactivation_certificate(&state, group, 3, &leader),
            Err(ControlHttpError::Operation(
                OperationError::InvalidTransition
            ))
        ));
        leader.metrics.maintenance.membership_joint = false;
        leader.metrics.maintenance.membership_log_index = Some(26);
        assert!(matches!(
            replica_reactivation_certificate(&state, group, 3, &leader),
            Err(ControlHttpError::Operation(
                OperationError::MissingEvidence {
                    raft_group_id: RaftGroupId(0)
                }
            ))
        ));
    }

    #[tokio::test]
    async fn learner_catchup_confirms_a_leader_outside_the_desired_voter_set() {
        let mut state = topology();
        let mut servers = Vec::new();
        for node_id in 1..=4 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            state.nodes.get_mut(&node_id).unwrap().cluster_url =
                format!("http://{}", listener.local_addr().unwrap());
            let observation = GroupObservation {
                process: ProcessIdentity {
                    epoch: 1,
                    incarnation: ursula_proto::admin::ProcessIncarnation::from_bits(u128::from(
                        node_id,
                    )),
                },
                metrics: RaftGroupMetrics {
                    apply_failure: None,
                    installed_replica_identities: BTreeMap::new(),
                    raft_group_id: 0,
                    node_id,
                    current_term: Some(2),
                    current_leader: Some(3),
                    committed_index: Some(100),
                    last_applied_index: Some(100),
                    voter_ids: vec![1, 2, 3],
                    learner_ids: vec![4],
                    maintenance: ursula_proto::admin::RaftGroupMaintenanceState {
                        running: true,
                        recovery_ready: true,
                        membership_log_index: Some(99),
                        ..Default::default()
                    },
                    last_log_index: Some(100),
                    committed_term: Some(2),
                    last_applied_term: Some(2),
                    snapshot_term: None,
                    snapshot_index: None,
                    purged_term: None,
                    purged_index: None,
                    log_bytes_since_snapshot: 0,
                    log_entries_since_snapshot: 0,
                    last_snapshot_bytes: 0,
                    has_snapshot: false,
                },
                quorum: (node_id == 3).then_some(QuorumPrefix {
                    raft_group_id: 0,
                    leader_id: 3,
                    leader_term: 2,
                    required_applied_index: 100,
                }),
            };
            let router = axum::Router::new().route(
                "/__ursula/control/group/0/evidence",
                axum::routing::get(move || {
                    let observation = observation.clone();
                    async move { Json(observation) }
                }),
            );
            servers.push(tokio::spawn(async move {
                axum::serve(listener, router).await.unwrap();
            }));
        }
        wait_replica_prefix(
            None,
            &reqwest::Client::new(),
            &state,
            RaftGroupId(0),
            &BTreeSet::from([1, 2, 4]),
        )
        .await
        .unwrap();
        for server in servers {
            server.abort();
            assert!(server.await.unwrap_err().is_cancelled());
        }
    }

    #[test]
    fn redirects_follow_committed_topology_updates_without_restart() {
        let mut state = topology();
        let (sender, receiver) = tokio::sync::watch::channel(state.clone());
        let mut router = crate::ClientWriteLeaderRouter::with_static_topology(
            Some(1),
            [(2, "http://stale".to_owned())],
            BTreeMap::new(),
        );
        router.live_topology = Some(receiver);
        let error = ursula_runtime::RuntimeError::GroupNotHosted {
            core_id: ursula_shard::CoreId(0),
            raft_group_id: RaftGroupId(0),
        };
        assert_eq!(
            router.hosted_group_base(&error),
            Some((2, "http://client-2".to_owned()))
        );
        assert_eq!(
            state.apply(ControlCommand::CommitPlacement {
                raft_group_id: RaftGroupId(0),
                voters: BTreeSet::from([1, 3]),
                learners: BTreeSet::new(),
                draining: BTreeSet::new(),
                now_ms: 1
            }),
            ControlResponse::Ok
        );
        sender.send_replace(state);
        assert_eq!(
            router.hosted_group_base(&error),
            Some((3, "http://client-3".to_owned()))
        );
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn pending_prepare_survives_participant_restart_and_operator_takeover() {
        use std::sync::Arc;

        use ursula_control::OperationOutcome;
        use ursula_proto::admin::ProcessIncarnation;
        use ursula_proto::admin::ReplicaIdentity;

        // Every case runs through the durable meta dispatcher and production
        // action admission/engine preparation; no action receipt is fabricated.
        for restarted in [Some(1_u64), Some(2), Some(4), None] {
            let dir = tempfile::tempdir().unwrap();
            let meta = ursula_raft::MetaRaftHandle::new_durable(
                1,
                dir.path().to_path_buf(),
                Arc::new(openraft::Config::default().validate().unwrap()),
            )
            .await
            .unwrap();
            meta.initialize_membership(BTreeMap::from([(1, openraft::BasicNode::new("local"))]))
                .await
                .unwrap();
            meta.wait_for_current_leader(1, Duration::from_secs(5))
                .await
                .unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let evidence_url = format!("http://{}", listener.local_addr().unwrap());
            for node_id in [1_u64, 2, 4] {
                assert_eq!(
                    meta.write(ControlCommand::RegisterNode {
                        node_id,
                        client_url: format!("http://node{node_id}"),
                        cluster_url: if node_id == 4 {
                            evidence_url.clone()
                        } else {
                            format!("http://node{node_id}")
                        },
                        labels: BTreeMap::new(),
                        now_ms: 1,
                    })
                    .await
                    .unwrap(),
                    ControlResponse::Ok
                );
                let boot = ProcessIncarnation::from_bits(u128::from(node_id));
                let claimed = meta
                    .write(ControlCommand::Operation {
                        command: OperationCommand::ClaimProcess {
                            node_id,
                            expected_epoch: 0,
                            incarnation: boot.clone(),
                        },
                        now_ms: 1,
                    })
                    .await
                    .unwrap();
                let ControlResponse::Operation(Ok(OperationOutcome::ProcessClaimed(process))) =
                    claimed
                else {
                    panic!("claim rejected")
                };
                assert!(matches!(
                    meta.write(ControlCommand::Operation {
                        command: OperationCommand::RegisterReplica {
                            node_id,
                            process,
                            identity: ReplicaIdentity {
                                generation: 1,
                                incarnation: boot
                            }
                        },
                        now_ms: 1,
                    })
                    .await
                    .unwrap(),
                    ControlResponse::Operation(Ok(OperationOutcome::ReplicaRegistered))
                ));
            }
            assert_eq!(
                meta.write(ControlCommand::SeedPlacement {
                    raft_group_id: RaftGroupId(0),
                    voters: BTreeSet::from([1, 2]),
                    now_ms: 1
                })
                .await
                .unwrap(),
                ControlResponse::Ok
            );
            let initial = meta.read_linearizable_state().await.unwrap();
            let kind = OperationKind::MoveReplicas {
                source: 1,
                target: 4,
                groups: BTreeSet::from([RaftGroupId(0)]),
            };
            let acquired = meta
                .write(ControlCommand::Operation {
                    command: OperationCommand::Begin {
                        participants: participants(&initial, &kind).unwrap(),
                        kind,
                        executor: ProcessIncarnation::from_bits(50),
                        meta_voters: BTreeSet::from([1]),
                    },
                    now_ms: 1,
                })
                .await
                .unwrap();
            let ControlResponse::Operation(Ok(OperationOutcome::Acquired(mut token))) = acquired
            else {
                panic!("begin rejected")
            };
            let prepared = meta
                .write(ControlCommand::Operation {
                    command: OperationCommand::PrepareAction {
                        token: token.clone(),
                        group: RaftGroupId(0),
                        leader: 4,
                        action: ursula_control::MembershipAction::PrepareReplica,
                    },
                    now_ms: 1,
                })
                .await
                .unwrap();
            let ControlResponse::Operation(Ok(OperationOutcome::ActionPrepared(mut receipt))) =
                prepared
            else {
                panic!("prepare rejected")
            };
            let original = ursula_control::ActionRequest {
                token: token.clone(),
                action: receipt.clone(),
            };
            let mut config = ursula_config::UrsulaConfig::default();
            config.runtime.core_count = 1;
            let runtime = ursula_runtime::ShardRuntime::spawn(
                ursula_runtime::RuntimeConfig::from_ursula_config(&config.runtime, 1),
            )
            .unwrap();
            // A previous attempt already prepared/promoted the target but lost
            // its reply. Serve a real Raft quorum proof for that retry path.
            let registry = ursula_raft::RaftGroupHandleRegistry::default();
            let placement = ursula_shard::ShardPlacement {
                core_id: ursula_shard::CoreId(0),
                shard_id: ursula_shard::ShardId(0),
                raft_group_id: RaftGroupId(0),
            };
            let wal = ursula_raft::wal::RaftWal::start(
                dir.path().join("data"),
                ursula_config::WalFsync::Always,
                &ursula_shard::StaticShardMap::new(1, 1).unwrap(),
            )
            .unwrap();
            let store = wal
                .open(
                    placement,
                    ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics(),
                )
                .unwrap();
            let engine = ursula_raft::RaftGroupEngine::new_single_node(
                placement,
                4,
                openraft::BasicNode::new(evidence_url.clone()),
                Arc::new(openraft::Config::default().validate().unwrap()),
                store,
                ursula_raft::RaftGroupEngineOptions::default(),
            )
            .await
            .unwrap();
            engine
                .wait_for_current_leader(4, Duration::from_secs(5))
                .await
                .unwrap();
            registry.register_engine(&engine, None);
            let mut old_http = HttpState::with_raft_registry(runtime.clone(), registry.clone())
                .with_meta_control(meta.clone());
            old_http.configured_node_id = Some(4);
            old_http.process_incarnation = receipt.process.incarnation.clone();
            let served = Arc::new(std::sync::Mutex::new(old_http.clone()));
            let endpoint = served.clone();
            let router = axum::Router::new().route(
                "/__ursula/control/group/0/evidence",
                axum::routing::get(move |headers: HeaderMap| {
                    let http = endpoint.lock().unwrap().clone();
                    async move { evidence(State(http), Path(0), headers).await }
                }),
            );
            let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
            execute_action(&old_http, ursula_control::ActionRequest {
                token: token.clone(),
                action: receipt.clone(),
            })
            .await
            .unwrap();
            // Lose the success response before FinishAction. The prepared group
            // exists, while durable meta still retains the unresolved receipt.
            drop(old_http);
            if let Some(node_id) = restarted {
                let previous = match &initial.operations.processes[&node_id] {
                    ProcessState::Active(process) => process.clone(),
                    other => panic!("{other:?}"),
                };
                let replica = match &initial.operations.replicas[&node_id] {
                    ursula_control::ReplicaState::Active { identity, .. } => identity.clone(),
                    other => panic!("{other:?}"),
                };
                assert!(matches!(
                    meta.write(ControlCommand::Operation {
                        command: OperationCommand::RestartProcess {
                            node_id,
                            previous,
                            incarnation: ProcessIncarnation::from_bits(100 + u128::from(node_id)),
                            replica
                        },
                        now_ms: 2,
                    })
                    .await
                    .unwrap(),
                    ControlResponse::Operation(Ok(OperationOutcome::ProcessClaimed(_)))
                ));
                assert_eq!(
                    meta.read_linearizable_state()
                        .await
                        .unwrap()
                        .operations
                        .active
                        .unwrap()
                        .pending_action,
                    Some(receipt.clone())
                );
                if node_id == 4 {
                    let reassigned = meta
                        .write(ControlCommand::Operation {
                            command: OperationCommand::ReassignAction {
                                token: token.clone(),
                                leader: 4,
                                drained: None,
                            },
                            now_ms: 2,
                        })
                        .await
                        .unwrap();
                    let ControlResponse::Operation(Ok(OperationOutcome::ActionPrepared(next))) =
                        reassigned
                    else {
                        panic!("reassign rejected")
                    };
                    assert!(next.sequence > receipt.sequence);
                    receipt = next;
                }
            } else {
                let takeover = meta
                    .write(ControlCommand::Operation {
                        command: OperationCommand::TakeOver {
                            expected: token.clone(),
                            executor: ProcessIncarnation::from_bits(99),
                        },
                        now_ms: 2,
                    })
                    .await
                    .unwrap();
                let ControlResponse::Operation(Ok(OperationOutcome::Acquired(next))) = takeover
                else {
                    panic!("takeover rejected")
                };
                token = next;
            }
            let mut http = HttpState::with_raft_registry(runtime.clone(), registry)
                .with_meta_control(meta.clone());
            http.configured_node_id = Some(4);
            http.process_incarnation = receipt.process.incarnation.clone();
            *served.lock().unwrap() = http.clone();
            if restarted == Some(4) || restarted.is_none() {
                assert!(matches!(
                    execute_action(&http, original).await,
                    Err(ControlHttpError::Operation(OperationError::StaleExecutor))
                ));
            }
            execute_action(&http, ursula_control::ActionRequest {
                token: token.clone(),
                action: receipt.clone(),
            })
            .await
            .unwrap();
            assert_eq!(
                meta.write(ControlCommand::Operation {
                    command: OperationCommand::FinishAction {
                        token,
                        sequence: receipt.sequence
                    },
                    now_ms: 3
                })
                .await
                .unwrap(),
                ControlResponse::Operation(Ok(OperationOutcome::ActionFinished))
            );
            assert!(
                meta.read_linearizable_state()
                    .await
                    .unwrap()
                    .operations
                    .active
                    .unwrap()
                    .pending_action
                    .is_none()
            );
            server.abort();
            server.await.expect_err("evidence listener stopped");
            engine.shutdown().await.unwrap();
            assert!(runtime.shutdown_owners().await.is_empty());
            meta.shutdown().await.unwrap();
        }
    }

    #[test]
    fn participant_pins_are_derived_from_committed_inventory() {
        let state = topology();
        let kind = OperationKind::MoveReplicas {
            source: 1,
            target: 3,
            groups: BTreeSet::from([RaftGroupId(0)]),
        };
        let pins = participants(&state, &kind).unwrap();
        assert_eq!(
            pins.keys().copied().collect::<BTreeSet<_>>(),
            BTreeSet::from([1, 2, 3])
        );
        assert!(!pins.contains_key(&4));
    }
}
