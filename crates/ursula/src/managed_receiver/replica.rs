//! Durable replica prepare/release, separate from the receiver's process fence.
//! The checkpoint describes work before any core action and retains its receipt.

use std::time::Duration;

use axum::Json;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use futures_util::StreamExt;
use serde::Deserialize;
use serde::Serialize;
use ursula_control::CompletedReceiverMutation;
use ursula_control::ControlProjection;
use ursula_control::MigrationToken;
use ursula_control::PendingReceiverMutation;
use ursula_control::ReceiverFencePhase;
use ursula_control::ReceiverLedger;
use ursula_control::ReceiverMutationKind;
use ursula_control::ReceiverProcess;
use ursula_control::ReplicaAssignment;
use ursula_control::ReplicaAssignmentPhase;
use ursula_control::ReplicaMutationResult;
use ursula_control::ReplicaRetirementEvidence;
use ursula_proto::admin::PROCESS_INCARNATION_HEADER;
use ursula_shard::RaftGroupId;

use super::ManagedReceiver;
use crate::HttpState;

#[cfg(all(test, not(madsim)))]
#[path = "replica_tests.rs"]
mod tests;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReplicaRequest {
    pub token: MigrationToken,
    pub raft_group_id: RaftGroupId,
    pub request_id: String,
    pub operation: ReceiverMutationKind,
}

impl ManagedReceiver {
    fn install_hosting(&self, state: &HttpState, ledger: &ReceiverLedger) -> Result<(), String> {
        let registry = state
            .raft_registry()
            .ok_or("replica work requires a data Raft registry")?;
        let groups = ledger
            .assignments
            .keys()
            .filter(|group| ledger.may_restore(**group))
            .copied()
            .collect();
        registry.set_managed_hosting(groups);
        Ok(())
    }

    async fn replica_authority(
        &self,
        state: &HttpState,
        request: &PendingReceiverMutation,
        recovering: bool,
    ) -> Result<ControlProjection, String> {
        let view = self.fresh(state, &request.token, false).await?;
        let own = self.store.identity().node.node_id;
        let migration = view.state.active_migration().ok_or("no migration")?;
        let managed = migration.managed.as_ref().ok_or("no managed migration")?;
        let ledger = self.store.snapshot().map_err(|e| e.to_string())?;
        let fence = ledger.fence.as_ref().ok_or("receiver has no fence")?;
        if migration.raft_group_id != request.raft_group_id
            || fence.token != request.token
            || fence.process != state.process_incarnation
            || request.process != state.process_incarnation
            || (!recovering
                && (fence.phase != ReceiverFencePhase::Active
                    || managed.receivers.get(&own) != Some(&state.process_incarnation)))
            || (recovering
                && !matches!(
                    fence.phase,
                    ReceiverFencePhase::Active | ReceiverFencePhase::Activating
                ))
        {
            return Err("replica work lacks current process and intent authority".to_owned());
        }
        match &request.operation {
            ReceiverMutationKind::PrepareReplica { epoch } => {
                let expected = managed.request.expected_epoch;
                if !migration.added_nodes.contains(&own) || *epoch != expected {
                    return Err(
                        "prepare differs from the intent's added replica or epoch".to_owned()
                    );
                }
            }
            ReceiverMutationKind::ReleaseReplica {
                epoch,
                membership_log_id,
            } => {
                let placement = view
                    .state
                    .placements
                    .get(&request.raft_group_id)
                    .ok_or("no placement")?;
                if !migration.removed_voters.contains(&own)
                    || managed.published_epoch != Some(*epoch)
                    || placement.epoch != *epoch
                    || placement.voters != migration.target_voters
                    || (!recovering
                        && (managed.verification_generation != Some(request.token.generation)
                            || !managed
                                .final_membership
                                .as_ref()
                                .is_some_and(|final_proof| {
                                    final_proof.membership.log_id == *membership_log_id
                                })))
                {
                    return Err(
                        "release requires published placement and verified target membership"
                            .to_owned(),
                    );
                }
                self.confirm_release_membership(&view, request, recovering)
                    .await?;
            }
            ReceiverMutationKind::Membership => {
                return Err("membership work requires its own reconciliation protocol".to_owned());
            }
        }
        Ok(view)
    }

    async fn confirm_release_membership(
        &self,
        view: &ControlProjection,
        request: &PendingReceiverMutation,
        recovering: bool,
    ) -> Result<(), String> {
        let migration = view.state.active_migration().ok_or("no migration")?;
        let ReceiverMutationKind::ReleaseReplica {
            membership_log_id, ..
        } = &request.operation
        else {
            return Err("not a release".to_owned());
        };
        let endpoints = migration
            .target_voters
            .iter()
            .map(|id| {
                view.state
                    .nodes
                    .get(id)
                    .map(|node| (*id, node.cluster_url.clone()))
                    .ok_or("target node has no registered origin")
            })
            .collect::<Result<Vec<_>, _>>()?;
        let group = request.raft_group_id;
        let attempts = endpoints.into_iter().map(move |(id, address)| async move {
            ursula_raft::confirm_group_membership(group, id, &address, Duration::from_secs(2)).await
        });
        let mut attempts =
            futures_util::stream::iter(attempts).buffer_unordered(migration.target_voters.len());
        let mut last = "no target data quorum".to_owned();
        while let Some(attempt) = attempts.next().await {
            let observed = match attempt {
                Ok(value) => value,
                Err(error) => {
                    last = error;
                    continue;
                }
            };
            if observed.membership.voters != migration.target_voters
                || !observed.membership.learners.is_empty()
                || observed
                    .membership
                    .voters
                    .contains(&self.store.identity().node.node_id)
                || (!recovering && observed.membership.log_id != *membership_log_id)
                || (recovering
                    && (observed.membership.log_id.index < membership_log_id.index
                        || observed.membership.log_id.term < membership_log_id.term
                        || (observed.membership.log_id.index == membership_log_id.index
                            && observed.membership.log_id != *membership_log_id)))
                || observed.nodes.iter().any(|(id, address)| {
                    view.state
                        .nodes
                        .get(id)
                        .is_none_or(|node| &node.cluster_url != address)
                })
            {
                return Err("actual data membership differs from the release witness".to_owned());
            }
            return Ok(());
        }
        Err(last)
    }

    async fn perform_replica(
        &self,
        state: &HttpState,
        request: PendingReceiverMutation,
        recovering: bool,
    ) -> Result<CompletedReceiverMutation, String> {
        self.replica_authority(state, &request, recovering).await?;
        let ledger = self.store.snapshot().map_err(|e| e.to_string())?;
        if ledger.pending.as_ref() != Some(&request) {
            return Err("durable pending work changed".to_owned());
        }
        self.install_hosting(state, &ledger)?;
        let own = ReceiverProcess {
            node_id: self.store.identity().node.node_id,
            incarnation: state.process_incarnation.clone(),
        };
        let result = match &request.operation {
            ReceiverMutationKind::PrepareReplica { .. } => {
                state
                    .runtime
                    .warm_group(request.raft_group_id)
                    .await
                    .map_err(|e| e.to_string())?;
                ReplicaMutationResult::Prepared { process: own }
            }
            ReceiverMutationKind::ReleaseReplica {
                epoch,
                membership_log_id,
            } => {
                state
                    .runtime
                    .retire_group_engine(request.raft_group_id)
                    .await
                    .map_err(|e| e.to_string())?;
                ReplicaMutationResult::Released {
                    evidence: ReplicaRetirementEvidence {
                        process: own,
                        placement_epoch: *epoch,
                        membership_log_id: membership_log_id.clone(),
                        work_drained: true,
                        snapshot_references_retired: true,
                        local_records_reclaimed: true,
                    },
                }
            }
            ReceiverMutationKind::Membership => {
                return Err("unsupported membership operation".to_owned());
            }
        };
        // A new metadata generation may have been committed during core/S3 I/O.
        // Leave pending authority intact rather than answering under a stale token.
        self.replica_authority(state, &request, recovering).await?;
        let mut ledger = self.store.snapshot().map_err(|e| e.to_string())?;
        if ledger.pending.as_ref() != Some(&request) {
            return Err("pending work changed after core action".to_owned());
        }
        let assignment = ledger
            .assignments
            .get_mut(&request.raft_group_id)
            .ok_or("missing assignment")?;
        assignment.phase = match request.operation {
            ReceiverMutationKind::PrepareReplica { .. } => ReplicaAssignmentPhase::Hosted,
            _ => ReplicaAssignmentPhase::Retired,
        };
        let completed = CompletedReceiverMutation { request, result };
        ledger.pending = None;
        ledger.completed = Some(completed.clone());
        self.store
            .persist(ledger)
            .await
            .map_err(|e| e.to_string())?;
        Ok(completed)
    }

    pub(super) async fn reconcile_replica(
        &self,
        state: &HttpState,
        token: &MigrationToken,
    ) -> Result<(), String> {
        let mut ledger = self.store.snapshot().map_err(|e| e.to_string())?;
        let Some(mut pending) = ledger.pending.clone() else {
            return Ok(());
        };
        if matches!(pending.operation, ReceiverMutationKind::Membership) {
            return Err("pending membership needs actual membership reconciliation".to_owned());
        }
        if pending.token.migration_id != token.migration_id {
            return Err("pending replica work belongs to another intent".to_owned());
        }
        if pending.token != *token || pending.process != state.process_incarnation {
            pending.token = token.clone();
            pending.process = state.process_incarnation.clone();
            ledger.pending = Some(pending.clone());
            let assignment = ledger
                .assignments
                .get_mut(&pending.raft_group_id)
                .ok_or("pending assignment is missing")?;
            assignment.generation = token.generation;
            self.store
                .persist(ledger)
                .await
                .map_err(|e| e.to_string())?;
        }
        self.perform_replica(state, pending, true).await?;
        Ok(())
    }

    async fn mutate_replica(
        &self,
        state: &HttpState,
        request: ReplicaRequest,
    ) -> Result<CompletedReceiverMutation, String> {
        let _guard = self.gate.write().await;
        let pending = PendingReceiverMutation {
            token: request.token,
            raft_group_id: request.raft_group_id,
            request_id: request.request_id,
            operation: request.operation,
            process: state.process_incarnation.clone(),
        };
        self.replica_authority(state, &pending, false).await?;
        if pending.request_id.is_empty() || pending.request_id.len() > 128 {
            return Err("invalid replica request id".to_owned());
        }
        let mut ledger = self.store.snapshot().map_err(|e| e.to_string())?;
        if let Some(completed) = &ledger.completed
            && completed.request.token == pending.token
        {
            if completed.request != pending {
                return Err("replica request id was reused with another payload".to_owned());
            }
            return Ok(completed.clone());
        }
        if let Some(old) = &ledger.pending {
            if old != &pending {
                return Err("another receiver mutation is unresolved".to_owned());
            }
        } else {
            self.barrier(state).await?;
            let (mut epoch, phase) = match pending.operation {
                ReceiverMutationKind::PrepareReplica { epoch } => {
                    (epoch, ReplicaAssignmentPhase::Preparing)
                }
                ReceiverMutationKind::ReleaseReplica { epoch, .. } => {
                    (epoch, ReplicaAssignmentPhase::Retiring)
                }
                ReceiverMutationKind::Membership => return Err("unsupported operation".to_owned()),
            };
            let old_phase = ledger
                .assignments
                .get(&pending.raft_group_id)
                .map(|assignment| assignment.phase);
            if matches!(
                pending.operation,
                ReceiverMutationKind::PrepareReplica { .. }
            ) {
                epoch = epoch.max(
                    ledger
                        .assignments
                        .get(&pending.raft_group_id)
                        .map_or(epoch, |assignment| assignment.epoch),
                );
            }
            let phase = match (phase, old_phase) {
                (ReplicaAssignmentPhase::Preparing, Some(ReplicaAssignmentPhase::Hosted)) => {
                    ReplicaAssignmentPhase::Hosted
                }
                (ReplicaAssignmentPhase::Retiring, Some(ReplicaAssignmentPhase::Retired)) => {
                    ReplicaAssignmentPhase::Retired
                }
                _ => phase,
            };
            ledger
                .assignments
                .insert(pending.raft_group_id, ReplicaAssignment {
                    epoch,
                    generation: pending.token.generation,
                    migration_id: pending.token.migration_id,
                    phase,
                });
            ledger.pending = Some(pending.clone());
            self.store
                .persist(ledger)
                .await
                .map_err(|e| e.to_string())?;
        }
        self.perform_replica(state, pending, false).await
    }
}

pub(super) async fn replica_prepare(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(request): Json<ReplicaRequest>,
) -> Response {
    replica_mutation(state, headers, request, false).await
}
pub(super) async fn replica_release(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(request): Json<ReplicaRequest>,
) -> Response {
    replica_mutation(state, headers, request, true).await
}

async fn replica_mutation(
    state: HttpState,
    headers: HeaderMap,
    request: ReplicaRequest,
    release: bool,
) -> Response {
    if headers
        .get(PROCESS_INCARNATION_HEADER)
        .and_then(|v| v.to_str().ok())
        != Some(state.process_incarnation.as_str())
    {
        return (
            StatusCode::PRECONDITION_FAILED,
            "receiver process incarnation changed or is missing",
        )
            .into_response();
    }
    if (release
        && !matches!(
            request.operation,
            ReceiverMutationKind::ReleaseReplica { .. }
        ))
        || (!release
            && !matches!(
                request.operation,
                ReceiverMutationKind::PrepareReplica { .. }
            ))
    {
        return (StatusCode::BAD_REQUEST, "replica action differs from route").into_response();
    }
    let Some(receiver) = state.managed_receiver.clone() else {
        return (StatusCode::CONFLICT, "managed receiver unavailable").into_response();
    };
    match crate::http_task::spawn(async move { receiver.mutate_replica(&state, request).await })
        .await
    {
        Ok(Ok(receipt)) => Json(receipt).into_response(),
        Ok(Err(error)) => (StatusCode::CONFLICT, error).into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("replica task failed: {error}"),
        )
            .into_response(),
    }
}
