//! Process-fenced membership steps and actual-configuration reconciliation.

use std::time::Duration;

use axum::Json;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use futures_util::StreamExt;
use openraft::BasicNode;
use openraft::storage::RaftStateMachine;
use openraft::vote::RaftLeaderId;
use serde::Deserialize;
use serde::Serialize;
use ursula_control::CommittedGroupConfiguration;
use ursula_control::CompletedMembershipMutation;
use ursula_control::ControlProjection;
use ursula_control::MAX_MEMBERSHIP_RECEIPTS;
use ursula_control::MembershipLogId;
use ursula_control::MembershipOutcome;
use ursula_control::MembershipStep;
use ursula_control::MigrationToken;
use ursula_control::PendingReceiverMutation;
use ursula_control::ReceiverFencePhase;
use ursula_control::ReceiverMutationKind;
use ursula_control::ReceiverProcess;
use ursula_control::ReplicaAppliedEvidence;
use ursula_proto::admin::PROCESS_INCARNATION_HEADER;
use ursula_shard::RaftGroupId;

use super::ManagedReceiver;
use super::replica::ReplicaRequest;
use crate::HttpState;

fn covers(applied: &MembershipLogId, prefix: &MembershipLogId) -> bool {
    applied.node_id != 0
        && prefix.node_id != 0
        && (applied == prefix || (applied.index > prefix.index && applied.term >= prefix.term))
}

impl ManagedReceiver {
    async fn membership_authority(
        &self,
        state: &HttpState,
        request: &PendingReceiverMutation,
        recovering: bool,
    ) -> Result<ControlProjection, String> {
        let view = self.fresh(state, &request.token, false).await?;
        let migration = view.state.active_migration().ok_or("no migration")?;
        let managed = migration.managed.as_ref().ok_or("no managed intent")?;
        let ledger = self.store.snapshot().map_err(|e| e.to_string())?;
        let own = self.store.identity().node.node_id;
        if migration.raft_group_id != request.raft_group_id
            || request.process != state.process_incarnation
            || !ledger.fence.as_ref().is_some_and(|fence| {
                fence.token == request.token
                    && fence.process == request.process
                    && if recovering {
                        matches!(
                            fence.phase,
                            ReceiverFencePhase::Active | ReceiverFencePhase::Activating
                        )
                    } else {
                        fence.phase == ReceiverFencePhase::Active
                    }
            })
            || (!recovering && managed.receivers.get(&own) != Some(&request.process))
        {
            return Err("membership work lacks current process and intent authority".to_owned());
        }
        let ReceiverMutationKind::ManagedMembership { step } = &request.operation else {
            return Err("typed membership action required".to_owned());
        };
        let valid = match step {
            MembershipStep::TransferLeader { epoch, node_id } => {
                *epoch == managed.request.expected_epoch
                    && migration.target_voters.contains(node_id)
            }
            MembershipStep::AddLearner {
                epoch,
                node_id,
                prefix,
            } => {
                *epoch == managed.request.expected_epoch
                    && migration.added_nodes.contains(node_id)
                    && managed.catchup_prefix.as_ref() == Some(prefix)
                    && (recovering || managed.prepared.contains(node_id))
            }
            MembershipStep::ChangeVoters {
                epoch,
                target_voters,
            } => {
                *epoch == managed.request.expected_epoch
                    && target_voters == &migration.target_voters
                    && managed.membership_may_have_changed
                    && (recovering
                        || managed.published_epoch.is_some()
                        || managed
                            .learner_applied
                            .keys()
                            .copied()
                            .collect::<std::collections::BTreeSet<_>>()
                            == migration.added_nodes)
            }
        };
        if !valid {
            return Err("membership step differs from its durable authorized intent".to_owned());
        }
        Ok(view)
    }

    fn validate_configuration(
        &self,
        view: &ControlProjection,
        configuration: &CommittedGroupConfiguration,
    ) -> Result<(), String> {
        configuration.validate()?;
        let migration = view.state.active_migration().ok_or("no migration")?;
        let managed = migration.managed.as_ref().ok_or("no managed intent")?;
        let source = &migration.from_voters;
        let target = &migration.target_voters;
        let legal = match configuration.voter_sets.as_slice() {
            [uniform] => uniform == source || uniform == target,
            [old, new] => old == source && new == target,
            _ => false,
        };
        if !legal
            || configuration.raft_group_id != migration.raft_group_id
            || !configuration.learners.is_subset(&migration.added_nodes)
            || !covers(
                &configuration.membership_log_id,
                &managed.request.source_membership.log_id,
            )
            || configuration.nodes.iter().any(|(id, address)| {
                view.state
                    .nodes
                    .get(id)
                    .is_none_or(|node| &node.cluster_url != address)
            })
            || (managed.published_epoch.is_some()
                && (configuration.voter_sets.as_slice() != [target.clone()]
                    || !configuration.learners.is_empty()))
        {
            return Err("actual data configuration is outside the immutable migration".to_owned());
        }
        Ok(())
    }

    async fn observe_configuration(
        &self,
        view: &ControlProjection,
    ) -> Result<CommittedGroupConfiguration, String> {
        let migration = view.state.active_migration().ok_or("no migration")?;
        let group = migration.raft_group_id;
        let endpoints = migration
            .from_voters
            .union(&migration.target_voters)
            .map(|id| {
                view.state
                    .nodes
                    .get(id)
                    .map(|node| (*id, node.cluster_url.clone()))
                    .ok_or("participant has no origin")
            })
            .collect::<Result<Vec<_>, _>>()?;
        let attempts = endpoints.into_iter().map(move |(id, address)| async move {
            ursula_raft::confirm_group_configuration(group, id, &address, Duration::from_secs(2))
                .await
        });
        let mut attempts = futures_util::stream::iter(attempts).buffer_unordered(5);
        let mut last = "no current data configuration quorum".to_owned();
        while let Some(attempt) = attempts.next().await {
            match attempt {
                Ok(configuration) => {
                    self.validate_configuration(view, &configuration)?;
                    return Ok(configuration);
                }
                Err(error) => last = error,
            }
        }
        Err(last)
    }

    fn step_applied(
        request: &PendingReceiverMutation,
        configuration: &CommittedGroupConfiguration,
    ) -> bool {
        let ReceiverMutationKind::ManagedMembership { step } = &request.operation else {
            return false;
        };
        match step {
            MembershipStep::TransferLeader { node_id, .. } => configuration.leader_id == *node_id,
            MembershipStep::AddLearner {
                node_id, prefix, ..
            } => {
                (configuration
                    .voter_sets
                    .iter()
                    .any(|voters| voters.contains(node_id))
                    || configuration.learners.contains(node_id))
                    && covers(&configuration.applied_log_id, prefix)
            }
            MembershipStep::ChangeVoters { target_voters, .. } => {
                configuration.voter_sets.as_slice() == [target_voters.clone()]
                    && configuration.learners.is_empty()
            }
        }
    }

    async fn complete_membership(
        &self,
        state: &HttpState,
        pending: PendingReceiverMutation,
        configuration: CommittedGroupConfiguration,
        outcome: MembershipOutcome,
        recovering: bool,
    ) -> Result<CompletedMembershipMutation, String> {
        let view = self
            .membership_authority(state, &pending, recovering)
            .await?;
        self.validate_configuration(&view, &configuration)?;
        let mut ledger = self.store.snapshot().map_err(|e| e.to_string())?;
        if ledger.pending.as_ref() != Some(&pending) {
            return Err("membership pending work changed".to_owned());
        }
        let receipt = CompletedMembershipMutation {
            request: pending,
            process: ReceiverProcess {
                node_id: self.store.identity().node.node_id,
                incarnation: state.process_incarnation.clone(),
            },
            configuration,
            outcome,
        };
        ledger.pending = None;
        ledger
            .membership_completed
            .insert(receipt.request.request_id.clone(), receipt.clone());
        self.store
            .persist(ledger)
            .await
            .map_err(|e| e.to_string())?;
        Ok(receipt)
    }

    pub(super) async fn reconcile_membership(
        &self,
        state: &HttpState,
        token: &MigrationToken,
    ) -> Result<(), String> {
        let mut ledger = self.store.snapshot().map_err(|e| e.to_string())?;
        let Some(mut pending) = ledger.pending.clone() else {
            return Ok(());
        };
        if pending.token.migration_id != token.migration_id {
            return Err("pending membership belongs to another intent".to_owned());
        }
        if pending.token != *token || pending.process != state.process_incarnation {
            pending.token = token.clone();
            pending.process = state.process_incarnation.clone();
            ledger.pending = Some(pending.clone());
            self.store
                .persist(ledger)
                .await
                .map_err(|e| e.to_string())?;
        }
        // Queue processing does not establish commit. Follow it with an actual
        // quorum/application observation. Takeover never resubmits old work
        // before all receiving processes have been recertified.
        crate::confirm_admin_command_submission(state).await?;
        let view = self.membership_authority(state, &pending, true).await?;
        let configuration = self.observe_configuration(&view).await?;
        self.complete_membership(
            state,
            pending,
            configuration,
            MembershipOutcome::Reconciled,
            true,
        )
        .await?;
        Ok(())
    }

    async fn mutate_membership(
        &self,
        state: &HttpState,
        request: ReplicaRequest,
    ) -> Result<CompletedMembershipMutation, String> {
        let _guard = self.gate.write().await;
        let pending = PendingReceiverMutation {
            token: request.token,
            raft_group_id: request.raft_group_id,
            request_id: request.request_id,
            process: state.process_incarnation.clone(),
            operation: request.operation,
        };
        let view = self.membership_authority(state, &pending, false).await?;
        if pending.request_id.is_empty() || pending.request_id.len() > 128 {
            return Err("invalid membership request id".to_owned());
        }
        let mut ledger = self.store.snapshot().map_err(|e| e.to_string())?;
        if let Some(receipt) = ledger.membership_completed.get(&pending.request_id) {
            if receipt.request != pending {
                return Err("membership request id reused with another payload".to_owned());
            }
            return Ok(receipt.clone());
        }
        if ledger.completed.as_ref().is_some_and(|receipt| {
            receipt.request.token == pending.token
                && receipt.request.request_id == pending.request_id
        }) {
            return Err("request id belongs to a replica action".to_owned());
        }
        if ledger.membership_completed.len() >= MAX_MEMBERSHIP_RECEIPTS {
            return Err("membership receipt generation budget exhausted".to_owned());
        }
        if let Some(old) = &ledger.pending {
            if old != &pending {
                return Err("another receiver mutation is unresolved".to_owned());
            }
        } else {
            self.barrier(state).await?;
            // Refuse initial follower submission before creating pending work.
            let observed = self.observe_configuration(&view).await?;
            if observed.leader_id != self.store.identity().node.node_id
                && !Self::step_applied(&pending, &observed)
            {
                return Err(format!(
                    "membership submission requires data leader {}",
                    observed.leader_id
                ));
            }
            if let ReceiverMutationKind::ManagedMembership {
                step: MembershipStep::TransferLeader { node_id, .. },
            } = &pending.operation
                && !observed
                    .voter_sets
                    .iter()
                    .any(|voters| voters.contains(node_id))
            {
                return Err("handoff target is not a current voter".to_owned());
            }
            ledger.pending = Some(pending.clone());
            self.store
                .persist(ledger)
                .await
                .map_err(|e| e.to_string())?;
        }
        crate::confirm_admin_command_submission(state).await?;
        let view = self.membership_authority(state, &pending, false).await?;
        let configuration = self.observe_configuration(&view).await?;
        if !Self::step_applied(&pending, &configuration) {
            if configuration.leader_id != self.store.identity().node.node_id {
                return Err(
                    "pending action must reconcile on a new generation after leader change"
                        .to_owned(),
                );
            }
            let _work = state
                .runtime
                .enter_group_work(pending.raft_group_id)
                .map_err(|e| e.to_string())?;
            let raft = state
                .raft_registry()
                .and_then(|registry| registry.get(pending.raft_group_id))
                .ok_or("membership replica is not hosted")?;
            let ReceiverMutationKind::ManagedMembership { step } = &pending.operation else {
                return Err("not a membership action".to_owned());
            };
            let action = async {
                match step {
                    MembershipStep::TransferLeader { node_id, .. } => {
                        if !configuration
                            .voter_sets
                            .iter()
                            .any(|voters| voters.contains(node_id))
                        {
                            return Err("handoff target is not a current voter".to_owned());
                        }
                        raft.trigger()
                            .transfer_leader(*node_id)
                            .await
                            .map_err(|e| e.to_string())?;
                    }
                    MembershipStep::AddLearner { node_id, .. } => {
                        let node = view
                            .state
                            .nodes
                            .get(node_id)
                            .ok_or("learner has no registered origin")?;
                        raft.add_learner(*node_id, BasicNode::new(&node.cluster_url), false)
                            .await
                            .map_err(|e| e.to_string())?;
                    }
                    MembershipStep::ChangeVoters { target_voters, .. } => {
                        if !target_voters
                            .iter()
                            .all(|node| configuration.nodes.contains_key(node))
                        {
                            return Err(
                                "target voters are not all present as voters/learners".to_owned()
                            );
                        }
                        // OpenRaft resumes its own joint-to-uniform protocol;
                        // never replace a joint configuration with the source.
                        raft.change_membership(target_voters.clone(), false)
                            .await
                            .map_err(|e| e.to_string())?;
                    }
                }
                Ok::<(), String>(())
            };
            crate::http_time::timeout(Duration::from_secs(5), action)
                .await
                .map_err(|_| {
                    "membership reply timed out; durable action remains pending".to_owned()
                })??;
        }
        let configuration = crate::http_time::timeout(Duration::from_secs(5), async {
            loop {
                let view = self.membership_authority(state, &pending, false).await?;
                match self.observe_configuration(&view).await {
                    Ok(configuration) if Self::step_applied(&pending, &configuration) => return Ok::<_, String>(configuration),
                    Ok(_) => {},
                    Err(error) => tracing::debug!(%error, "data quorum observation is not ready after membership submission"),
                }
                crate::http_time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .map_err(|_| {
            "membership result observation timed out; durable action remains pending".to_owned()
        })??;
        self.complete_membership(
            state,
            pending,
            configuration,
            MembershipOutcome::Applied,
            false,
        )
        .await
    }
}

fn process_matches(state: &HttpState, headers: &HeaderMap) -> bool {
    headers
        .get(PROCESS_INCARNATION_HEADER)
        .and_then(|value| value.to_str().ok())
        == Some(state.process_incarnation.as_str())
}

pub(super) async fn membership_mutation(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(request): Json<ReplicaRequest>,
) -> Response {
    if !process_matches(&state, &headers) {
        return (
            StatusCode::PRECONDITION_FAILED,
            "receiver process incarnation changed or is missing",
        )
            .into_response();
    }
    if !matches!(
        request.operation,
        ReceiverMutationKind::ManagedMembership { .. }
    ) {
        return (StatusCode::BAD_REQUEST, "typed membership action required").into_response();
    }
    let Some(receiver) = state.managed_receiver.clone() else {
        return (StatusCode::CONFLICT, "managed receiver unavailable").into_response();
    };
    match crate::http_task::spawn(async move { receiver.mutate_membership(&state, request).await })
        .await
    {
        Ok(Ok(receipt)) => Json(receipt).into_response(),
        Ok(Err(error)) => (StatusCode::CONFLICT, error).into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("membership task failed: {error}"),
        )
            .into_response(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AppliedRequest {
    pub token: MigrationToken,
    pub raft_group_id: RaftGroupId,
    pub prefix: MembershipLogId,
}

pub(super) async fn applied_evidence(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(request): Json<AppliedRequest>,
) -> Response {
    if !process_matches(&state, &headers) {
        return (
            StatusCode::PRECONDITION_FAILED,
            "receiver process incarnation changed or is missing",
        )
            .into_response();
    }
    let Some(receiver) = state.managed_receiver.clone() else {
        return (StatusCode::CONFLICT, "managed receiver unavailable").into_response();
    };
    let result = async {
        let _guard = receiver.gate.read().await;
        let view = receiver.fresh(&state, &request.token, false).await?;
        let migration = view.state.active_migration().ok_or("no migration")?;
        let managed = migration.managed.as_ref().ok_or("no managed intent")?;
        let own = receiver.store.identity().node.node_id;
        let ledger = receiver.store.snapshot().map_err(|e| e.to_string())?;
        if migration.raft_group_id != request.raft_group_id
            || managed.receivers.get(&own) != Some(&state.process_incarnation)
            || !ledger.fence.as_ref().is_some_and(|fence| {
                fence.token == request.token
                    && fence.process == state.process_incarnation
                    && fence.phase == ReceiverFencePhase::Active
            })
            || !managed
                .catchup_prefix
                .as_ref()
                .is_some_and(|prefix| covers(&request.prefix, prefix))
        {
            return Err(
                "applied evidence lacks current process and captured prefix authority".to_owned(),
            );
        }
        let _work = state
            .runtime
            .enter_group_work(request.raft_group_id)
            .map_err(|e| e.to_string())?;
        let raft = state
            .raft_registry()
            .and_then(|registry| registry.get(request.raft_group_id))
            .ok_or("replica is not hosted")?;
        let applied = raft
            .with_state_machine(|machine| Box::pin(async move { machine.applied_state().await }))
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?
            .0
            .ok_or("replica has no applied state")?;
        let applied_log_id = MembershipLogId {
            term: applied.committed_leader_id().term(),
            node_id: *applied.committed_leader_id().node_id(),
            index: applied.index(),
        };
        if !covers(&applied_log_id, &request.prefix) {
            return Err("replica has not applied the requested committed prefix".to_owned());
        }
        Ok::<_, String>(ReplicaAppliedEvidence {
            process: ReceiverProcess {
                node_id: own,
                incarnation: state.process_incarnation.clone(),
            },
            applied_log_id,
        })
    }
    .await;
    match result {
        Ok(evidence) => Json(evidence).into_response(),
        Err(error) => (StatusCode::CONFLICT, error).into_response(),
    }
}
