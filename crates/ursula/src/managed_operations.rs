//! Managed HTTP operations and a meta-leader-owned, resumable executor.
//! Every iteration starts from a fresh quorum projection. Local timers and HTTP
//! replies schedule work; only durable tokens and actual Raft facts authorize it.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::extract::Path;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use serde::de::DeserializeOwned;
use ursula_control::CommittedGroupConfiguration;
use ursula_control::CompletedMembershipMutation;
use ursula_control::CompletedReceiverMutation;
use ursula_control::ControlCommand;
use ursula_control::ControlProjection;
use ursula_control::ControlResponse;
use ursula_control::FinalMembershipEvidence;
use ursula_control::GroupMigration;
use ursula_control::MembershipLogId;
use ursula_control::MembershipOutcome;
use ursula_control::MembershipStep;
use ursula_control::MigrationRequest;
use ursula_control::MigrationToken;
use ursula_control::MigrationUpdate;
use ursula_control::NodeRegistration;
use ursula_control::ReceiverFencePhase;
use ursula_control::ReceiverLedger;
use ursula_control::ReceiverMutationKind;
use ursula_control::ReceiverProcess;
use ursula_control::ReplicaAppliedEvidence;
use ursula_control::ReplicaMutationResult;
use ursula_proto::admin::PROCESS_INCARNATION_HEADER;
use ursula_proto::admin::ProcessIncarnation;
use ursula_shard::RaftGroupId;

use crate::HttpState;
use crate::managed_receiver::ManagedReceiver;
use crate::managed_receiver::ReceiverInventory;

const IO_TIMEOUT: Duration = Duration::from_secs(12);
const MAX_REPLY_BYTES: usize = 1024 * 1024;

pub(crate) use ursula_control::MigrationOperationRequest as OperationRequest;

enum OperationError {
    Conflict(String),
    Unavailable(String),
}

pub(crate) fn router(state: HttpState) -> Router {
    Router::new()
        .route("/__ursula/control/operations", get(list).post(submit))
        .route("/__ursula/control/operations/{migration_id}", get(status))
        .layer(axum::extract::DefaultBodyLimit::max(16 * 1024))
        .merge(
            Router::new()
                .route(
                    "/__ursula/control/nodes",
                    axum::routing::post(register_node),
                )
                .layer(axum::extract::DefaultBodyLimit::max(128 * 1024)),
        )
        .with_state(state)
}

fn coordinator(state: HttpState) -> Result<Coordinator, String> {
    let receiver = state
        .managed_receiver
        .clone()
        .ok_or("managed control is absent")?;
    let client = reqwest::Client::builder()
        .timeout(IO_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| error.to_string())?;
    Ok(Coordinator {
        state,
        receiver,
        client,
    })
}

async fn list(State(state): State<HttpState>) -> Response {
    match coordinator(state) {
        Ok(control) => match control.read().await {
            Ok(view) => Json(view).into_response(),
            Err(error) => (StatusCode::SERVICE_UNAVAILABLE, error).into_response(),
        },
        Err(error) => (StatusCode::CONFLICT, error).into_response(),
    }
}

async fn status(State(state): State<HttpState>, Path(id): Path<u64>) -> Response {
    match coordinator(state) {
        Ok(control) => match control.read().await {
            Ok(view) => match view.state.migrations.get(&id) {
                Some(operation) => Json(operation).into_response(),
                None => (StatusCode::NOT_FOUND, "unknown operation").into_response(),
            },
            Err(error) => (StatusCode::SERVICE_UNAVAILABLE, error).into_response(),
        },
        Err(error) => (StatusCode::CONFLICT, error).into_response(),
    }
}

async fn submit(State(state): State<HttpState>, Json(request): Json<OperationRequest>) -> Response {
    let control = match coordinator(state) {
        Ok(control) => control,
        Err(error) => return (StatusCode::CONFLICT, error).into_response(),
    };
    // Losing the HTTP response cannot cancel the durable submit. The request
    // key remains discoverable via the fresh operation list after reconnection.
    let result = crate::http_task::spawn(async move { control.submit(request).await })
        .await
        .unwrap_or_else(|error| Err(OperationError::Unavailable(error.to_string())));
    render_result(result, StatusCode::ACCEPTED)
}

fn render_result(result: Result<ControlResponse, OperationError>, status: StatusCode) -> Response {
    match result {
        Ok(response) => (status, Json(response)).into_response(),
        Err(OperationError::Conflict(error)) => (StatusCode::CONFLICT, error).into_response(),
        Err(OperationError::Unavailable(error)) => {
            (StatusCode::SERVICE_UNAVAILABLE, error).into_response()
        }
    }
}

async fn register_node(
    State(state): State<HttpState>,
    Json(node): Json<NodeRegistration>,
) -> Response {
    let control = match coordinator(state) {
        Ok(control) => control,
        Err(error) => return (StatusCode::CONFLICT, error).into_response(),
    };
    let result = crate::http_task::spawn(async move {
        match control
            .send(ControlCommand::RegisterManagedNode {
                node,
                now_ms: control.state.unix_time_ms(),
            })
            .await
            .map_err(OperationError::Unavailable)?
        {
            ControlResponse::Rejected { reason } => Err(OperationError::Conflict(reason)),
            response => Ok(response),
        }
    })
    .await
    .unwrap_or_else(|error| Err(OperationError::Unavailable(error.to_string())));
    render_result(result, StatusCode::OK)
}

pub(crate) struct Coordinator {
    state: HttpState,
    receiver: Arc<ManagedReceiver>,
    client: reqwest::Client,
}

fn covers(applied: &MembershipLogId, prefix: &MembershipLogId) -> bool {
    applied == prefix || (applied.index > prefix.index && applied.term >= prefix.term)
}

impl Coordinator {
    async fn read(&self) -> Result<ControlProjection, String> {
        let mut last = "no fresh meta quorum".to_owned();
        for id in &self.receiver.recipe.initial_meta_voters {
            let node = self
                .receiver
                .recipe
                .nodes
                .get(id)
                .ok_or("meta voter is absent")?;
            match ursula_raft::read_control_projection(
                &self.receiver.recipe.identity,
                *id,
                &node.cluster_url,
                Duration::from_secs(2),
            )
            .await
            {
                Ok(view) => return Ok(view),
                Err(error) => last = error.to_string(),
            }
        }
        Err(last)
    }

    async fn send(&self, command: ControlCommand) -> Result<ControlResponse, String> {
        let mut last = "no writable meta quorum".to_owned();
        for id in &self.receiver.recipe.initial_meta_voters {
            let node = self
                .receiver
                .recipe
                .nodes
                .get(id)
                .ok_or("meta voter is absent")?;
            match ursula_raft::write_control_command(
                &self.receiver.recipe.identity,
                *id,
                &node.cluster_url,
                &command,
                IO_TIMEOUT,
            )
            .await
            {
                Ok(response) => return Ok(response),
                Err(error) => last = error.to_string(),
            }
        }
        Err(last)
    }

    async fn write(&self, command: ControlCommand) -> Result<ControlResponse, String> {
        match self.send(command).await? {
            ControlResponse::Rejected { reason } => Err(reason),
            response => Ok(response),
        }
    }

    async fn submit(&self, request: OperationRequest) -> Result<ControlResponse, OperationError> {
        let view = self.read().await.map_err(OperationError::Unavailable)?;
        // Reuse the original observed source certificate on replay. Capturing a
        // new certificate after completion would incorrectly conflict by key.
        let old = view.state.migrations.values().find_map(|migration| {
            migration
                .managed
                .as_ref()
                .filter(|managed| managed.request.operation_key == request.operation_key)
        });
        let intent = if let Some(old) = old {
            if old.request.raft_group_id != request.raft_group_id
                || old.request.expected_epoch != request.expected_epoch
                || old.request.target_voters != request.target_voters
                || old.request.target_policy != request.target_policy
            {
                return Err(OperationError::Conflict(
                    "operation key identifies a different immutable request".to_owned(),
                ));
            }
            old.request.clone()
        } else {
            let placement = view
                .state
                .placements
                .get(&request.raft_group_id)
                .ok_or_else(|| OperationError::Conflict("group is absent".to_owned()))?;
            if placement.epoch != request.expected_epoch {
                return Err(OperationError::Conflict(
                    "placement epoch CAS failed".to_owned(),
                ));
            }
            let configuration = self
                .configuration(&view, request.raft_group_id, &placement.voters)
                .await
                .map_err(OperationError::Unavailable)?;
            let source_membership = configuration
                .uniform_membership()
                .map_err(OperationError::Conflict)?;
            if source_membership.voters != placement.voters
                || !source_membership.learners.is_empty()
            {
                return Err(OperationError::Conflict(
                    "actual source membership differs from settled placement".to_owned(),
                ));
            }
            MigrationRequest {
                operation_key: request.operation_key,
                raft_group_id: request.raft_group_id,
                expected_epoch: request.expected_epoch,
                source_membership,
                target_voters: request.target_voters,
                target_policy: request.target_policy,
            }
        };
        match self
            .send(ControlCommand::SubmitMigration {
                request: intent,
                now_ms: self.state.unix_time_ms(),
            })
            .await
            .map_err(OperationError::Unavailable)?
        {
            ControlResponse::Rejected { reason } => Err(OperationError::Conflict(reason)),
            response => Ok(response),
        }
    }

    async fn configuration(
        &self,
        view: &ControlProjection,
        group: RaftGroupId,
        participants: &BTreeSet<u64>,
    ) -> Result<CommittedGroupConfiguration, String> {
        let mut last = "no fresh data configuration".to_owned();
        for id in participants {
            let node = view
                .state
                .nodes
                .get(id)
                .ok_or("data participant is unregistered")?;
            match ursula_raft::confirm_group_configuration(
                group,
                *id,
                &node.cluster_url,
                Duration::from_secs(2),
            )
            .await
            {
                Ok(configuration) => {
                    if configuration.nodes.iter().any(|(id, url)| {
                        view.state
                            .nodes
                            .get(id)
                            .is_none_or(|node| node.cluster_url != *url)
                    }) {
                        return Err("data configuration has an unregistered origin".to_owned());
                    }
                    return Ok(configuration);
                }
                Err(error) => last = error,
            }
        }
        Err(last)
    }

    async fn reply<T: DeserializeOwned>(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<T, String> {
        let mut response = request.send().await.map_err(|error| error.to_string())?;
        let status = response.status();
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|error| error.to_string())? {
            if bytes.len().saturating_add(chunk.len()) > MAX_REPLY_BYTES {
                return Err("control reply exceeds 1 MiB".to_owned());
            }
            bytes.extend_from_slice(&chunk);
        }
        if !status.is_success() {
            return Err(format!(
                "receiver HTTP {status}: {}",
                String::from_utf8_lossy(&bytes)
            ));
        }
        serde_json::from_slice(&bytes).map_err(|error| error.to_string())
    }

    fn admin_url<'a>(&self, view: &'a ControlProjection, id: u64) -> Result<&'a str, String> {
        view.state
            .nodes
            .get(&id)
            .and_then(|node| node.admin_url.as_deref())
            .ok_or_else(|| "participant lacks registered admin origin".to_owned())
    }

    async fn processes(
        &self,
        view: &ControlProjection,
        peers: &BTreeSet<u64>,
    ) -> Result<BTreeMap<u64, ProcessIncarnation>, String> {
        let mut processes = BTreeMap::new();
        for id in peers {
            let url = self.admin_url(view, *id)?;
            let inventory: ReceiverInventory = self
                .reply(
                    self.client
                        .get(format!("{url}/__ursula/control/receiver/process")),
                )
                .await?;
            let node = view.state.nodes.get(id).ok_or("participant is absent")?;
            if inventory.protocol_version != crate::managed_receiver::RECEIVER_PROTOCOL_VERSION {
                return Err("receiver protocol lacks required snapshot-pruning barriers".to_owned());
            }
            if inventory.identity.cluster != view.identity
                || inventory.identity.node.node_id != *id
                || inventory.identity.node.cluster_url != node.cluster_url
                || inventory.identity.node.client_url != node.client_url
                || Some(&inventory.identity.node.admin_url) != node.admin_url.as_ref()
                || inventory.identity.node.labels != node.labels
            {
                return Err("receiving process differs from registered identity".to_owned());
            }
            processes.insert(*id, inventory.process);
        }
        Ok(processes)
    }

    async fn call<T: DeserializeOwned>(
        &self,
        view: &ControlProjection,
        id: u64,
        process: &ProcessIncarnation,
        path: &str,
        body: serde_json::Value,
    ) -> Result<T, String> {
        let url = self.admin_url(view, id)?;
        self.reply(
            self.client
                .post(format!("{url}/__ursula/control/receiver/{path}"))
                .header(PROCESS_INCARNATION_HEADER, process.as_str())
                .json(&body),
        )
        .await
    }

    async fn update(
        &self,
        token: &MigrationToken,
        revision: u64,
        update: MigrationUpdate,
    ) -> Result<(), String> {
        match self
            .write(ControlCommand::UpdateMigration {
                token: token.clone(),
                expected_revision: revision,
                update,
                now_ms: self.state.unix_time_ms(),
            })
            .await?
        {
            ControlResponse::Ok => Ok(()),
            _ => Err("unexpected meta update response".to_owned()),
        }
    }

    async fn membership(
        &self,
        view: &ControlProjection,
        token: &MigrationToken,
        migration: &GroupMigration,
        leader: u64,
        step: MembershipStep,
        key: &str,
    ) -> Result<(), String> {
        let managed = migration
            .managed
            .as_ref()
            .ok_or("managed intent is absent")?;
        let process = managed
            .receivers
            .get(&leader)
            .ok_or("leader has no certified process")?;
        // Activation may have reconciled a lost native reply without completing
        // its action. Never mistake that historical receipt for action success.
        let url = self.admin_url(view, leader)?;
        let ledger: ReceiverLedger = self
            .reply(self.client.get(format!("{url}/__ursula/control/receiver")))
            .await?;
        let request_id = (0..ursula_control::MAX_MEMBERSHIP_RECEIPTS)
            .map(|attempt| format!("{key}-{attempt}"))
            .find(|id| !ledger.membership_completed.contains_key(id));
        let Some(request_id) = request_id.filter(|_| {
            ledger.membership_completed.len() < ursula_control::MAX_MEMBERSHIP_RECEIPTS
        }) else {
            self.write(ControlCommand::ClaimMigrationExecutor {
                migration_id: token.migration_id,
                expected_generation: token.generation,
                claim_key: ProcessIncarnation::from_bits(rand::random()),
                executor: token.executor.clone(),
                now_ms: self.state.unix_time_ms(),
            })
            .await?;
            return Ok(());
        };
        let receipt: CompletedMembershipMutation = self.call(view, leader, process, "membership", serde_json::json!({
            "token": token, "raft_group_id": migration.raft_group_id, "request_id": request_id,
            "operation": ReceiverMutationKind::ManagedMembership { step },
        })).await?;
        if receipt.outcome != MembershipOutcome::Applied
            || receipt.process.node_id != leader
            || receipt.process.incarnation != *process
            || receipt.request.token != *token
        {
            return Err("membership reply lacks current action/process evidence".to_owned());
        }
        Ok(())
    }

    /// One durable state transition, or one idempotent native action. Returning
    /// an error leaves the operation active; the next iteration reconciles it.
    pub(crate) async fn step(&self) -> Result<(), String> {
        // A follower cannot claim work, even if it has a cached projection.
        let view = self
            .receiver
            .meta
            .read_projection(Duration::from_secs(2))
            .await
            .map_err(|error| error.to_string())?;
        let Some(migration) = view.state.active_migration() else {
            return Ok(());
        };
        let managed = migration
            .managed
            .as_ref()
            .ok_or("managed intent is absent")?;
        let own = ReceiverProcess {
            node_id: self.receiver.store.identity().node.node_id,
            incarnation: self.state.process_incarnation.clone(),
        };
        let peers: BTreeSet<_> = migration
            .from_voters
            .union(&migration.target_voters)
            .copied()
            .collect();
        let assignment = managed.executor.as_ref();
        if assignment.is_none_or(|assignment| assignment.token.executor != own) {
            self.write(ControlCommand::ClaimMigrationExecutor {
                migration_id: migration.migration_id,
                expected_generation: assignment.map_or(0, |assignment| assignment.token.generation),
                claim_key: ProcessIncarnation::from_bits(rand::random()),
                executor: own,
                now_ms: self.state.unix_time_ms(),
            })
            .await?;
            return Ok(());
        }
        let processes = self.processes(&view, &peers).await?;
        if !managed.receivers.is_empty() && managed.receivers != processes {
            self.write(ControlCommand::ClaimMigrationExecutor {
                migration_id: migration.migration_id,
                expected_generation: assignment.map_or(0, |assignment| assignment.token.generation),
                claim_key: ProcessIncarnation::from_bits(rand::random()),
                executor: own,
                now_ms: self.state.unix_time_ms(),
            })
            .await?;
            return Ok(());
        }
        let token = &assignment.ok_or("executor is absent")?.token;
        let revision = managed.revision;
        if managed.receivers_retired {
            return self.update(token, revision, MigrationUpdate::Finish).await;
        }
        if !managed.receiver_activation_authorized {
            return self
                .update(token, revision, MigrationUpdate::AuthorizeReceivers)
                .await;
        }
        if managed.receivers.is_empty() {
            for (id, process) in &processes {
                let ledger: ReceiverLedger = self
                    .call(
                        &view,
                        *id,
                        process,
                        "activate",
                        serde_json::json!({"token": token}),
                    )
                    .await?;
                if ledger.pending.is_some()
                    || !ledger.fence.is_some_and(|fence| {
                        fence.token == *token
                            && fence.process == *process
                            && fence.phase == ReceiverFencePhase::Active
                    })
                {
                    return Err(
                        "activation has not drained and certified its current process".to_owned(),
                    );
                }
            }
            return self
                .update(token, revision, MigrationUpdate::CertifyReceivers {
                    processes,
                })
                .await;
        }
        // A lost reply can leave native work pending on a former data leader.
        // Reconcile every participant before issuing an unrelated mutation;
        // activation orders behind that process's detached native task.
        for (id, process) in &processes {
            let url = self.admin_url(&view, *id)?;
            let ledger: ReceiverLedger = self
                .reply(self.client.get(format!("{url}/__ursula/control/receiver")))
                .await?;
            if ledger.pending.is_some() {
                let _: ReceiverLedger = self
                    .call(
                        &view,
                        *id,
                        process,
                        "activate",
                        serde_json::json!({"token": token}),
                    )
                    .await?;
                return Ok(());
            }
        }
        if managed.published_epoch.is_none()
            && let Some(id) = migration.added_nodes.difference(&managed.prepared).next()
        {
            let process = processes.get(id).ok_or("added process is absent")?;
            let receipt: CompletedReceiverMutation = self.call(&view, *id, process, "prepare", serde_json::json!({
                    "token": token, "raft_group_id": migration.raft_group_id, "request_id": "prepare",
                    "operation": ReceiverMutationKind::PrepareReplica { epoch: managed.request.expected_epoch },
                })).await?;
            let ReplicaMutationResult::Prepared { process: proof } = receipt.result else {
                return Err("expected prepare receipt".to_owned());
            };
            if proof.node_id != *id
                || proof.incarnation != *process
                || receipt.request.token != *token
            {
                return Err("prepare proof has another process or intent".to_owned());
            }
            return self
                .update(token, revision, MigrationUpdate::RecordPrepared {
                    process: proof,
                })
                .await;
        }
        let configuration = self
            .configuration(&view, migration.raft_group_id, &peers)
            .await?;
        let source = vec![migration.from_voters.clone()];
        let target = vec![migration.target_voters.clone()];
        let joint = vec![
            migration.from_voters.clone(),
            migration.target_voters.clone(),
        ];
        if ![&source, &target, &joint].contains(&&configuration.voter_sets)
            || !configuration.learners.is_subset(&migration.added_nodes)
            || !covers(
                &configuration.membership_log_id,
                &managed.request.source_membership.log_id,
            )
        {
            return Err("actual data membership is outside the immutable intent".to_owned());
        }
        if managed.catchup_prefix.is_none() {
            return self
                .update(token, revision, MigrationUpdate::CapturePrefix {
                    prefix: configuration.applied_log_id,
                })
                .await;
        }
        let prefix = managed
            .catchup_prefix
            .as_ref()
            .ok_or("fixed prefix is absent")?;
        if managed.published_epoch.is_none() {
            for id in &migration.added_nodes {
                if managed.learner_applied.contains_key(id) {
                    continue;
                }
                if !configuration.nodes.contains_key(id) {
                    return self
                        .membership(
                            &view,
                            token,
                            migration,
                            configuration.leader_id,
                            MembershipStep::AddLearner {
                                epoch: managed.request.expected_epoch,
                                node_id: *id,
                                prefix: prefix.clone(),
                            },
                            &format!("learner-{id}"),
                        )
                        .await;
                }
                let process = processes.get(id).ok_or("learner process is absent")?;
                let evidence: ReplicaAppliedEvidence = self.call(&view, *id, process, "applied", serde_json::json!({
                    "token": token, "raft_group_id": migration.raft_group_id, "prefix": prefix,
                })).await?;
                return self
                    .update(token, revision, MigrationUpdate::RecordLearner { evidence })
                    .await;
            }
            if !managed.membership_may_have_changed {
                return self
                    .update(token, revision, MigrationUpdate::AuthorizeMembership)
                    .await;
            }
        }
        if configuration.voter_sets != target || !configuration.learners.is_empty() {
            if managed.published_epoch.is_some() {
                return Err("published target membership drifted".to_owned());
            }
            if !migration.target_voters.contains(&configuration.leader_id)
                && let Some(successor) = migration
                    .target_voters
                    .intersection(&migration.from_voters)
                    .next()
            {
                return self
                    .membership(
                        &view,
                        token,
                        migration,
                        configuration.leader_id,
                        MembershipStep::TransferLeader {
                            epoch: managed.request.expected_epoch,
                            node_id: *successor,
                        },
                        "handoff",
                    )
                    .await;
            }
            return self
                .membership(
                    &view,
                    token,
                    migration,
                    configuration.leader_id,
                    MembershipStep::ChangeVoters {
                        epoch: managed.request.expected_epoch,
                        target_voters: migration.target_voters.clone(),
                    },
                    "voters",
                )
                .await;
        }
        if managed.verification_generation != Some(token.generation) {
            // Hold this post-membership prefix fixed while polling all targets.
            // Cancellation/restart discards observations, never publishes them.
            let committed_prefix = configuration.applied_log_id.clone();
            let replicas = crate::http_time::timeout(IO_TIMEOUT, async {
                loop {
                    let mut replicas = BTreeMap::new();
                    for id in &migration.target_voters {
                        let process = processes.get(id).ok_or("target process is absent")?;
                        if let Ok(evidence) = self.call::<ReplicaAppliedEvidence>(&view, *id, process, "applied", serde_json::json!({
                            "token": token, "raft_group_id": migration.raft_group_id, "prefix": committed_prefix,
                        })).await { replicas.insert(*id, evidence); }
                    }
                    if replicas.len() == migration.target_voters.len() { return Ok::<_, String>(replicas); }
                    crate::http_time::sleep(Duration::from_millis(50)).await;
                }
            }).await.map_err(|_| "target applied-prefix verification is not ready".to_owned())??;
            return self
                .update(token, revision, MigrationUpdate::VerifyMembership {
                    evidence: FinalMembershipEvidence {
                        membership: configuration.uniform_membership()?,
                        committed_prefix,
                        replicas,
                    },
                })
                .await;
        }
        if managed.published_epoch.is_none() {
            return self
                .update(token, revision, MigrationUpdate::PublishPlacement)
                .await;
        }
        if let Some(id) = migration
            .removed_voters
            .difference(&managed.released)
            .next()
        {
            let process = processes.get(id).ok_or("removed process is absent")?;
            let membership_log_id = &managed
                .final_membership
                .as_ref()
                .ok_or("verified membership is absent")?
                .membership
                .log_id;
            let receipt: CompletedReceiverMutation = self.call(&view, *id, process, "release", serde_json::json!({
                "token": token, "raft_group_id": migration.raft_group_id, "request_id": "release",
                "operation": ReceiverMutationKind::ReleaseReplica { epoch: managed.published_epoch.ok_or("published epoch is absent")?, membership_log_id: membership_log_id.clone() },
            })).await?;
            let ReplicaMutationResult::Released { evidence } = receipt.result else {
                return Err("expected release receipt".to_owned());
            };
            return self
                .update(token, revision, MigrationUpdate::RecordReleased {
                    evidence,
                })
                .await;
        }
        for (id, process) in &processes {
            let ledger: ReceiverLedger = self
                .call(
                    &view,
                    *id,
                    process,
                    "retire",
                    serde_json::json!({"token": token}),
                )
                .await?;
            if !ledger.fence.is_some_and(|fence| {
                fence.token == *token
                    && fence.process == *process
                    && fence.phase == ReceiverFencePhase::Retired
            }) {
                return Err("receiver has not retired current authority".to_owned());
            }
        }
        self.update(token, revision, MigrationUpdate::RetireReceivers {
            processes,
        })
        .await
    }
}

impl Coordinator {
    async fn record_error(&self, error: &str) -> Result<bool, String> {
        let view = self
            .receiver
            .meta
            .read_projection(Duration::from_secs(2))
            .await
            .map_err(|error| error.to_string())?;
        let Some(migration) = view.state.active_migration() else {
            return Ok(false);
        };
        let managed = migration
            .managed
            .as_ref()
            .ok_or("managed intent is absent")?;
        let Some(assignment) = &managed.executor else {
            return Ok(false);
        };
        if assignment.token.executor.node_id != self.receiver.store.identity().node.node_id
            || assignment.token.executor.incarnation != self.state.process_incarnation
        {
            return Ok(false);
        }
        let reason: String = error.chars().take(1024).collect();
        if migration.last_error.as_ref() == Some(&reason) {
            return Ok(false);
        }
        self.update(
            &assignment.token,
            managed.revision,
            MigrationUpdate::RecordError { reason },
        )
        .await?;
        Ok(true)
    }
}

/// The task owns no unique progress: aborting it leaves only durable intent and
/// receiver checkpoints, which the next quorum-confirmed meta leader resumes.
pub(crate) async fn run(state: HttpState, interval: Duration) {
    let control = match coordinator(state) {
        Ok(control) => control,
        Err(error) => {
            tracing::error!(%error, "managed executor unavailable");
            return;
        }
    };
    let mut delay = interval;
    loop {
        match control.step().await {
            Ok(()) => delay = interval,
            Err(error) => {
                if control.record_error(&error).await == Ok(true) {
                    tracing::warn!(%error, "managed operation remains active for reconciliation");
                }
                delay = delay
                    .saturating_mul(2)
                    .min(Duration::from_secs(2))
                    .max(interval);
            }
        }
        crate::http_time::sleep(delay).await;
    }
}
