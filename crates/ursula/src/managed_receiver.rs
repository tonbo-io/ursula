//! Process-bound receiver admission. Lifecycle calls are detached from HTTP
//! cancellation, ordered with admin work, durable before queue reconciliation,
//! and authorized only through fresh independent meta quorum reads.

use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use axum::routing::post;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::OwnedRwLockReadGuard;
use tokio::sync::RwLock;
use ursula_control::ClusterBootstrap;
use ursula_control::ControlProjection;
use ursula_control::MigrationPhase;
use ursula_control::MigrationToken;
use ursula_control::NodeState;
use ursula_control::ReceiverFencePhase;
use ursula_control::ReceiverFenceRecord;
use ursula_control::ReceiverLedger;
use ursula_control::ReplicaAssignmentPhase;
use ursula_proto::admin::PROCESS_INCARNATION_HEADER;
use ursula_raft::ManagedReceiverStore;
use ursula_raft::MetaRaftHandle;

use crate::HttpState;

// V3 requires activation to drain snapshot pruning before certification.
pub(crate) const RECEIVER_PROTOCOL_VERSION: u32 = 3;

mod membership;
mod replica;
use membership::applied_evidence;
use membership::membership_mutation;
use replica::replica_prepare;
use replica::replica_release;

pub(crate) struct ManagedReceiver {
    pub(crate) store: Arc<ManagedReceiverStore>,
    pub(crate) recipe: ClusterBootstrap,
    pub(crate) meta: MetaRaftHandle,
    gate: Arc<RwLock<()>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ReceiverRequest {
    pub token: MigrationToken,
}

impl ManagedReceiver {
    pub(crate) fn new(
        store: Arc<ManagedReceiverStore>,
        recipe: ClusterBootstrap,
        meta: MetaRaftHandle,
    ) -> Self {
        Self {
            store,
            recipe,
            meta,
            gate: Arc::new(RwLock::new(())),
        }
    }

    pub(crate) async fn admit_unmanaged(&self) -> Result<OwnedRwLockReadGuard<()>, String> {
        let guard = self.gate.clone().read_owned().await;
        let ledger = self.store.snapshot().map_err(|e| e.to_string())?;
        if ledger.pending.is_some()
            || ledger
                .fence
                .as_ref()
                .is_some_and(|f| f.phase != ReceiverFencePhase::Retired)
        {
            return Err("managed receiver authority excludes unrelated administration".to_owned());
        }
        Ok(guard)
    }

    async fn fresh(
        &self,
        state: &HttpState,
        token: &MigrationToken,
        retiring: bool,
    ) -> Result<ControlProjection, String> {
        let mut last = "no fresh meta quorum".to_owned();
        for id in &self.recipe.initial_meta_voters {
            let Some(node) = self.recipe.nodes.get(id) else {
                continue;
            };
            let view = match ursula_raft::read_control_projection(
                &self.recipe.identity,
                *id,
                &node.cluster_url,
                Duration::from_secs(1),
            )
            .await
            {
                Ok(view) => view,
                Err(error) => {
                    last = error.to_string();
                    continue;
                }
            };
            let migration = view
                .state
                .active_migration()
                .ok_or("no active managed migration")?;
            let managed = migration
                .managed
                .as_ref()
                .ok_or("migration lacks receiver authority")?;
            let own = self.store.identity().node.node_id;
            if migration.migration_id != token.migration_id
                || managed.executor.as_ref().map(|e| &e.token) != Some(token)
                || !managed.receiver_activation_authorized
                || managed.receivers_retired
                || !migration
                    .from_voters
                    .union(&migration.target_voters)
                    .any(|id| *id == own)
                || !view
                    .state
                    .nodes
                    .get(&own)
                    .is_some_and(|n| matches!(n.state, NodeState::Active | NodeState::Draining))
            {
                return Err("receiver token or participant differs from fresh intent".to_owned());
            }
            if retiring
                && (migration.phase != MigrationPhase::Finalizing
                    || managed.published_epoch.is_none()
                    || managed.released != migration.removed_voters
                    || managed.receivers.get(&own) != Some(&state.process_incarnation))
            {
                return Err(
                    "receiver retirement requires published cleanup and current process evidence"
                        .to_owned(),
                );
            }
            if retiring && migration.removed_voters.contains(&own) {
                let ledger = self.store.snapshot().map_err(|e| e.to_string())?;
                if !ledger
                    .assignments
                    .get(&migration.raft_group_id)
                    .is_some_and(|assignment| {
                        assignment.phase == ReplicaAssignmentPhase::Retired
                            && Some(assignment.epoch) == managed.published_epoch
                            && assignment.migration_id == token.migration_id
                            && assignment.generation == token.generation
                    })
                {
                    return Err(
                        "removed receiver requires its own durable replica tombstone".to_owned(),
                    );
                }
            }
            self.meta
                .persist_projection(view.clone())
                .await
                .map_err(|e| e.to_string())?;
            if let Some(cursor) = &state.managed_projection {
                cursor
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .install(view.clone())?;
            }
            return Ok(view);
        }
        Err(last)
    }

    async fn barrier(&self, state: &HttpState) -> Result<(), String> {
        if state.admin_fence.is_uncertain() {
            return Err("unresolved admin work requires process replacement".to_owned());
        }
        if self
            .store
            .snapshot()
            .map_err(|e| e.to_string())?
            .pending
            .is_some()
        {
            return Err(
                "pending managed submission requires actual membership reconciliation".to_owned(),
            );
        }
        crate::confirm_admin_command_submission(state).await
    }

    async fn activate(
        &self,
        state: &HttpState,
        token: MigrationToken,
    ) -> Result<ReceiverLedger, String> {
        let _guard = self.gate.write().await;
        let view = self.fresh(state, &token, false).await?;
        let mut ledger = self.store.snapshot().map_err(|e| e.to_string())?;
        if ledger.fence.as_ref().is_some_and(|old| {
            old.token == token
                && old.process == state.process_incarnation
                && old.phase == ReceiverFencePhase::Active
        }) {
            self.pause_snapshot_pruning(state, &view).await?;
            self.reconcile_replica(state, &token).await?;
            self.barrier(state).await?;
            return self.store.snapshot().map_err(|e| e.to_string());
        }
        if ledger.high_water_generation < token.generation {
            ledger.membership_completed.clear();
        }
        ledger.high_water_generation = token.generation;
        ledger.fence = Some(ReceiverFenceRecord {
            token: token.clone(),
            process: state.process_incarnation.clone(),
            phase: ReceiverFencePhase::Activating,
        });
        self.store
            .persist(ledger.clone())
            .await
            .map_err(|e| e.to_string())?;
        self.pause_snapshot_pruning(state, &view).await?;
        self.reconcile_replica(state, &token).await?;
        self.barrier(state).await?;
        // A generation can be replaced while this process waits for its queue.
        self.fresh(state, &token, false).await?;
        ledger = self.store.snapshot().map_err(|e| e.to_string())?;
        ledger.fence.as_mut().ok_or("missing receiver fence")?.phase = ReceiverFencePhase::Active;
        ledger = self
            .store
            .persist(ledger.clone())
            .await
            .map_err(|e| e.to_string())?;
        Ok(ledger)
    }

    async fn retire(
        &self,
        state: &HttpState,
        token: MigrationToken,
    ) -> Result<ReceiverLedger, String> {
        let _guard = self.gate.write().await;
        let view = self.fresh(state, &token, true).await?;
        let mut ledger = self.store.snapshot().map_err(|e| e.to_string())?;
        let migration = view.state.active_migration().ok_or("no migration")?;
        let own = self.store.identity().node.node_id;
        if migration.target_voters.contains(&own) {
            let epoch = migration
                .managed
                .as_ref()
                .and_then(|managed| managed.published_epoch)
                .ok_or("no published epoch")?;
            let assignment = ledger
                .assignments
                .get_mut(&migration.raft_group_id)
                .ok_or("target replica has no local assignment")?;
            if assignment.phase != ReplicaAssignmentPhase::Hosted {
                return Err("target replica is not hosted".to_owned());
            }
            assignment.epoch = epoch;
            assignment.migration_id = token.migration_id;
            assignment.generation = token.generation;
        }
        let current = ledger
            .fence
            .as_mut()
            .ok_or("receiver has no active fence")?;
        if current.token != token || current.process != state.process_incarnation {
            return Err("receiver token or process changed".to_owned());
        }
        if current.phase == ReceiverFencePhase::Retired {
            self.resume_snapshot_pruning(state, &view).await?;
            return Ok(ledger);
        }
        current.phase = ReceiverFencePhase::Retiring;
        ledger = self
            .store
            .persist(ledger.clone())
            .await
            .map_err(|e| e.to_string())?;
        self.barrier(state).await?;
        self.fresh(state, &token, true).await?;
        ledger.fence.as_mut().ok_or("missing receiver fence")?.phase = ReceiverFencePhase::Retired;
        ledger = self
            .store
            .persist(ledger.clone())
            .await
            .map_err(|e| e.to_string())?;
        self.resume_snapshot_pruning(state, &view).await?;
        Ok(ledger)
    }

    async fn pause_snapshot_pruning(
        &self,
        state: &HttpState,
        view: &ControlProjection,
    ) -> Result<(), String> {
        let migration = view.state.active_migration().ok_or("no active migration")?;
        if let Some(registry) = state.raft_registry() {
            registry
                .snapshot_store()
                .configure_pruning(
                    migration.raft_group_id.0,
                    migration
                        .from_voters
                        .union(&migration.target_voters)
                        .copied()
                        .collect(),
                    false,
                )
                .await
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }

    async fn resume_snapshot_pruning(
        &self,
        state: &HttpState,
        view: &ControlProjection,
    ) -> Result<(), String> {
        let migration = view.state.active_migration().ok_or("no active migration")?;
        if let Some(registry) = state.raft_registry() {
            registry
                .snapshot_store()
                .configure_pruning(
                    migration.raft_group_id.0,
                    migration.target_voters.clone(),
                    true,
                )
                .await
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }

    /// Only a fresh meta read may resume pruning after restart. Local cached
    /// placement is enough for serving recovery, but not for object deletion.
    pub(crate) async fn sync_snapshot_pruning(
        &self,
        state: &HttpState,
        view: &ControlProjection,
    ) -> Result<(), String> {
        let _guard = self.gate.read().await;
        view.validate()?;
        if let Some(cursor) = &state.managed_projection {
            let cursor = cursor
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if cursor
                .current()
                .is_some_and(|current| current.applied_log_id.index > view.applied_log_id.index)
            {
                return Ok(());
            }
        }
        let Some(registry) = state.raft_registry() else {
            return Ok(());
        };
        let store = registry.snapshot_store();
        let ledger = self.store.snapshot().map_err(|error| error.to_string())?;
        let fence = ledger
            .fence
            .as_ref()
            .filter(|fence| fence.phase != ReceiverFencePhase::Retired);
        let fenced_group = fence
            .and_then(|fence| view.state.migrations.get(&fence.token.migration_id))
            .map(|migration| migration.raft_group_id);
        let active_group = view
            .state
            .active_migration()
            .map(|migration| migration.raft_group_id);
        let own = self.store.identity().node.node_id;
        let pause_all =
            (fence.is_some() && fenced_group.is_none())
                || !view.state.nodes.get(&own).is_some_and(|node| {
                    matches!(node.state, NodeState::Active | NodeState::Draining)
                });
        for (group, placement) in &view.state.placements {
            store
                .configure_pruning(
                    group.0,
                    placement.voters.clone(),
                    !pause_all && active_group != Some(*group) && fenced_group != Some(*group),
                )
                .await
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }

    /// Inventory is derived from placement plus the durable assignment ledger,
    /// never from handles observed in metrics. Transitional roles do not count
    /// as serving voters or grant disruption permission.
    pub(crate) fn maintenance_report(
        &self,
        view: &ControlProjection,
        node_id: u64,
        groups: &[ursula_raft::RaftGroupMetricsSnapshot],
        admin_uncertain: bool,
    ) -> ursula_raft::RaftMaintenanceReport {
        use ursula_raft::ManagedRaftInventory;
        use ursula_raft::ManagedReplicaRole;

        let active = view.state.active_migration().filter(|migration| {
            migration.from_voters.contains(&node_id) || migration.target_voters.contains(&node_id)
        });
        let mut expected: std::collections::BTreeMap<_, _> = view
            .state
            .placements
            .iter()
            .filter(|(_, placement)| placement.voters.contains(&node_id))
            .map(|(group, placement)| (group.0, placement.voters.clone()))
            .collect();
        let mut inventory = ManagedRaftInventory {
            applied_meta_index: view.applied_log_id.index,
            active_migration_id: active.map(|migration| migration.migration_id),
            replica_roles: expected
                .keys()
                .map(|group| (*group, ManagedReplicaRole::Voter))
                .collect(),
            receiver_fenced: admin_uncertain,
            receiver_pending: false,
            serving_ready: false,
            assignment_drift: node_id != self.store.identity().node.node_id
                || !view.state.nodes.get(&node_id).is_some_and(|node| {
                    matches!(node.state, NodeState::Active | NodeState::Draining)
                }),
        };
        match self.store.snapshot() {
            Err(_) => inventory.assignment_drift = true,
            Ok(ledger) => {
                inventory.receiver_fenced |= ledger
                    .fence
                    .as_ref()
                    .is_some_and(|fence| fence.phase != ReceiverFencePhase::Retired);
                inventory.receiver_pending = ledger.pending.is_some();
                inventory.assignment_drift |= !ledger.assignments_seeded;
                for (group, placement) in &view.state.placements {
                    if placement.voters.contains(&node_id)
                        && !ledger.assignments.get(group).is_some_and(|assignment| {
                            assignment.phase == ReplicaAssignmentPhase::Hosted
                                && (assignment.epoch == placement.epoch
                                    || active
                                        .filter(|migration| migration.raft_group_id == *group)
                                        .and_then(|migration| migration.managed.as_ref())
                                        .is_some_and(|managed| {
                                            assignment.epoch == managed.request.expected_epoch
                                        }))
                        })
                    {
                        inventory.assignment_drift = true;
                    }
                }
                for (group, assignment) in &ledger.assignments {
                    if assignment.phase == ReplicaAssignmentPhase::Retired {
                        continue;
                    }
                    let Some(placement) = view.state.placements.get(group) else {
                        inventory.assignment_drift = true;
                        continue;
                    };
                    let intent = active.filter(|migration| migration.raft_group_id == *group);
                    let role = match assignment.phase {
                        ReplicaAssignmentPhase::Hosted if placement.voters.contains(&node_id) => {
                            ManagedReplicaRole::Voter
                        }
                        ReplicaAssignmentPhase::Preparing | ReplicaAssignmentPhase::Hosted
                            if intent.is_some_and(|migration| {
                                migration.added_nodes.contains(&node_id)
                                    && assignment.migration_id == migration.migration_id
                            }) =>
                        {
                            if assignment.phase == ReplicaAssignmentPhase::Preparing {
                                ManagedReplicaRole::PreparingLearner
                            } else {
                                ManagedReplicaRole::Learner
                            }
                        }
                        ReplicaAssignmentPhase::Retiring | ReplicaAssignmentPhase::Hosted
                            if intent.is_some_and(|migration| {
                                migration.removed_voters.contains(&node_id)
                            }) =>
                        {
                            ManagedReplicaRole::Retiring
                        }
                        _ => {
                            inventory.assignment_drift = true;
                            ManagedReplicaRole::Retiring
                        }
                    };
                    expected.insert(group.0, placement.voters.clone());
                    inventory.replica_roles.insert(group.0, role);
                }
            }
        }
        inventory.serving_ready = managed_serving_ready(view, node_id, groups, &inventory);
        ursula_raft::check_managed_raft_inventory(groups, node_id, expected, 16, inventory)
    }
}

fn managed_serving_ready(
    view: &ControlProjection,
    node_id: u64,
    groups: &[ursula_raft::RaftGroupMetricsSnapshot],
    inventory: &ursula_raft::ManagedRaftInventory,
) -> bool {
    use ursula_raft::ManagedReplicaRole;
    if inventory.assignment_drift {
        return false;
    }
    let resident: std::collections::BTreeMap<_, _> = groups
        .iter()
        .map(|group| (group.raft_group_id, group))
        .collect();
    if resident.len() != groups.len()
        || groups.iter().any(|group| {
            group.node_id != node_id || !inventory.replica_roles.contains_key(&group.raft_group_id)
        })
    {
        return false;
    }
    for (id, role) in &inventory.replica_roles {
        let Some(group) = resident.get(id) else {
            if *role == ManagedReplicaRole::Voter {
                return false;
            }
            continue;
        };
        if *role == ManagedReplicaRole::Retiring {
            continue;
        }
        let voters: std::collections::BTreeSet<_> = group.voter_ids.iter().copied().collect();
        let learners: std::collections::BTreeSet<_> = group.learner_ids.iter().copied().collect();
        if voters.len() != group.voter_ids.len() || learners.len() != group.learner_ids.len() {
            return false;
        }
        let intent = view
            .state
            .active_migration()
            .filter(|migration| migration.raft_group_id.0 == *id);
        // A prepared destination may have no membership before AddLearner.
        // Neither missing learner progress nor its existence makes it a voter.
        if !voters.contains(&node_id)
            && matches!(
                role,
                ManagedReplicaRole::PreparingLearner | ManagedReplicaRole::Learner
            )
        {
            continue;
        }
        let allowed = if let Some(migration) = intent {
            let Some(managed) = &migration.managed else {
                return false;
            };
            let shape = if group.maintenance.membership_joint {
                managed.membership_may_have_changed
                    && voters
                        == migration
                            .from_voters
                            .union(&migration.target_voters)
                            .copied()
                            .collect()
            } else {
                voters == migration.from_voters
                    || (managed.membership_may_have_changed && voters == migration.target_voters)
            };
            shape
                && learners.is_subset(&migration.added_nodes)
                && learners.is_subset(&managed.prepared)
        } else {
            view.state
                .placements
                .get(&ursula_shard::RaftGroupId(*id))
                .is_some_and(|placement| voters == placement.voters)
                && learners.is_empty()
                && !group.maintenance.membership_joint
        };
        if !allowed {
            return false;
        }
        if !voters.contains(&node_id) {
            // A removed source stops counting before placement publication;
            // it still routes requests through the actual current data leader.
            if intent.is_some_and(|migration| migration.removed_voters.contains(&node_id)) {
                continue;
            }
            return false;
        }
        if *role == ManagedReplicaRole::PreparingLearner
            || !group.maintenance.running
            || !group.maintenance.recovery_ready
            || group.maintenance.stopped_for_operator
            || !group
                .current_leader
                .is_some_and(|leader| voters.contains(&leader))
        {
            return false;
        }
        let (Some(committed), Some(applied), Some(membership)) = (
            group.committed,
            group.last_applied,
            group.maintenance.membership_log_index,
        ) else {
            return false;
        };
        if applied.index < membership || committed.index.saturating_sub(applied.index) > 16 {
            return false;
        }
    }
    true
}

pub(crate) fn router(state: HttpState) -> Router {
    Router::new()
        .route("/__ursula/control/receiver", get(status))
        .route("/__ursula/control/receiver/process", get(process))
        .route("/__ursula/control/receiver/activate", post(activate))
        .route("/__ursula/control/receiver/retire", post(retire))
        .route("/__ursula/control/receiver/prepare", post(replica_prepare))
        .route("/__ursula/control/receiver/release", post(replica_release))
        .route(
            "/__ursula/control/receiver/membership",
            post(membership_mutation),
        )
        .route("/__ursula/control/receiver/applied", post(applied_evidence))
        .layer(axum::extract::DefaultBodyLimit::max(16 * 1024))
        .with_state(state)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ReceiverInventory {
    pub protocol_version: u32,
    pub identity: ursula_control::MetaLocalIdentity,
    pub process: ursula_proto::admin::ProcessIncarnation,
}

async fn process(State(state): State<HttpState>) -> Response {
    match &state.managed_receiver {
        Some(receiver) => Json(ReceiverInventory {
            protocol_version: RECEIVER_PROTOCOL_VERSION,
            identity: receiver.store.identity().clone(),
            process: state.process_incarnation,
        })
        .into_response(),
        None => (StatusCode::CONFLICT, "managed receiver is unavailable").into_response(),
    }
}

#[cfg(all(test, not(madsim)))]
#[path = "managed_receiver_tests.rs"]
mod tests;

async fn status(State(state): State<HttpState>) -> Response {
    match state
        .managed_receiver
        .as_ref()
        .ok_or("managed receiver is unavailable")
        .and_then(|r| r.store.snapshot().map_err(|_| "receiver storage failed"))
    {
        Ok(ledger) => Json(ledger).into_response(),
        Err(reason) => (StatusCode::SERVICE_UNAVAILABLE, reason).into_response(),
    }
}

async fn activate(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(request): Json<ReceiverRequest>,
) -> Response {
    lifecycle(state, headers, request.token, false).await
}
async fn retire(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(request): Json<ReceiverRequest>,
) -> Response {
    lifecycle(state, headers, request.token, true).await
}

async fn lifecycle(
    state: HttpState,
    headers: HeaderMap,
    token: MigrationToken,
    retiring: bool,
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
    let Some(receiver) = state.managed_receiver.clone() else {
        return (StatusCode::CONFLICT, "managed receiver is unavailable").into_response();
    };
    match tokio::spawn(async move {
        if retiring {
            receiver.retire(&state, token).await
        } else {
            receiver.activate(&state, token).await
        }
    })
    .await
    {
        Ok(Ok(ledger)) => Json(ledger).into_response(),
        Ok(Err(reason)) => (StatusCode::CONFLICT, reason).into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("receiver lifecycle task failed: {error}"),
        )
            .into_response(),
    }
}
