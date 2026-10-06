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

pub(crate) struct ManagedReceiver {
    pub(crate) store: Arc<ManagedReceiverStore>,
    recipe: ClusterBootstrap,
    meta: MetaRaftHandle,
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
        self.fresh(state, &token, false).await?;
        let mut ledger = self.store.snapshot().map_err(|e| e.to_string())?;
        if ledger.fence.as_ref().is_some_and(|old| {
            old.token == token
                && old.process == state.process_incarnation
                && old.phase == ReceiverFencePhase::Active
        }) {
            self.barrier(state).await?;
            return Ok(ledger);
        }
        ledger.high_water_generation = token.generation;
        ledger.fence = Some(ReceiverFenceRecord {
            token: token.clone(),
            process: state.process_incarnation.clone(),
            phase: ReceiverFencePhase::Activating,
        });
        ledger = self
            .store
            .persist(ledger.clone())
            .await
            .map_err(|e| e.to_string())?;
        self.barrier(state).await?;
        // A generation can be replaced while this process waits for its queue.
        self.fresh(state, &token, false).await?;
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
        self.fresh(state, &token, true).await?;
        let mut ledger = self.store.snapshot().map_err(|e| e.to_string())?;
        let current = ledger
            .fence
            .as_mut()
            .ok_or("receiver has no active fence")?;
        if current.token != token || current.process != state.process_incarnation {
            return Err("receiver token or process changed".to_owned());
        }
        if current.phase == ReceiverFencePhase::Retired {
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
        Ok(ledger)
    }
}

pub(crate) fn router(state: HttpState) -> Router {
    Router::new()
        .route("/__ursula/control/receiver", get(status))
        .route("/__ursula/control/receiver/activate", post(activate))
        .route("/__ursula/control/receiver/retire", post(retire))
        .layer(axum::extract::DefaultBodyLimit::max(16 * 1024))
        .with_state(state)
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
