//! `GET /__ursula/placement` (design 11.1): this node's placement status
//! (`ursula_control::PlacementStatus`), on the client and the admin
//! listener. `GET /__ursula/placement/groups/{group}` is one group's
//! placement with this node's replica of it
//! (`ursula_control::PlacementGroupStatus`), on both listeners too.
//! `?confirmed=true`, a meta read barrier, is served on the admin listener
//! only.
//!
//! The admin listener's writes go to the meta leader:
//! `POST /__ursula/placement/groups/{group}/move` ([`request_move`]) starts,
//! retargets or cancels a move, `POST /__ursula/placement/move-limit`
//! ([`set_move_limit`]) sets the move limit,
//! `POST /__ursula/placement/nodes/{node}/replace-disk` ([`replace_disk`])
//! releases the root a node id is bound to, the `ursulactl node
//! replace-disk` verb, and `POST /__ursula/placement/nodes/{node}/cordon`,
//! `.../uncordon` and `.../decommission` ([`cordon`], [`uncordon`],
//! [`decommission`]) change a node's lifecycle, the `ursulactl node`
//! lifecycle verbs.
//!
//! Every refusal of a placement route answers through
//! [`placement_api_error_response`], one status per reason.

use axum::Json;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use ursula_control::ControlApplied;
use ursula_control::ControlError;
use ursula_control::LifecycleChange;
use ursula_control::LifecycleResponse;
use ursula_control::MoveLimitRequest;
use ursula_control::MoveLimitResponse;
use ursula_control::MoveRequest;
use ursula_control::MoveResponse;
use ursula_control::PlacementApiError;
use ursula_control::PlacementApiReason;
use ursula_control::ReplaceDiskRequest;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::WriteEffect;
use ursula_shard::RaftGroupId;

use crate::HttpState;

pub(crate) const PLACEMENT_PATH: &str = "/__ursula/placement";
pub(crate) const GROUP_ROUTE: &str = "/__ursula/placement/groups/{group}";
pub(crate) const MOVE_ROUTE: &str = "/__ursula/placement/groups/{group}/move";
pub(crate) const MOVE_LIMIT_ROUTE: &str = "/__ursula/placement/move-limit";
pub(crate) const REPLACE_DISK_ROUTE: &str = "/__ursula/placement/nodes/{node}/replace-disk";
pub(crate) const CORDON_ROUTE: &str = "/__ursula/placement/nodes/{node}/cordon";
pub(crate) const UNCORDON_ROUTE: &str = "/__ursula/placement/nodes/{node}/uncordon";
pub(crate) const DECOMMISSION_ROUTE: &str = "/__ursula/placement/nodes/{node}/decommission";

/// The query of the placement reads.
#[derive(Debug, Default, serde::Deserialize)]
pub(crate) struct PlacementQuery {
    /// Confirm the view by a meta read barrier first (admin listener only).
    #[serde(default)]
    confirmed: bool,
}

/// The client listener's answer to `?confirmed=true`: a barrier is the
/// admin listener's.
fn confirmed_on_client() -> Response {
    (
        StatusCode::BAD_REQUEST,
        "`confirmed=true` is served on the admin listener only",
    )
        .into_response()
}

/// The client listener's placement status: the current view, never a
/// confirmed one, since a barrier is the admin listener's.
pub(crate) async fn placement_status(
    State(state): State<HttpState>,
    Query(query): Query<PlacementQuery>,
) -> Response {
    if query.confirmed {
        return confirmed_on_client();
    }
    match state.meta().status_reader() {
        Some(reader) => Json(reader.placement_status()).into_response(),
        None => placement_api_error_response(no_meta_runtime()),
    }
}

/// The admin listener's placement status, confirmed by a meta read
/// barrier when the query asks.
pub(crate) async fn admin_placement_status(
    State(state): State<HttpState>,
    Query(query): Query<PlacementQuery>,
) -> Response {
    let Some(reader) = state.meta().status_reader() else {
        return placement_api_error_response(no_meta_runtime());
    };
    if !query.confirmed {
        return Json(reader.placement_status()).into_response();
    }
    match reader.confirmed_placement_status().await {
        Ok(status) => Json(status).into_response(),
        Err(error) => placement_api_error_response(error.into()),
    }
}

/// The client listener's status of one group, from the current view.
pub(crate) async fn group_status(
    state: State<HttpState>,
    group: Path<u32>,
    Query(query): Query<PlacementQuery>,
) -> Response {
    if query.confirmed {
        return confirmed_on_client();
    }
    admin_group_status(state, group, Query(query)).await
}

/// The admin listener's status of one group, from a view a meta read
/// barrier confirmed when the query asks. A group the view does not place
/// answers `404` (`unknown_group`), a view before Bootstrap `503`.
pub(crate) async fn admin_group_status(
    State(state): State<HttpState>,
    Path(group): Path<u32>,
    Query(query): Query<PlacementQuery>,
) -> Response {
    let Some(reader) = state.meta().status_reader() else {
        return placement_api_error_response(no_meta_runtime());
    };
    match reader
        .group_status(RaftGroupId(group), query.confirmed)
        .await
    {
        Ok(status) => Json(status).into_response(),
        Err(error) => placement_api_error_response(error.into()),
    }
}

/// Starts, retargets or cancels a move of `group` through the meta leader,
/// and answers the group's new placement key. A placement no longer at
/// `expected` answers `409` (`stale_placement`) with the placement as
/// `current`.
pub(crate) async fn request_move(
    State(state): State<HttpState>,
    Path(group): Path<u32>,
    Json(request): Json<MoveRequest>,
) -> Response {
    let Some(reader) = state.meta().status_reader() else {
        return placement_api_error_response(no_meta_runtime());
    };
    match reader
        .request_move(RaftGroupId(group), request.expected, request.to)
        .await
    {
        Ok(ControlApplied::MoveAccepted { since }) => Json(MoveResponse { since }).into_response(),
        Ok(other) => other_command_response("move", &other),
        Err(error) => placement_api_error_response(error.into()),
    }
}

/// Sets the move limit through the meta leader, and answers the limit
/// applied.
pub(crate) async fn set_move_limit(
    State(state): State<HttpState>,
    Json(request): Json<MoveLimitRequest>,
) -> Response {
    let Some(reader) = state.meta().status_reader() else {
        return placement_api_error_response(no_meta_runtime());
    };
    match reader.set_move_limit(request.limit).await {
        Ok(ControlApplied::MoveLimitSet { limit }) => {
            Json(MoveLimitResponse { limit }).into_response()
        }
        Ok(other) => other_command_response("move-limit", &other),
        Err(error) => placement_api_error_response(error.into()),
    }
}

/// Releases `expect_root`, the root node `node` is bound to, through the
/// meta leader, and answers the node's record with its released root. A
/// slot on another root answers `409` (`stale_root`), an unknown or
/// removed node `404` or `409`.
pub(crate) async fn replace_disk(
    State(state): State<HttpState>,
    Path(node): Path<u64>,
    Json(request): Json<ReplaceDiskRequest>,
) -> Response {
    let Some(reader) = state.meta().status_reader() else {
        return placement_api_error_response(no_meta_runtime());
    };
    match reader
        .release_root(NodeId::new(node), request.expect_root)
        .await
    {
        Ok(ControlApplied::RootReleased(record)) => Json(record).into_response(),
        Ok(other) => other_command_response("replace-disk", &other),
        Err(error) => placement_api_error_response(error.into()),
    }
}

/// Cordons `node`: it keeps its replicas and takes no new one.
pub(crate) async fn cordon(state: State<HttpState>, node: Path<u64>) -> Response {
    set_lifecycle(state, node, LifecycleChange::Cordon).await
}

/// Uncordons `node`, which aborts its decommission while it has a role.
pub(crate) async fn uncordon(state: State<HttpState>, node: Path<u64>) -> Response {
    set_lifecycle(state, node, LifecycleChange::Uncordon).await
}

/// Decommissions `node`: the meta leader moves its groups away, and the
/// node is removed for good once it has no role. A meta voter answers
/// `409` (`meta_voter`).
pub(crate) async fn decommission(state: State<HttpState>, node: Path<u64>) -> Response {
    set_lifecycle(state, node, LifecycleChange::Decommission).await
}

/// Changes `node`'s lifecycle through the meta leader and answers the
/// lifecycle after the change. An unknown node answers `404`, a removed
/// one `409` (`node_removed`).
async fn set_lifecycle(
    State(state): State<HttpState>,
    Path(node): Path<u64>,
    change: LifecycleChange,
) -> Response {
    let Some(reader) = state.meta().status_reader() else {
        return placement_api_error_response(no_meta_runtime());
    };
    match reader.set_lifecycle(NodeId::new(node), change).await {
        Ok(ControlApplied::LifecycleChanged { lifecycle }) => {
            Json(LifecycleResponse { lifecycle }).into_response()
        }
        Ok(other) => other_command_response("lifecycle", &other),
        Err(error) => placement_api_error_response(error.into()),
    }
}

/// The answer when the meta group answered a placement write as another
/// command: a bug, never a refusal.
fn other_command_response(route: &str, other: &ControlApplied) -> Response {
    tracing::error!(
        route,
        ?other,
        "the meta group answered a placement write as another command"
    );
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "the meta group answered another command",
    )
        .into_response()
}

/// The refusal of a node that runs no meta runtime: the zero-config
/// in-memory node, or one that shuts down.
fn no_meta_runtime() -> PlacementApiError {
    PlacementApiError {
        reason: PlacementApiReason::Stopped,
        write_effect: WriteEffect::NotApplied,
        current: None,
        message: "this node runs no meta runtime".to_owned(),
    }
}

/// The one answer of every placement refusal: `504` when the write may
/// have applied, `404` for an unknown group or node, `503` while the meta
/// group cannot answer (not bootstrapped, no leader, a failed forward, a
/// stopped replica or an unconfirmed view), and `409` for any other
/// refusal of the meta state machine.
pub(crate) fn placement_api_error_response(error: PlacementApiError) -> Response {
    let status = match (&error.reason, error.write_effect) {
        (_, WriteEffect::MayHaveApplied) => StatusCode::GATEWAY_TIMEOUT,
        (
            PlacementApiReason::Refused(
                ControlError::UnknownGroup { .. } | ControlError::UnknownNode { .. },
            ),
            WriteEffect::NotApplied,
        ) => StatusCode::NOT_FOUND,
        (PlacementApiReason::Refused(ControlError::NotBootstrapped), WriteEffect::NotApplied)
        | (
            PlacementApiReason::NoLeader
            | PlacementApiReason::ForwardFailed { .. }
            | PlacementApiReason::LeaderRefused { .. }
            | PlacementApiReason::Stopped
            | PlacementApiReason::Unconfirmed,
            WriteEffect::NotApplied,
        ) => StatusCode::SERVICE_UNAVAILABLE,
        (PlacementApiReason::Refused(_), WriteEffect::NotApplied) => StatusCode::CONFLICT,
    };
    (status, Json(error)).into_response()
}
