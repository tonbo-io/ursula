//! `GET /__ursula/placement` (design 11.1): this node's placement status
//! (`ursula_control::PlacementStatus`), on the client and the admin
//! listener. `?confirmed=true`, a meta read barrier, is served on the admin
//! listener only.
//!
//! Every refusal of a placement route answers through
//! [`placement_api_error_response`], one status per reason.

use axum::Json;
use axum::extract::Query;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use ursula_control::ControlError;
use ursula_control::PlacementApiError;
use ursula_control::PlacementApiReason;
use ursula_proto::placement::WriteEffect;

use crate::HttpState;

pub(crate) const PLACEMENT_PATH: &str = "/__ursula/placement";

/// The query of `GET /__ursula/placement`.
#[derive(Debug, Default, serde::Deserialize)]
pub(crate) struct PlacementQuery {
    /// Confirm the view by a meta read barrier first (admin listener only).
    #[serde(default)]
    confirmed: bool,
}

/// The client listener's placement status: the current view, never a
/// confirmed one, since a barrier is the admin listener's.
pub(crate) async fn placement_status(
    State(state): State<HttpState>,
    Query(query): Query<PlacementQuery>,
) -> Response {
    if query.confirmed {
        return (
            StatusCode::BAD_REQUEST,
            "`confirmed=true` is served on the admin listener only",
        )
            .into_response();
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
