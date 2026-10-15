//! Decisions before the body (#581 section 6.1).
//!
//! A write that carries a body (a create, an append or close, a snapshot
//! publish or part) is decided from its head, in this order, before the node
//! polls any of its body:
//!
//! 1. size (`413`), checked by ingress admission from `Content-Length`,
//!    which runs before the handler;
//! 2. malformed headers (`400`), checked by each handler;
//! 3. routing: a node that does not lead the stream's group answers `307`
//!    to the leader with `x-ursula-raft-leader-id`, `x-ursula-raft-group` and
//!    `x-ursula-raft-term`, or `503` with `Retry-After` and
//!    `x-ursula-raft-group` when it knows no leader ([`route_before_body`]);
//! 4. early answers: the leader's answer to a repeat of an applied producer
//!    write or a key conflict (section 2.5);
//! 5. pressure (`503` with `Retry-After`), the admission checks deferred
//!    until here.
//!
//! Then the handler reads the body. After the body is read a node never
//! redirects: a leadership change answers `503` with `Retry-After`
//! ([`BodyState::Read`]), and the write's key resolves the outcome.

use axum::extract::MatchedPath;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::Method;
use axum::http::StatusCode;
use axum::http::header::LOCATION;
use axum::http::header::RETRY_AFTER;
use axum::response::IntoResponse;
use axum::response::Response;
use ursula_headers::ErrorCode;
use ursula_headers::NodeId;
use ursula_headers::RaftGroup;
use ursula_headers::RaftTerm;
use ursula_headers::RoutingAnswer;
use ursula_raft::GroupLeadership;
use ursula_runtime::EarlyAnswerQuery;
use ursula_runtime::RouteWriteRequest;
use ursula_runtime::RoutedWrite;
use ursula_runtime::RuntimeError;
use ursula_runtime::WriteRoute;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;

use crate::HttpState;
use crate::admission::AdmissionPermit;
use crate::admission::DeferredAdmission;
use crate::render::error_answer;
use crate::render::insert_default_response_headers;
use crate::render::insert_header_str;

/// The route of a stream: create (`PUT`), append or close (`POST`), read,
/// `HEAD` and delete.
pub(crate) const STREAM_ROUTE: &str = "/{bucket}/{stream}";
/// The route of a snapshot: publish or parts commit (`PUT`) and read.
pub(crate) const SNAPSHOT_ROUTE: &str = "/{bucket}/{stream}/snapshot/{snapshot_offset}";
/// The route that stages one part of a snapshot (`POST`, #581 section 3.1).
pub(crate) const SNAPSHOT_PARTS_ROUTE: &str = "/{bucket}/{stream}/snapshot/{snapshot_offset}/parts";

/// Whether a request is a write that is routed before its body, told by
/// the route that matched ([`STREAM_ROUTE`], [`SNAPSHOT_ROUTE`] and
/// [`SNAPSHOT_PARTS_ROUTE`], which the router is built with), never by
/// parsing the path again: a create, an append or close, a snapshot publish
/// or parts commit, and a snapshot part. Its handler runs the pressure
/// checks after routing.
pub(crate) fn routed_before_body(method: &Method, route: Option<&MatchedPath>) -> bool {
    matches!(
        (method, route.map(MatchedPath::as_str)),
        (&Method::PUT | &Method::POST, Some(STREAM_ROUTE))
            | (&Method::PUT, Some(SNAPSHOT_ROUTE))
            | (&Method::POST, Some(SNAPSHOT_PARTS_ROUTE))
    )
}

/// What a write's head decided.
pub(crate) enum HeadDecision {
    /// This node leads the stream's group and admitted the write: read the
    /// body, holding the permit (if admission runs) until the answer.
    Read(Option<AdmissionPermit>),
    /// The head alone decided the answer.
    Answer(Response),
}

/// Routes a write, then answers it early or admits it (items 3 to 5 of
/// the module's order). `early_answer` is the key of a tokened producer
/// write whose answer the leader may decide from its head.
pub(crate) async fn route_before_body(
    state: &HttpState,
    stream_id: &BucketStreamId,
    request_target: &str,
    early_answer: Option<EarlyAnswerQuery>,
    admission: Option<DeferredAdmission>,
) -> HeadDecision {
    let incarnation = early_answer.as_ref().map(|query| query.incarnation);
    let raft_group_id = state.runtime.locate(stream_id).raft_group_id;
    // The leadership view decides without a trip through the group's core.
    // Only a leader's early answer, and a group the view does not know,
    // need the core.
    let viewed = leadership_view(state, raft_group_id);
    let routed = match viewed {
        Some(WriteRoute::Leader { .. }) if early_answer.is_none() => return admit(admission),
        Some(route @ (WriteRoute::Follower { .. } | WriteRoute::NoLeader)) => RoutedWrite {
            raft_group_id,
            route,
        },
        Some(WriteRoute::Leader { .. }) | None => match state
            .runtime
            .route_write(RouteWriteRequest {
                stream_id: stream_id.clone(),
                early_answer,
            })
            .await
        {
            Ok(routed) => routed,
            // A node that hosts no replica of the group redirects to one
            // that does.
            Err(err) => {
                return HeadDecision::Answer(
                    crate::runtime_error_or_leader_redirect_async(state, err, request_target).await,
                );
            }
        },
    };
    let group = RaftGroup::from_u32(routed.raft_group_id.0);
    match routed.route {
        WriteRoute::Follower {
            leader_id,
            leader_address,
            term,
        } => {
            let location = state.client_write_router().and_then(|router| {
                router.leader_url(leader_id, leader_address.as_deref(), request_target)
            });
            HeadDecision::Answer(match location {
                Some(location) => routing_response(
                    RoutingAnswer::NotLeader {
                        leader: NodeId::from_u64(leader_id),
                        group,
                        term: RaftTerm::from_u64(term),
                    },
                    Some(&location),
                ),
                // No address for the leader: as good as none known.
                None => routing_response(RoutingAnswer::NoLeader { group }, None),
            })
        }
        WriteRoute::NoLeader => {
            HeadDecision::Answer(routing_response(RoutingAnswer::NoLeader { group }, None))
        }
        // An early answer exists only for a query, which names its token.
        WriteRoute::Leader {
            early_answer: Some(answer),
        } if let Some(incarnation) = incarnation => {
            HeadDecision::Answer(crate::early_answer_response(answer, incarnation))
        }
        WriteRoute::Leader { .. } => admit(admission),
    }
}

/// This node's view of a group's leadership, from its Raft metrics. A node
/// without replication leads every group. `None` when the node has no
/// replica of the group registered: it may host none, or not have started
/// it yet, and the group's core decides.
fn leadership_view(state: &HttpState, raft_group_id: RaftGroupId) -> Option<WriteRoute> {
    let Some(registry) = state.raft_registry() else {
        return Some(WriteRoute::Leader { early_answer: None });
    };
    registry
        .leadership(raft_group_id)
        .map(GroupLeadership::write_route)
}

/// Runs the deferred pressure checks, if admission runs.
pub(crate) fn admit(admission: Option<DeferredAdmission>) -> HeadDecision {
    match admission.map(DeferredAdmission::admit) {
        None => HeadDecision::Read(None),
        Some(Ok(permit)) => HeadDecision::Read(Some(permit)),
        Some(Err(refusal)) => HeadDecision::Answer(refusal.into_response()),
    }
}

/// A routing answer: `307` to `location`, or `503` with `Retry-After`.
fn routing_response(answer: RoutingAnswer, location: Option<&str>) -> Response {
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    answer.render(|name, value| insert_header_str(&mut headers, name, value));
    match (
        answer,
        location.and_then(|location| HeaderValue::from_str(location).ok()),
    ) {
        (RoutingAnswer::NotLeader { .. }, Some(location)) => {
            headers.insert(LOCATION, location);
            (
                StatusCode::TEMPORARY_REDIRECT,
                headers,
                "this node does not lead the stream's group",
            )
                .into_response()
        }
        _ => {
            headers.insert(RETRY_AFTER, HeaderValue::from_static("1"));
            error_answer(
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorCode::NoLeader,
                headers,
            )
        }
    }
}

/// Whether a write's body was read, which decides whether its answer may
/// still redirect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BodyState {
    /// Nothing of the body was read: a leadership refusal redirects.
    Unread,
    /// The body was read: a leadership refusal answers `503` with
    /// `Retry-After`, never a redirect.
    Read,
}

/// The answer to a failed write, by [`BodyState`].
pub(crate) async fn write_error_response(
    state: &HttpState,
    err: RuntimeError,
    request_target: &str,
    body: BodyState,
) -> Response {
    match body {
        BodyState::Unread => {
            crate::runtime_error_or_leader_redirect_async(state, err, request_target).await
        }
        BodyState::Read => crate::runtime_error_after_body(state, err),
    }
}
