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
//!    `x-ursula-raft-term`. When it knows no leader, a node whose replica
//!    counts as a voter answers `503` with `Retry-After` and
//!    `x-ursula-raft-group`, and any other node `307` (`NotHosted`) to a
//!    node its placement view gives the group ([`route_before_body`]);
//! 4. early answers: the leader's answer to a repeat of an applied producer
//!    write or a key conflict (section 2.5), and the `429` of a snapshot
//!    part whose stream holds as many staged parts as it may
//!    ([`crate::staged_parts`]);
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
use ursula_runtime::UploadFence;
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
/// The route that discards a stream's staged snapshot parts (`DELETE`,
/// #622). It carries no body.
pub(crate) const STAGED_PARTS_ROUTE: &str = "/{bucket}/{stream}/snapshot/parts";
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
    /// body, holding the permit (if admission runs) until the answer. A
    /// cold upload of the write is fenced to `fence`, the leadership that
    /// admitted it (F14j).
    Read {
        permit: Option<AdmissionPermit>,
        fence: UploadFence,
    },
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
    // The leadership view decides a leader and a follower without a trip
    // through the group's core. A leader's early answer, a group the view
    // does not know, and a view with no leader need the core. The view
    // shows a replica from the moment its open publishes it, while the
    // core still opens the group: the open of a group's only voter waits
    // until it leads, and the core holds the write until that open ended.
    let viewed = leadership_view(state, raft_group_id);
    let routed = match viewed {
        Some(WriteRoute::Leader { fence, .. }) if early_answer.is_none() => {
            return admit(admission, fence);
        }
        Some(route @ WriteRoute::Follower { .. }) => RoutedWrite {
            raft_group_id,
            route,
        },
        Some(WriteRoute::Leader { .. } | WriteRoute::NoLeader) | None => match state
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
                    crate::write_error_or_leader_redirect_async(state, err, request_target).await,
                );
            }
        },
    };
    let group = RaftGroup::from_u32(routed.raft_group_id.0);
    match routed.route {
        WriteRoute::Follower { leader_id, term } => {
            let location = state
                .client_write_router()
                .and_then(|router| router.leader_url(leader_id, request_target));
            HeadDecision::Answer(match location {
                Some(location) => routing_response(
                    RoutingAnswer::NotLeader {
                        leader: NodeId::from_u64(leader_id),
                        group,
                        term: RaftTerm::from_u64(term),
                    },
                    Some(&location),
                ),
                // A leader that `raft.peers` does not list: as good as none
                // known.
                None => no_leader_answer(state, routed.raft_group_id, request_target),
            })
        }
        WriteRoute::NoLeader => HeadDecision::Answer(no_leader_answer(
            state,
            routed.raft_group_id,
            request_target,
        )),
        // An early answer exists only for a query, which names its token.
        WriteRoute::Leader {
            early_answer: Some(answer),
            ..
        } if let Some(incarnation) = incarnation => {
            HeadDecision::Answer(crate::early_answer_response(answer, incarnation))
        }
        WriteRoute::Leader { fence, .. } => admit(admission, fence),
    }
}

/// This node's view of a group's leadership, from its Raft metrics. A node
/// without replication leads every group. `None` when the node has no
/// replica of the group registered: it may host none, or not have started
/// it yet, and the group's core decides.
///
/// A node without a Raft registry serves in-memory engines only, and its
/// uploads are fenced to [`UploadFence::Unreplicated`]. A Raft engine behind
/// such a front door refuses those uploads before proposing them and the
/// uploader deletes them: the fail-safe answer, since a fence the engine
/// supplied after the PUT would not bound the upload. A producer write with
/// a query routes through its engine before the body and takes the engine's
/// fence there, still before its PUT.
fn leadership_view(state: &HttpState, raft_group_id: RaftGroupId) -> Option<WriteRoute> {
    let Some(registry) = state.raft_registry() else {
        return Some(WriteRoute::Leader {
            early_answer: None,
            fence: UploadFence::Unreplicated,
        });
    };
    registry
        .leadership(raft_group_id)
        .map(GroupLeadership::write_route)
}

/// The leadership in which this node leads `raft_group_id` now, by its
/// leadership view, which a cold upload it admits now is fenced to (F14j):
/// [`UploadFence::Unreplicated`] for a node without replication, which
/// leads every group, and `None` while it does not lead the group, a
/// candidate included.
pub(crate) fn upload_fence(state: &HttpState, raft_group_id: RaftGroupId) -> Option<UploadFence> {
    registry_upload_fence(state.raft_registry(), raft_group_id)
}

/// [`upload_fence`] from a registry: [`UploadFence::Unreplicated`] without
/// one.
pub(crate) fn registry_upload_fence(
    registry: Option<&ursula_raft::RaftGroupHandleRegistry>,
    raft_group_id: RaftGroupId,
) -> Option<UploadFence> {
    let Some(registry) = registry else {
        return Some(UploadFence::Unreplicated);
    };
    registry
        .leadership(raft_group_id)
        .and_then(|leadership| leadership.upload_fence())
}

/// The Raft term in which this node leads `raft_group_id` now, the term of
/// its [`upload_fence`]: `0` for a node without replication, and `None`
/// while it does not lead the group. A leader's node-local state (the
/// staged parts it counts) is valid for one term.
pub(crate) fn leader_term(state: &HttpState, raft_group_id: RaftGroupId) -> Option<u64> {
    upload_fence(state, raft_group_id).map(UploadFence::term)
}

/// [`leader_term`] from a registry: `0` without one.
pub(crate) fn registry_leader_term(
    registry: Option<&ursula_raft::RaftGroupHandleRegistry>,
    raft_group_id: RaftGroupId,
) -> Option<u64> {
    registry_upload_fence(registry, raft_group_id).map(UploadFence::term)
}

/// The answer to a write whose node stopped leading after it routed the
/// write: the routing answer now, `307` to the new leader or `503` with
/// `Retry-After`.
pub(crate) async fn not_leading(
    state: &HttpState,
    stream_id: &BucketStreamId,
    request_target: &str,
) -> Response {
    match route_before_body(state, stream_id, request_target, None, None).await {
        HeadDecision::Answer(answer) => answer,
        HeadDecision::Read { .. } => {
            let group = RaftGroup::from_u32(state.runtime.locate(stream_id).raft_group_id.0);
            routing_response(RoutingAnswer::NoLeader { group }, None)
        }
    }
}

/// The answer of a replica of `raft_group_id` that knows no leader to route
/// to: `NotHosted` toward a node its placement view gives the group when the
/// replica does not count as a voter, else `503` with `Retry-After`, as the
/// router decides for every such refusal
/// ([`crate::ClientWriteLeaderRouter`]).
fn no_leader_answer(
    state: &HttpState,
    raft_group_id: RaftGroupId,
    request_target: &str,
) -> Response {
    state
        .client_write_router()
        .and_then(|router| router.no_leader_redirect(raft_group_id, request_target))
        .unwrap_or_else(|| {
            routing_response(
                RoutingAnswer::NoLeader {
                    group: RaftGroup::from_u32(raft_group_id.0),
                },
                None,
            )
        })
}

/// Runs the deferred pressure checks, if admission runs, for a write this
/// node admits under `fence`.
pub(crate) fn admit(admission: Option<DeferredAdmission>, fence: UploadFence) -> HeadDecision {
    match admission.map(DeferredAdmission::admit) {
        None => HeadDecision::Read {
            permit: None,
            fence,
        },
        Some(Ok(permit)) => HeadDecision::Read {
            permit: Some(permit),
            fence,
        },
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
                "no leader of the stream's group is known",
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
            crate::write_error_or_leader_redirect_async(state, err, request_target).await
        }
        BodyState::Read => crate::runtime_error_after_body(state, err),
    }
}
