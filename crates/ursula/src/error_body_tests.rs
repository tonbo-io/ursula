//! Every error answer of the client plane carries one JSON body,
//! `{"error":{"code":"<code>","message":"<detail>"}}` (extensions Section 11): the handlers' own refusals,
//! the runtime's, and axum's `404`, `405` and extractor rejections, which
//! the router renders. The operator paths under `/__ursula/` keep their own
//! answers.

use axum::Router;
use axum::body::Body;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use axum::response::Response;
use tower::ServiceExt;
use ursula_headers::ErrorCode;
use ursula_headers::names;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::ShardRuntime;

use crate::render::error_body_of;
use crate::render::error_code_of;
use crate::router;

fn app() -> Router {
    router(ShardRuntime::spawn(RuntimeConfig::new(1, 1)).expect("runtime"))
}

async fn send(app: &Router, method: &str, uri: &str, headers: &[(&str, &str)]) -> Response {
    send_body(app, method, uri, headers, "").await
}

async fn send_body(
    app: &Router,
    method: &str,
    uri: &str,
    headers: &[(&str, &str)],
    body: &'static str,
) -> Response {
    let mut request = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    app.clone()
        .oneshot(request.body(Body::from(body)).expect("request"))
        .await
        .expect("response")
}

/// Asserts `response`'s status and the code of its error body.
async fn assert_refused(response: Response, status: StatusCode, code: ErrorCode) {
    assert_eq!(response.status(), status, "{code}");
    assert_eq!(error_code_of(response).await, code);
}

const TEXT: (&str, &str) = ("content-type", "text/plain");

#[tokio::test]
async fn front_door_refusals_name_their_code() {
    let app = app();
    let created = send_body(&app, "PUT", "/errors/s", &[TEXT], "abc").await;
    assert_eq!(created.status(), StatusCode::CREATED);

    // 400s of the request's shape.
    let refused = send(&app, "GET", "/errors/s?consistency=strong", &[]).await;
    assert_eq!(refused.status(), StatusCode::BAD_REQUEST);
    let body = error_body_of(refused).await;
    assert_eq!(body.code, ErrorCode::InvalidQuery);
    assert_eq!(body.message, "invalid consistency");
    assert_refused(
        send(&app, "GET", "/errors/s?offset=abc", &[]).await,
        StatusCode::BAD_REQUEST,
        ErrorCode::InvalidOffset,
    )
    .await;
    // The message says which header and why.
    let refused = send(&app, "PUT", "/errors/t", &[("stream-ttl", "01")]).await;
    assert_eq!(refused.status(), StatusCode::BAD_REQUEST);
    let body = error_body_of(refused).await;
    assert_eq!(body.code, ErrorCode::InvalidHeader);
    assert_eq!(body.message, "stream-ttl must not contain leading zeros");
    assert_refused(
        send(&app, "GET", "/errors/s", &[(
            names::STREAM_INCARNATION,
            "x",
        )])
        .await,
        StatusCode::BAD_REQUEST,
        ErrorCode::InvalidHeader,
    )
    .await;
    assert_refused(
        send_body(
            &app,
            "PUT",
            "/errors/t",
            &[
                ("producer-id", "p"),
                ("producer-epoch", "0"),
                ("producer-seq", "0"),
            ],
            "x",
        )
        .await,
        StatusCode::BAD_REQUEST,
        ErrorCode::InvalidProducer,
    )
    .await;
    assert_refused(
        send(&app, "POST", "/errors/s", &[TEXT]).await,
        StatusCode::BAD_REQUEST,
        ErrorCode::EmptyAppend,
    )
    .await;
    assert_refused(
        send_body(&app, "POST", "/errors/s", &[], "x").await,
        StatusCode::BAD_REQUEST,
        ErrorCode::MissingContentType,
    )
    .await;
    assert_refused(
        send(&app, "PUT", "/errors/s/snapshot/x", &[]).await,
        StatusCode::BAD_REQUEST,
        ErrorCode::InvalidOffset,
    )
    .await;

    // A malformed JSON body is named, never echoed.
    let json = send_body(
        &app,
        "PUT",
        "/errors/j",
        &[("content-type", "application/json")],
        "{\"secret-token\": nope}",
    )
    .await;
    assert_eq!(json.status(), StatusCode::BAD_REQUEST);
    let body = error_body_of(json).await;
    assert_eq!(body.code, ErrorCode::InvalidRecordBoundaries);
    assert!(!body.message.contains("secret-token"), "{}", body.message);
    assert!(!body.message.contains("nope"), "{}", body.message);

    // The stream's own refusals.
    assert_refused(
        send(&app, "GET", "/errors/missing", &[]).await,
        StatusCode::NOT_FOUND,
        ErrorCode::StreamNotFound,
    )
    .await;
    assert_refused(
        send_body(
            &app,
            "POST",
            "/errors/s",
            &[("content-type", "application/octet-stream")],
            "x",
        )
        .await,
        StatusCode::CONFLICT,
        ErrorCode::ContentTypeMismatch,
    )
    .await;
    let token = created
        .headers()
        .get(names::STREAM_INCARNATION)
        .and_then(|value| value.to_str().ok())
        .expect("an incarnation")
        .to_owned();
    let other = (token.parse::<u64>().expect("a token") + 1).to_string();
    assert_refused(
        send(&app, "GET", "/errors/s?offset=-1", &[(
            names::STREAM_INCARNATION,
            other.as_str(),
        )])
        .await,
        StatusCode::PRECONDITION_FAILED,
        ErrorCode::IncarnationMismatch,
    )
    .await;
    assert_refused(
        send(&app, "GET", "/errors/s/snapshot/1", &[]).await,
        StatusCode::NOT_FOUND,
        ErrorCode::SnapshotNotFound,
    )
    .await;
    let closed = send(&app, "POST", "/errors/s", &[
        TEXT,
        (names::STREAM_CLOSED, "true"),
    ])
    .await;
    assert_eq!(closed.status(), StatusCode::NO_CONTENT);
    let refused = send_body(&app, "POST", "/errors/s", &[TEXT], "x").await;
    assert_eq!(
        refused
            .headers()
            .get(names::STREAM_CLOSED)
            .and_then(|value| value.to_str().ok()),
        Some("true"),
        "the typed header stays"
    );
    assert_refused(refused, StatusCode::CONFLICT, ErrorCode::StreamClosed).await;

    // A node without a cold store takes no parts.
    assert_refused(
        send_body(&app, "POST", "/errors/s/snapshot/1/parts", &[], "part").await,
        StatusCode::NOT_IMPLEMENTED,
        ErrorCode::NoColdStore,
    )
    .await;
}

/// axum's own answers get a body too: an unknown path, a method the route
/// does not serve, and the removed latest-snapshot path, which keeps its
/// empty `Allow`.
#[tokio::test]
async fn the_routers_own_answers_carry_an_error_body() {
    let app = app();
    assert_refused(
        send(&app, "GET", "/a/b/c/d", &[]).await,
        StatusCode::NOT_FOUND,
        ErrorCode::NotFound,
    )
    .await;
    assert_refused(
        send(&app, "PATCH", "/errors/s", &[]).await,
        StatusCode::METHOD_NOT_ALLOWED,
        ErrorCode::MethodNotAllowed,
    )
    .await;
    let removed = send(&app, "GET", "/errors/s/snapshot", &[]).await;
    assert_eq!(
        removed
            .headers()
            .get("allow")
            .and_then(|value| value.to_str().ok()),
        Some("")
    );
    assert_refused(
        removed,
        StatusCode::METHOD_NOT_ALLOWED,
        ErrorCode::Unsupported,
    )
    .await;
    // A `HEAD` answer has no body, but says what one would be.
    let head = send(&app, "HEAD", "/errors/missing", &[]).await;
    assert_eq!(head.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        head.headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/json")
    );
}

/// The operator paths keep their own answers.
#[tokio::test]
async fn operator_paths_are_left_alone() {
    let app = app();
    let response = send(&app, "POST", "/__ursula/metrics", &[]).await;
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert!(response.headers().get(CONTENT_TYPE).is_none());
}

/// Whether `message` names an internal of the server: an address, an
/// object key or path, a request id, or the node's placement.
fn names_internals(message: &str) -> bool {
    let ipv4 = message
        .as_bytes()
        .windows(3)
        .any(|window| matches!(window, [a, b'.', c] if a.is_ascii_digit() && c.is_ascii_digit()));
    ipv4 || [
        "://",
        "_packs",
        ".bin",
        "RequestId",
        "x-amz",
        "s3",
        "core ",
        "raft group",
        "BasicNode",
        "10.",
        "cluster-",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

/// M1: no message from any source class names an address, an S3 key or
/// path, a request id or the node's placement, on an HTTP answer and an SSE
/// `error` event alike. The full error stays in the server's log.
#[tokio::test]
async fn no_message_names_the_servers_internals() {
    use ursula_runtime::ColdOperation;
    use ursula_runtime::ForwardFailure;
    use ursula_runtime::ForwardedRpc;
    use ursula_runtime::GroupEngineError;
    use ursula_runtime::GroupInfraError;
    use ursula_runtime::KnownLeader;
    use ursula_runtime::LeadershipReason;
    use ursula_runtime::LeadershipRefusal;
    use ursula_runtime::Proposal;
    use ursula_runtime::RuntimeError;
    use ursula_runtime::StreamErrorCode;
    use ursula_shard::BucketStreamId;
    use ursula_shard::CoreId;
    use ursula_shard::RaftGroupId;

    let engine = |error| RuntimeError::GroupEngine {
        core_id: CoreId(3),
        raft_group_id: RaftGroupId(9),
        error,
    };
    let leader = "http://10.1.2.3:4438";
    let cases = [
        // A leadership lost after the write was proposed: its outcome is
        // unknown.
        (
            engine(GroupEngineError::NotLeader(LeadershipRefusal {
                leader: Some(KnownLeader {
                    node_id: 2,
                    term: 5,
                    address: Some(leader.to_owned()),
                }),
                proposal: Proposal::MayHaveCommitted,
                reason: LeadershipReason::SteppedDown,
            })),
            ErrorCode::OutcomeUnknown,
        ),
        // A leadership check that failed before the proposal.
        (
            engine(GroupEngineError::NotLeader(LeadershipRefusal {
                leader: Some(KnownLeader {
                    node_id: 2,
                    term: 5,
                    address: Some(leader.to_owned()),
                }),
                proposal: Proposal::NotProposed,
                reason: LeadershipReason::Follower,
            })),
            ErrorCode::NoLeader,
        ),
        // A cold-tier read on GET or bootstrap.
        (
            engine(GroupEngineError::Infra(GroupInfraError::ColdStore {
                operation: ColdOperation::ReadPayload {
                    stream_id: BucketStreamId::new("tenant", "s"),
                },
            })),
            ErrorCode::ColdStoreFailed,
        ),
        // A forwarded read the leader refused.
        (
            engine(GroupEngineError::Infra(GroupInfraError::ForwardFailed {
                rpc: ForwardedRpc::GroupRead,
                failure: ForwardFailure::Refused { grpc_code: 9 },
            })),
            ErrorCode::Internal,
        ),
        // Pressure, whose detail counts other tenants' bytes.
        (
            engine(GroupEngineError::cold_backpressure(
                BucketStreamId::new("other", "s"),
                10,
                20,
                15,
            )),
            ErrorCode::ColdBackpressure,
        ),
        (
            RuntimeError::ColdStoreIo {
                message: "s3://tenant-cold/_packs/0/pack-7.bin: timed out".to_owned(),
            },
            ErrorCode::ColdStoreFailed,
        ),
        // A stream refusal: its own text, without the node's placement.
        (
            engine(GroupEngineError::stream(
                StreamErrorCode::StreamClosed,
                "stream 'errors/s' is closed",
            )),
            ErrorCode::StreamClosed,
        ),
        (
            engine(GroupEngineError::stream(
                StreamErrorCode::StreamNotFound,
                "stream 'errors/s' does not exist",
            )),
            ErrorCode::StreamNotFound,
        ),
    ];
    for (err, code) in cases {
        let event = crate::render::sse_error_event(&crate::render::runtime_error_body(&err).1);
        assert!(!names_internals(&event), "{event}");
        let answer = if err.not_leader().is_some() {
            crate::leader_unknown_retry_response(err, crate::Access::Write)
        } else {
            crate::runtime_error_response(err)
        };
        let body = error_body_of(answer).await;
        assert_eq!(body.code, code);
        assert!(!names_internals(&body.message), "{code}: {}", body.message);
    }
}

/// m6: a node's `404` for a missing stream carries the code's fixed message
/// and only the headers every error answer carries, so a gateway's
/// concealing `404` is the same answer.
#[tokio::test]
async fn a_missing_streams_404_is_fixed() {
    let app = app();
    let created = send_body(&app, "PUT", "/errors/s", &[TEXT], "abc").await;
    assert_eq!(created.status(), StatusCode::CREATED);
    assert_eq!(
        error_body_of(send(&app, "GET", "/errors/missing", &[]).await).await,
        ursula_headers::ErrorBody::fixed(ErrorCode::StreamNotFound)
    );
    // A bucket nothing created: the answer a concealing gateway gives.
    let response = send(&app, "GET", "/nobucket/missing", &[]).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let mut headers: Vec<(String, String)> = response
        .headers()
        .iter()
        .filter(|(name, _)| *name != axum::http::header::CONTENT_LENGTH)
        .map(|(name, value)| {
            (
                name.as_str().to_owned(),
                value.to_str().expect("text").to_owned(),
            )
        })
        .collect();
    headers.sort();
    assert_eq!(headers, vec![
        ("cache-control".to_owned(), "no-store".to_owned()),
        ("content-type".to_owned(), "application/json".to_owned()),
        (
            "cross-origin-resource-policy".to_owned(),
            "cross-origin".to_owned()
        ),
        ("x-content-type-options".to_owned(), "nosniff".to_owned()),
    ]);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    assert_eq!(
        body,
        ursula_headers::ErrorBody::fixed(ErrorCode::BucketNotFound)
            .render()
            .as_bytes()
    );
}

/// m4: an error answer is never compressed, however long its body, while
/// a JSON success body is.
#[test]
fn error_answers_are_never_compressed() {
    let error = crate::render::error_response(
        StatusCode::BAD_REQUEST,
        ErrorCode::InvalidHeader,
        "x".repeat(1024),
    );
    assert!(!crate::finite_record_response(
        error.status(),
        error.version(),
        error.headers(),
        error.extensions(),
    ));
    let json = (
        StatusCode::OK,
        [(CONTENT_TYPE, "application/json")],
        "[]".repeat(512),
    )
        .into_response();
    assert!(crate::finite_record_response(
        json.status(),
        json.version(),
        json.headers(),
        json.extensions(),
    ));
}

/// m4: the net keeps a response's extensions and adds the one that keeps
/// compression off.
#[test]
fn the_net_keeps_extensions() {
    #[derive(Debug, Clone, PartialEq)]
    struct Marker;
    let mut response = StatusCode::NOT_FOUND.into_response();
    response.extensions_mut().insert(Marker);
    crate::render::render_unrendered_error(&mut response);
    assert_eq!(response.extensions().get::<Marker>(), Some(&Marker));
    assert!(
        response
            .extensions()
            .get::<crate::render::Uncompressed>()
            .is_some()
    );
}

/// A `Stream-Seq` used as a compare-and-set: its `409` has its own code and
/// echoes the stream's last accepted seq, and a content-type mismatch stays
/// apart.
#[tokio::test]
async fn a_stream_seq_conflict_echoes_the_last_seq() {
    let app = app();
    let created = send_body(&app, "PUT", "/errors/cas", &[TEXT], "").await;
    assert_eq!(created.status(), StatusCode::CREATED);
    let first = send_body(
        &app,
        "POST",
        "/errors/cas",
        &[TEXT, ("stream-seq", "0002")],
        "a",
    )
    .await;
    assert_eq!(first.status(), StatusCode::NO_CONTENT);

    let stale = send_body(
        &app,
        "POST",
        "/errors/cas",
        &[TEXT, ("stream-seq", "0001")],
        "b",
    )
    .await;
    assert_eq!(stale.status(), StatusCode::CONFLICT);
    let typed = ursula_headers::StreamSeqConflict::parse(
        409,
        stale
            .headers()
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes())),
    )
    .expect("a well-formed answer")
    .expect("a Stream-Seq conflict");
    assert_eq!(typed.last.as_str(), "0002");
    assert_eq!(error_code_of(stale).await, ErrorCode::StreamSeqConflict);

    // A retry from the echoed seq applies.
    let retried = send_body(
        &app,
        "POST",
        "/errors/cas",
        &[TEXT, ("stream-seq", "0003")],
        "c",
    )
    .await;
    assert_eq!(retried.status(), StatusCode::NO_CONTENT);

    let mismatch = send_body(
        &app,
        "POST",
        "/errors/cas",
        &[
            ("content-type", "application/octet-stream"),
            ("stream-seq", "0004"),
        ],
        "d",
    )
    .await;
    assert_eq!(mismatch.status(), StatusCode::CONFLICT);
    assert!(mismatch.headers().get(names::STREAM_LAST_SEQ).is_none());
    assert_eq!(
        error_code_of(mismatch).await,
        ErrorCode::ContentTypeMismatch
    );
}

/// Every allowlisted refusal sends its own text, and no such text names an
/// internal of the server ([`crate::render::REFUSALS_WITH_TEXT`]).
#[tokio::test]
async fn every_allowlisted_refusal_names_no_internals() {
    let app = app();
    let producer = |epoch: &'static str, seq: &'static str| {
        [
            TEXT,
            ("producer-id", "writer"),
            ("producer-epoch", epoch),
            ("producer-seq", seq),
        ]
    };
    let digest = blake3::hash(b"other").to_hex().to_string();
    assert_eq!(
        send_body(&app, "PUT", "/allow/s", &[TEXT], "")
            .await
            .status(),
        StatusCode::CREATED
    );
    let mut seen = Vec::new();
    let check = |response: Response| async {
        let status = response.status();
        let body = error_body_of(response).await;
        assert!(
            !names_internals(&body.message),
            "{}: {}",
            body.code,
            body.message
        );
        assert_ne!(
            body.message,
            body.code.default_message(),
            "{status} {} sends its own text",
            body.code
        );
        body.code
    };
    for (method, uri, headers, payload) in [
        ("PUT", "/ab/s", vec![TEXT], ""),
        ("PUT", "/allow/$s", vec![TEXT], ""),
        (
            "PUT",
            "/allow/s",
            vec![("content-type", "application/json")],
            "",
        ),
        (
            "POST",
            "/allow/s",
            vec![("content-type", "application/json")],
            "{}",
        ),
        ("POST", "/allow/s", vec![TEXT, ("stream-seq", "0002")], "a"),
        ("POST", "/allow/s", vec![TEXT, ("stream-seq", "0001")], "b"),
        ("GET", "/allow/s?offset=99", vec![], ""),
    ] {
        let response = send_body(&app, method, uri, &headers, payload).await;
        if response.status().is_client_error() {
            seen.push(check(response).await);
        }
    }
    for (headers, payload) in [
        (producer("0", "0").to_vec(), "a"),
        (producer("0", "5").to_vec(), "a"),
        (producer("1", "1").to_vec(), "a"),
        (producer("1", "0").to_vec(), "a"),
        (producer("2", "0").to_vec(), "a"),
        (producer("0", "1").to_vec(), "a"),
        (
            [producer("2", "0").as_slice(), &[(
                "producer-digest",
                digest.as_str(),
            )]]
            .concat(),
            "other",
        ),
    ] {
        let response = send_body(&app, "POST", "/allow/s", &headers, payload).await;
        if response.status().is_client_error() {
            seen.push(check(response).await);
        }
    }
    let closed = send(&app, "POST", "/allow/s", &[
        TEXT,
        (names::STREAM_CLOSED, "true"),
    ])
    .await;
    assert!(closed.status().is_success(), "{}", closed.status());
    seen.push(check(send_body(&app, "POST", "/allow/s", &[TEXT], "x").await).await);
    for code in crate::render::REFUSALS_WITH_TEXT {
        let (_, wire) = crate::render::stream_error_answer(*code);
        assert!(seen.contains(&wire), "no case sends {wire}; saw {seen:?}");
    }
}

/// The status and code of every group engine error sample: the table, an
/// exhaustive match. A new variant states its answer here.
#[test]
fn every_group_engine_error_answers_its_table_row() {
    use ursula_runtime::GroupEngineError;
    use ursula_runtime::GroupInfraError;
    use ursula_runtime::GroupSnapshotError;
    use ursula_runtime::RuntimeError;
    use ursula_shard::CoreId;
    use ursula_shard::RaftGroupId;

    fn expected(error: &GroupEngineError) -> (StatusCode, ErrorCode) {
        let internal = (StatusCode::INTERNAL_SERVER_ERROR, ErrorCode::Internal);
        let unavailable = |code| (StatusCode::SERVICE_UNAVAILABLE, code);
        match error {
            GroupEngineError::Stream(refusal) => crate::render::stream_error_answer(refusal.code),
            GroupEngineError::NotLeader(_) => internal,
            GroupEngineError::Infra(infra) => match infra {
                GroupInfraError::ColdBackpressure { .. } => {
                    unavailable(ErrorCode::ColdBackpressure)
                }
                GroupInfraError::RaftUncommittedBackpressure { .. } => {
                    unavailable(ErrorCode::RaftUncommittedBackpressure)
                }
                GroupInfraError::RecoveryPending { .. } => unavailable(ErrorCode::HistoryPending),
                GroupInfraError::ApplyStopped { .. } => unavailable(ErrorCode::ApplyStopped),
                GroupInfraError::EngineWorkOutlivedShutdown { .. }
                | GroupInfraError::PreviousEngineWorkRunning { .. } => {
                    unavailable(ErrorCode::GroupRestarting)
                }
                GroupInfraError::OutcomeUnknown => unavailable(ErrorCode::OutcomeUnknown),
                GroupInfraError::InvalidRaftCommand { .. } => {
                    (StatusCode::BAD_REQUEST, ErrorCode::InvalidRequest)
                }
                GroupInfraError::ColdStore { .. } => {
                    (StatusCode::BAD_GATEWAY, ErrorCode::ColdStoreFailed)
                }
                GroupInfraError::Snapshot(
                    GroupSnapshotError::InvalidContent(_)
                    | GroupSnapshotError::ForeignKey { .. }
                    | GroupSnapshotError::ObjectMissing { .. }
                    | GroupSnapshotError::ObjectCorrupt { .. }
                    | GroupSnapshotError::PointerUnreadable { .. }
                    | GroupSnapshotError::StoreUnavailable { .. },
                )
                | GroupInfraError::RecoveryNodeMismatch { .. }
                | GroupInfraError::RaftGroupNotRegistered { .. }
                | GroupInfraError::RecoveryAlreadyBound { .. }
                | GroupInfraError::RecoveryGroupMismatch { .. }
                | GroupInfraError::WalOpen { .. }
                | GroupInfraError::LogBelowSnapshot { .. }
                | GroupInfraError::RecoveryVoteFloor { .. }
                | GroupInfraError::RecoveryCutting { .. }
                | GroupInfraError::CommittedOverwrite { .. }
                | GroupInfraError::UnrelatedHistory { .. }
                | GroupInfraError::LogBehindCommit { .. }
                | GroupInfraError::RecoveryPlanFenced { .. }
                | GroupInfraError::OwnerStopped
                | GroupInfraError::Internal { .. }
                | GroupInfraError::UnexpectedResponse { .. }
                | GroupInfraError::MissingStreamId { .. }
                | GroupInfraError::Unsupported { .. }
                | GroupInfraError::BootstrapWindowNotHot { .. }
                | GroupInfraError::ColdCompactionStale { .. }
                | GroupInfraError::ForwardFailed { .. }
                | GroupInfraError::ForwardedAnswerInvalid { .. }
                | GroupInfraError::InvalidRaftConfig { .. }
                | GroupInfraError::StaticMembership { .. }
                | GroupInfraError::RecoveryGateOpen { .. }
                | GroupInfraError::MembershipInit { .. }
                | GroupInfraError::LeaderWaitTimedOut { .. } => internal,
            },
        }
    }

    for error in ursula_runtime::group_engine_error_samples() {
        let row = expected(&error);
        let err = RuntimeError::GroupEngine {
            core_id: CoreId(0),
            raft_group_id: RaftGroupId(0),
            error,
        };
        assert_eq!(crate::render::runtime_error_answer(&err), row, "{err:?}");
    }
}
