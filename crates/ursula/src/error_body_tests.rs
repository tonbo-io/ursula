//! Every error answer of the client plane carries one JSON body,
//! `{"error":"<code>","message":"<detail>"}` (extensions Section 11): the handlers' own refusals,
//! the runtime's, and axum's `404`, `405` and extractor rejections, which
//! the router renders. The operator paths under `/__ursula/` keep their own
//! answers.

use axum::Router;
use axum::body::Body;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
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
