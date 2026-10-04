//! Request surface removed in 0.6.0 that must fail loudly instead of being
//! ignored.
//!
//! - `Stream-Snapshot-Match` answers 400. Ignoring it would turn a
//!   conditional snapshot publish into an unconditional one.
//! - A bare `GET {stream}/snapshot` (the old latest-snapshot redirect) answers
//!   405, not 404: Loro's streams client reads a 404 as "no snapshot". The
//!   router registers only `PUT` on that path, so axum answers 405 with
//!   `Allow: PUT`. Clients read the latest snapshot's offset from HEAD and
//!   fetch `GET {stream}/snapshot/{offset}`.
//! - Three-segment stream paths (path affinity) and
//!   `POST /{bucket}/{group}/$transaction` answer 404: no route matches. A
//!   two-segment stream ID may not start with `$` (400), so a future
//!   bucket-level `$` subresource cannot collide with an existing stream.
//! - `/__ursula/feature-level` has no route: it falls through to stream
//!   routing, where bucket `__ursula` holds no streams (`GET` answers 404)
//!   and is a reserved bucket ID (`POST` answers 4xx). Every group runs one
//!   behaviour, so there is no level to report or raise.
//!
//! The removed names live only in this file, which the release's "nothing
//! left" check allowlists.

use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;

/// The removed compare-and-set precondition on snapshot publish.
const HEADER_STREAM_SNAPSHOT_MATCH: &str = "stream-snapshot-match";

/// Rejects a snapshot publish that carries a removed precondition header.
pub(crate) fn reject_removed_snapshot_headers(headers: &HeaderMap) -> Result<(), Box<Response>> {
    if headers.contains_key(HEADER_STREAM_SNAPSHOT_MATCH) {
        return Err(Box::new(
            (
                StatusCode::BAD_REQUEST,
                "Stream-Snapshot-Match is not supported",
            )
                .into_response(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::Router;
    use axum::body::Body;
    use axum::body::to_bytes;
    use axum::http::Request;
    use axum::http::header::CONTENT_TYPE;
    use tower::ServiceExt;
    use ursula_runtime::RuntimeConfig;
    use ursula_runtime::ShardRuntime;

    use super::*;
    use crate::HEADER_STREAM_NEXT_OFFSET;
    use crate::HEADER_STREAM_SNAPSHOT_OFFSET;

    fn app() -> Router {
        crate::router(ShardRuntime::spawn(RuntimeConfig::new(1, 1)).expect("runtime"))
    }

    async fn send(
        app: &Router,
        method: &str,
        uri: &str,
        headers: &[(&str, &str)],
        body: &str,
    ) -> Response {
        let mut request = Request::builder().method(method).uri(uri);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        app.clone()
            .oneshot(request.body(Body::from(body.to_owned())).expect("request"))
            .await
            .expect("response")
    }

    async fn body_text(response: Response) -> String {
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        String::from_utf8(body.to_vec()).expect("utf-8 body")
    }

    /// Creates an octet stream holding `abc` and returns its tail offset.
    async fn stream_with_bytes(app: &Router, uri: &str) -> String {
        let octet = [(CONTENT_TYPE.as_str(), "application/octet-stream")];
        let response = send(app, "PUT", uri, &octet, "abc").await;
        assert_eq!(response.status(), StatusCode::CREATED);
        response
            .headers()
            .get(HEADER_STREAM_NEXT_OFFSET)
            .expect("next offset")
            .to_str()
            .expect("utf-8 header")
            .to_owned()
    }

    #[tokio::test]
    async fn stream_snapshot_match_answers_400_and_publishes_nothing() {
        let app = app();
        let uri = "/removed/snapshot-match";
        let at = stream_with_bytes(&app, uri).await;
        let digest = "0".repeat(64);
        let header = [(HEADER_STREAM_SNAPSHOT_MATCH, digest.as_str())];

        for target in [
            format!("{uri}/snapshot/{at}"),
            format!("{uri}/snapshot?record=0"),
        ] {
            let response = send(&app, "PUT", &target, &header, "state").await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{target}");
            assert!(
                body_text(response).await.contains("Stream-Snapshot-Match"),
                "{target}"
            );
        }

        let head = send(&app, "HEAD", uri, &[], "").await;
        assert_eq!(head.status(), StatusCode::OK);
        assert!(!head.headers().contains_key(HEADER_STREAM_SNAPSHOT_OFFSET));
    }

    #[tokio::test]
    async fn three_segment_paths_answer_404_and_dollar_stream_ids_400() {
        let app = app();
        let text = [(CONTENT_TYPE.as_str(), "text/plain")];
        let response = send(&app, "PUT", "/removed/run-42/journal", &text, "event").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let json = [(CONTENT_TYPE.as_str(), "application/json")];
        let response = send(
            &app,
            "POST",
            "/removed/run-42/$transaction",
            &json,
            r#"{"operations":[]}"#,
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = send(&app, "PUT", "/removed/$transaction", &json, "").await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn feature_level_endpoint_is_gone() {
        let app = app();
        let response = send(&app, "GET", "/__ursula/feature-level", &[], "").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let json = [(CONTENT_TYPE.as_str(), "application/json")];
        let response = send(
            &app,
            "POST",
            "/__ursula/feature-level",
            &json,
            r#"{"level":1}"#,
        )
        .await;
        assert!(response.status().is_client_error(), "{}", response.status());
    }
}
