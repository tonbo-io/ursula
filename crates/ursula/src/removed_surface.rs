//! Request surface removed in 0.6.0 that must fail loudly instead of being
//! ignored.
//!
//! - `Stream-Snapshot-Match` answers 400. Ignoring it would turn a
//!   conditional snapshot publish into an unconditional one.
//! - JSON record coordinates (F2): the read parameters `record`,
//!   `tail_records`, `max_records` and `record_view` answer 400 on every
//!   read (catch-up, long-poll and SSE), and so does `Stream-Record-Match`
//!   on an append. Ignoring them would read from the wrong position or turn
//!   a conditional append into an unconditional one. The record-addressed
//!   `PUT {stream}/snapshot?record=` and `PUT {stream}/retention?record=`
//!   routes are gone.
//! - A bare `GET {stream}/snapshot` (the old latest-snapshot redirect) answers
//!   405, not 404: Loro's streams client reads a 404 as "no snapshot". An
//!   explicit handler answers it. Clients read the latest snapshot's offset
//!   from HEAD and fetch `GET {stream}/snapshot/{offset}`.
//! - Three-segment stream paths (path affinity) and
//!   `POST /{bucket}/{group}/$transaction` answer 404: no route matches. A
//!   two-segment stream ID may not start with `$` (400), so a future
//!   bucket-level `$` subresource cannot collide with an existing stream.
//! - `/__ursula/feature-level` has no route. It only ever lived on the admin
//!   listener, which has no fallback, so it answers 404 there. (On the
//!   merged single-router build used in tests it falls through to stream
//!   routing, where `__ursula` is a reserved bucket ID, so it answers 400.)
//!   Every group runs one behaviour, so there is no level to report or raise.
//!
//! The removed names live only in this file, which the release's "nothing
//! left" check allowlists.

use std::collections::HashMap;

use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;

/// The removed compare-and-set precondition on snapshot publish.
const HEADER_STREAM_SNAPSHOT_MATCH: &str = "stream-snapshot-match";

/// The removed compare-and-append precondition on JSON appends.
const HEADER_STREAM_RECORD_MATCH: &str = "stream-record-match";

/// The removed record-coordinate read parameters.
const RECORD_READ_PARAMETERS: [&str; 4] = ["record", "tail_records", "max_records", "record_view"];

/// Rejects a read that carries a removed record-coordinate parameter.
pub(crate) fn reject_removed_read_parameters(
    query: &HashMap<String, String>,
) -> Result<(), Box<Response>> {
    match RECORD_READ_PARAMETERS
        .iter()
        .find(|name| query.contains_key(**name))
    {
        Some(name) => Err(Box::new(
            (
                StatusCode::BAD_REQUEST,
                format!("the {name} parameter is not supported; record coordinates were removed"),
            )
                .into_response(),
        )),
        None => Ok(()),
    }
}

/// Rejects an append that carries the removed `Stream-Record-Match`.
pub(crate) fn reject_removed_append_headers(headers: &HeaderMap) -> Result<(), Box<Response>> {
    if headers.contains_key(HEADER_STREAM_RECORD_MATCH) {
        return Err(Box::new(
            (
                StatusCode::BAD_REQUEST,
                "Stream-Record-Match is not supported",
            )
                .into_response(),
        ));
    }
    Ok(())
}

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

        let target = format!("{uri}/snapshot/{at}");
        let response = send(&app, "PUT", &target, &header, "state").await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(body_text(response).await.contains("Stream-Snapshot-Match"));

        let head = send(&app, "HEAD", uri, &[], "").await;
        assert_eq!(head.status(), StatusCode::OK);
        assert!(!head.headers().contains_key(HEADER_STREAM_SNAPSHOT_OFFSET));
    }

    #[tokio::test]
    async fn record_coordinate_surface_answers_400_and_writes_nothing() {
        let app = app();
        let uri = "/removed/records";
        let json = [(CONTENT_TYPE.as_str(), "application/json")];
        let response = send(&app, "PUT", uri, &json, "{\"a\":1}\n").await;
        assert_eq!(response.status(), StatusCode::CREATED);

        for query in [
            "record=0",
            "record=now",
            "tail_records=1",
            "offset=-1&max_records=1",
            "offset=-1&record_view=envelope",
        ] {
            for live in ["", "&live=long-poll", "&live=sse"] {
                let target = format!("{uri}?{query}{live}");
                let response = send(&app, "GET", &target, &[], "").await;
                assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{target}");
            }
        }

        let matched = [
            (CONTENT_TYPE.as_str(), "application/json"),
            (HEADER_STREAM_RECORD_MATCH, "1"),
        ];
        let response = send(&app, "POST", uri, &matched, "{\"b\":2}\n").await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(body_text(response).await.contains("Stream-Record-Match"));
        let head = send(&app, "HEAD", uri, &[], "").await;
        assert_eq!(
            head.headers()
                .get(HEADER_STREAM_NEXT_OFFSET)
                .expect("next offset"),
            &format!("{:020}", 8)
        );
        assert!(!head.headers().contains_key("stream-extensions"));

        for target in [
            format!("{uri}/snapshot?record=0"),
            format!("{uri}/retention?record=0"),
        ] {
            let response = send(&app, "PUT", &target, &[], "state").await;
            assert!(
                matches!(
                    response.status(),
                    StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED
                ),
                "{target}: {}",
                response.status()
            );
        }
        let response = send(&app, "GET", &format!("{uri}/snapshot"), &[], "").await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
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
        // Production served this path only on the admin listener.
        let admin = crate::admin_router(crate::HttpState::new(
            ShardRuntime::spawn(RuntimeConfig::new(1, 1)).expect("runtime"),
        ));
        let json = [(CONTENT_TYPE.as_str(), "application/json")];
        let response = send(&admin, "GET", "/__ursula/feature-level", &[], "").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let response = send(
            &admin,
            "POST",
            "/__ursula/feature-level",
            &json,
            r#"{"level":1}"#,
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        // The merged router routes it as a stream in the reserved `__ursula` bucket.
        let response = send(&app(), "GET", "/__ursula/feature-level", &[], "").await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
