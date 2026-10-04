//! Request surface removed in 0.6.0 that must fail loudly instead of being
//! ignored.
//!
//! - `Stream-Snapshot-Match` answers 400. Ignoring it would turn a
//!   conditional snapshot publish into an unconditional one.
//! - A bare `GET {stream}/snapshot` (the old latest-snapshot redirect) answers
//!   405, not 404: Loro's streams client reads a 404 as "no snapshot". Clients
//!   read the latest snapshot's offset from HEAD and fetch
//!   `GET {stream}/snapshot/{offset}`.
//!
//! The removed names live only in this file, which the release's "nothing
//! left" check allowlists.

use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::http::header::ALLOW;
use axum::response::IntoResponse;
use axum::response::Response;

use crate::render::insert_default_response_headers;

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

/// `GET {stream}/snapshot`: the latest-snapshot redirect was removed. The
/// path still takes `PUT` (publish at a record), so `Allow` names it.
pub(crate) async fn latest_snapshot_removed() -> Response {
    let mut headers = HeaderMap::new();
    insert_default_response_headers(&mut headers);
    headers.insert(ALLOW, HeaderValue::from_static("PUT"));
    (StatusCode::METHOD_NOT_ALLOWED, headers).into_response()
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

    const HEADER_STREAM_NEXT_OFFSET: &str = "stream-next-offset";
    const HEADER_STREAM_SNAPSHOT_OFFSET: &str = "stream-snapshot-offset";

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
    async fn bare_snapshot_get_and_snapshot_delete_answer_405() {
        let app = app();
        let uri = "/removed/latest-snapshot";
        let at = stream_with_bytes(&app, uri).await;
        let response = send(&app, "PUT", &format!("{uri}/snapshot/{at}"), &[], "state").await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let response = send(&app, "GET", &format!("{uri}/snapshot"), &[], "").await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(
            response.headers().get(ALLOW).map(HeaderValue::as_bytes),
            Some(b"PUT".as_slice())
        );

        let response = send(&app, "DELETE", &format!("{uri}/snapshot/{at}"), &[], "").await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        let response = send(&app, "GET", &format!("{uri}/snapshot/{at}"), &[], "").await;
        assert_eq!(response.status(), StatusCode::OK);
    }
}
