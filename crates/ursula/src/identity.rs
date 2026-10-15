//! The server's identity on every HEAD answer (extensions Section 10):
//! `Ursula-Protocol` and `Ursula-Cluster-Id`, so a client tells a host alias
//! routed to another deployment from a deleted stream.

use axum::body::Body;
use axum::extract::State;
use axum::http::Method;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::Response;
use ursula_headers::ServerIdentity;

use crate::HttpState;
use crate::render::insert_header_str;

/// Adds the server's identity to every answer to a `HEAD`: a stream's
/// metadata, a `404`, a redirect and a `503` alike.
pub(crate) async fn identify_head_answers(
    State(state): State<HttpState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let head = request.method() == Method::HEAD;
    let mut response = next.run(request).await;
    if head {
        let headers = response.headers_mut();
        ServerIdentity::current(state.cluster_id().clone())
            .render(|name, value| insert_header_str(headers, name, value));
    }
    response
}
