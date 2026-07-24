//! Ursula Gateway core.
//!
//! Routes client HTTP requests to upstream Ursula nodes. Redirect responses stay
//! visible to clients, but upstream node addresses are stripped from Location so
//! redirect-following clients keep talking to the gateway.
//!
//! Raft leadership redirects (marked by `x-ursula-raft-leader-id`) are followed
//! internally by the gateway after buffering the request body. This avoids
//! leaking internal node addresses to clients and prevents SSE live reads from
//! looping when the initial random upstream lands on a follower.
//!
//! Module map:
//!
//! - [`auth`]: opt-in, provider-neutral authentication and authorization hooks.
//! - [`service`]: command arguments and the long-running gateway service entrypoint.

use std::error::Error as _;
use std::net::SocketAddr;
use std::time::Duration;

use axum::body::Body;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::Method;
use axum::http::Request;
use axum::http::Response;
use axum::http::StatusCode;
use axum::http::Uri;
use axum::http::header::AUTHORIZATION;
use axum::http::header::CONNECTION;
use axum::http::header::LOCATION;
use axum::http::header::WWW_AUTHENTICATE;
use axum::response::IntoResponse;
use axum::response::Response as AxumResponse;
use percent_encoding::percent_decode_str;
use rand::prelude::IndexedRandom;
use tracing::debug;
use tracing::error;

pub mod auth;
pub mod service;

use crate::auth::AccessControl;
use crate::auth::Action;
use crate::auth::AuthenticationError;
use crate::auth::AuthorizationDecision;
use crate::auth::AuthorizationRequest;
use crate::auth::Resource;

const HEADER_URSULA_RAFT_LEADER_ID: &str = "x-ursula-raft-leader-id";
const HEADER_STREAM_CLOSED: &str = "stream-closed";
pub const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct GatewayConfig {
    pub listen: SocketAddr,
    pub upstreams: Vec<String>,
    /// Covers only response headers so SSE bodies stay open.
    pub response_header_timeout: Duration,
    pub connect_timeout: Duration,
    pub max_request_body_bytes: usize,
}

#[derive(Clone)]
pub struct Gateway {
    config: GatewayConfig,
    client: reqwest::Client,
    response_header_timeout: Duration,
    access_control: Option<AccessControl>,
}

impl std::fmt::Debug for Gateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gateway")
            .field("config", &self.config)
            .field("response_header_timeout", &self.response_header_timeout)
            .field("access_control", &self.access_control)
            .finish_non_exhaustive()
    }
}

impl Gateway {
    pub fn new(config: GatewayConfig) -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("static gateway reqwest client config should be valid");
        let response_header_timeout = config.response_header_timeout;
        Self {
            config,
            client,
            response_header_timeout,
            access_control: None,
        }
    }

    /// Installs provider-neutral authentication and authorization hooks.
    ///
    /// [`Gateway::new`] intentionally leaves access control disabled so
    /// self-hosted deployments retain the original pass-through behavior.
    pub fn with_access_control(config: GatewayConfig, access_control: AccessControl) -> Self {
        Self {
            access_control: Some(access_control),
            ..Self::new(config)
        }
    }

    pub async fn handle(&self, req: Request<Body>) -> AxumResponse {
        let (mut parts, body) = req.into_parts();

        if let Some(response) = self.authorize(&parts).await {
            return response;
        }
        if self.access_control.is_some() {
            // The credential terminates at the gateway. Internal Ursula nodes
            // do not need the user's bearer token.
            parts.headers.remove(AUTHORIZATION);
        }

        let body_bytes = match axum::body::to_bytes(body, self.config.max_request_body_bytes).await
        {
            Ok(b) => b,
            Err(err) => {
                if err
                    .source()
                    .is_some_and(|source| source.is::<http_body_util::LengthLimitError>())
                {
                    return StatusCode::PAYLOAD_TOO_LARGE.into_response();
                } else {
                    return StatusCode::BAD_REQUEST.into_response();
                }
            }
        };

        let upstream = match self.pick_upstream() {
            Some(u) => u,
            None => {
                error!("no upstreams configured");
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
        };

        match self.forward(upstream, &parts, body_bytes).await {
            Ok(response) => response,
            Err(e) => {
                error!(error = %e, "gateway request failed");
                e.into_response()
            }
        }
    }

    async fn authorize(&self, parts: &axum::http::request::Parts) -> Option<AxumResponse> {
        let access_control = self.access_control.as_ref()?;
        let Some(request) = classify_request(&parts.method, &parts.uri, &parts.headers) else {
            // An access-controlled gateway is a public resource server, not a
            // transparent escape hatch to internal or newly added routes.
            return Some(StatusCode::NOT_FOUND.into_response());
        };

        let principal = match bearer_token(&parts.headers) {
            Ok(Some(token)) => match access_control.principal_resolver().resolve(token).await {
                Ok(principal) => Some(principal),
                Err(error) => return Some(authentication_error_response(error)),
            },
            Ok(None) => None,
            Err(()) => {
                return Some(authentication_error_response(
                    AuthenticationError::InvalidCredential,
                ));
            }
        };

        match access_control
            .authorizer()
            .authorize(AuthorizationRequest {
                principal,
                resource: request.resource,
                action: request.action,
            })
            .await
        {
            Ok(AuthorizationDecision::Allow) => None,
            Ok(AuthorizationDecision::Deny) => Some(StatusCode::FORBIDDEN.into_response()),
            Ok(AuthorizationDecision::ConcealAsNotFound) => {
                Some(StatusCode::NOT_FOUND.into_response())
            }
            Err(error) => {
                error!(error = %error, "gateway authorization failed");
                Some(StatusCode::SERVICE_UNAVAILABLE.into_response())
            }
        }
    }

    fn pick_upstream(&self) -> Option<&str> {
        let mut rng = rand::rng();
        self.config.upstreams.choose(&mut rng).map(String::as_str)
    }

    async fn forward(
        &self,
        upstream: &str,
        parts: &axum::http::request::Parts,
        body: bytes::Bytes,
    ) -> Result<Response<Body>, GatewayError> {
        let path_and_query = parts
            .uri
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("/");
        let target_url = format!("{}{}", upstream.trim_end_matches('/'), path_and_query);

        let upstream_resp = self.send_request(&target_url, parts, body.clone()).await?;

        // Raft leadership redirect: follow internally for all methods because
        // the body has been buffered and the client cannot do better than
        // another random hop through the gateway.
        if upstream_resp.status() == StatusCode::TEMPORARY_REDIRECT
            && upstream_resp
                .headers()
                .contains_key(HEADER_URSULA_RAFT_LEADER_ID)
        {
            let response_headers = copy_forwarded_headers(upstream_resp.headers(), false);
            if let Some(leader_upstream) = self.resolve_leader_upstream(&response_headers) {
                // Drop the follower response; it has no meaningful body.
                drop(upstream_resp);

                let leader_target = format!(
                    "{}{}",
                    leader_upstream.trim_end_matches('/'),
                    path_and_query
                );
                let leader_resp = self.send_request(&leader_target, parts, body).await?;
                return Self::build_response(leader_resp);
            }
        }

        Self::build_response(upstream_resp)
    }

    /// Covers only response headers so SSE bodies stay open.
    async fn send_request(
        &self,
        url: &str,
        parts: &axum::http::request::Parts,
        body: bytes::Bytes,
    ) -> Result<reqwest::Response, GatewayError> {
        debug!(method = %parts.method, url = %url, "sending upstream request");

        tokio::time::timeout(
            self.response_header_timeout,
            self.client
                .request(parts.method.clone(), url)
                .headers(copy_forwarded_headers(&parts.headers, true))
                .body(body)
                .send(),
        )
        .await
        .map_err(|e| GatewayError::Upstream(format!("upstream response header timeout: {e}")))?
        .map_err(|e| GatewayError::Upstream(e.to_string()))
    }

    fn build_response(upstream_resp: reqwest::Response) -> Result<Response<Body>, GatewayError> {
        let status = upstream_resp.status();
        let mut headers = copy_forwarded_headers(upstream_resp.headers(), false);

        if status == StatusCode::TEMPORARY_REDIRECT {
            // Keep redirects client-facing: strip internal host so the client
            // reconnects through the gateway, not directly to the Ursula node.
            headers.remove(HEADER_URSULA_RAFT_LEADER_ID);
            if let Some(location) = headers
                .get(LOCATION)
                .and_then(|value| value.to_str().ok())
                .and_then(|location| location.parse::<Uri>().ok())
                .and_then(|uri| uri.path_and_query().map(|pq| pq.as_str().to_owned()))
                .and_then(|path| HeaderValue::from_str(&path).ok())
            {
                headers.insert(LOCATION, location);
            }
        }

        let mut response = Response::builder().status(status);
        if let Some(response_headers) = response.headers_mut() {
            *response_headers = headers;
        }
        response
            .body(Body::from_stream(upstream_resp.bytes_stream()))
            .map_err(|e| GatewayError::ResponseBuild(e.to_string()))
    }

    fn resolve_leader_upstream(&self, response_headers: &HeaderMap) -> Option<&str> {
        let location = response_headers.get(LOCATION)?.to_str().ok()?;
        self.config
            .upstreams
            .iter()
            .find(|u| location.starts_with(*u))
            .map(String::as_str)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ClassifiedRequest {
    resource: Resource,
    action: Action,
}

fn classify_request(method: &Method, uri: &Uri, headers: &HeaderMap) -> Option<ClassifiedRequest> {
    let segments = uri
        .path()
        .strip_prefix('/')?
        .split('/')
        .map(decode_path_segment)
        .collect::<Option<Vec<_>>>()?;
    let bucket_id = segments.first()?.clone();
    if bucket_id.is_empty() || bucket_id == "__ursula" {
        return None;
    }

    let (stream_id, action) = match segments.as_slice() {
        [_bucket] if *method == Method::PUT => (None, Action::AdministerBucket),
        [_bucket, stream] if *method == Method::PUT => {
            let action = if closes_stream(headers) {
                Action::CreateAndClose
            } else {
                Action::Create
            };
            (Some(stream.clone()), action)
        }
        [_bucket, stream] if *method == Method::POST => {
            let action = if closes_stream(headers) {
                Action::AppendAndClose
            } else {
                Action::Append
            };
            (Some(stream.clone()), action)
        }
        [_bucket, stream] if *method == Method::GET => {
            let action = if query_has_pair(uri, "live", "sse") {
                Action::Tail
            } else {
                Action::Read
            };
            (Some(stream.clone()), action)
        }
        [_bucket, stream] if *method == Method::HEAD => (Some(stream.clone()), Action::Head),
        [_bucket, stream] if *method == Method::DELETE => (Some(stream.clone()), Action::Delete),
        [_bucket, stream, suffix] if suffix == "attrs" && *method == Method::PUT => {
            (Some(stream.clone()), Action::Update)
        }
        [_bucket, stream, suffix] if suffix == "attrs" && *method == Method::GET => {
            (Some(stream.clone()), Action::Head)
        }
        [_bucket, stream, suffix] if suffix == "bootstrap" && *method == Method::GET => {
            (Some(stream.clone()), Action::Read)
        }
        [_bucket, stream, suffix] if suffix == "append-batch" && *method == Method::POST => {
            (Some(stream.clone()), Action::Append)
        }
        [_bucket, stream, suffix] if suffix == "snapshot" && *method == Method::GET => {
            (Some(stream.clone()), Action::ReadSnapshot)
        }
        [_bucket, stream, suffix, _offset] if suffix == "snapshot" && *method == Method::PUT => {
            (Some(stream.clone()), Action::PublishSnapshot)
        }
        [_bucket, stream, suffix, _offset] if suffix == "snapshot" && *method == Method::GET => {
            (Some(stream.clone()), Action::ReadSnapshot)
        }
        [_bucket, stream, suffix, _offset] if suffix == "snapshot" && *method == Method::DELETE => {
            (Some(stream.clone()), Action::DeleteSnapshot)
        }
        _ => return None,
    };

    Some(ClassifiedRequest {
        resource: Resource {
            bucket_id,
            stream_id,
        },
        action,
    })
}

fn closes_stream(headers: &HeaderMap) -> bool {
    headers
        .get(HEADER_STREAM_CLOSED)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

fn decode_path_segment(segment: &str) -> Option<String> {
    percent_decode_str(segment)
        .decode_utf8()
        .ok()
        .map(|decoded| decoded.into_owned())
}

fn query_has_pair(uri: &Uri, expected_name: &str, expected_value: &str) -> bool {
    uri.query().is_some_and(|query| {
        query.split('&').any(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            decode_path_segment(name).as_deref() == Some(expected_name)
                && decode_path_segment(value).as_deref() == Some(expected_value)
        })
    })
}

fn bearer_token(headers: &HeaderMap) -> Result<Option<&str>, ()> {
    let mut values = headers.get_all(AUTHORIZATION).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(());
    }
    let value = value.to_str().map_err(|_invalid_header| ())?;
    let (scheme, token) = value.split_once(' ').ok_or(())?;
    if !scheme.eq_ignore_ascii_case("bearer")
        || token.is_empty()
        || token.bytes().any(|byte| byte.is_ascii_whitespace())
    {
        return Err(());
    }
    Ok(Some(token))
}

fn authentication_error_response(error: AuthenticationError) -> AxumResponse {
    if error == AuthenticationError::Unavailable {
        error!(error = %error, "gateway authentication failed");
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }

    let mut response = StatusCode::UNAUTHORIZED.into_response();
    response.headers_mut().insert(
        WWW_AUTHENTICATE,
        HeaderValue::from_static(r#"Bearer error="invalid_token""#),
    );
    response
}

// Copy end-to-end headers while removing hop-by-hop proxy headers. Hop-by-hop
// headers describe only the current TCP/HTTP connection, so the gateway must
// let its HTTP client/server stack recreate them for the next connection.
//
// Request forwarding also drops Host so the client can set it from the upstream
// URL. Use append, not insert, so repeated headers such as Set-Cookie survive.
fn copy_forwarded_headers(src: &HeaderMap, drop_host: bool) -> HeaderMap {
    let mut dst = HeaderMap::new();
    let connection_tokens = connection_header_tokens(src);
    for (key, value) in src {
        let key_str = key.as_str();
        if (drop_host && key_str == "host") || is_hop_by_hop_header(key_str, &connection_tokens) {
            continue;
        }
        dst.append(key.clone(), value.clone());
    }
    dst
}

// RFC 9110 lets Connection nominate additional hop-by-hop fields:
//
//     Connection: x-local
//     X-Local: value
//
// In that case X-Local is also scoped to this one connection and must not be
// forwarded. Normalize tokens once so later comparisons are cheap and
// case-insensitive.
fn connection_header_tokens(headers: &HeaderMap) -> Vec<String> {
    headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

fn is_hop_by_hop_header(key: &str, connection_tokens: &[String]) -> bool {
    // The fixed names below are standard hop-by-hop headers. The dynamic
    // Connection tokens cover extension headers named by the sender.
    matches!(
        key,
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "trailers"
            | "transfer-encoding"
            | "upgrade"
    ) || connection_tokens
        .iter()
        .any(|token| token.eq_ignore_ascii_case(key))
}

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("upstream request failed: {0}")]
    Upstream(String),
    #[error("failed to build response: {0}")]
    ResponseBuild(String),
}

impl GatewayError {
    fn into_response(self) -> AxumResponse {
        match self {
            Self::Upstream(_) | Self::ResponseBuild(_) => StatusCode::BAD_GATEWAY.into_response(),
        }
    }
}

#[cfg(test)]
mod tests;
