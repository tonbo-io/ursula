//! Metadata a router adds to its answers (extensions Section 10): the
//! server's identity (`Ursula-Protocol` and `Ursula-Cluster-Id`) on every
//! answer to a `HEAD`, so a client tells a host alias routed to another
//! deployment from a deleted stream, and the request limit on any `413`
//! that names no limit. On the client plane it also gives an error body
//! (extensions Section 11) to an error answer that no handler rendered,
//! outside the operator paths under `/__ursula/`.
//!
//! One tower layer does both. It calls the inner service directly, so it
//! adds no clone of the routes to a request, as a `from_fn` middleware
//! would.

use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use axum::body::Body;
use axum::http::HeaderName;
use axum::http::HeaderValue;
use axum::http::Method;
use axum::http::Request;
use axum::response::Response;
use futures_util::future::BoxFuture;
use tower::Layer;
use tower::Service;
use ursula_headers::ClusterId;
use ursula_headers::ServerIdentity;

use crate::limits;
use crate::render;

/// The identity headers, rendered once through `ursula-headers` when the
/// router is built.
#[derive(Clone)]
pub(crate) struct IdentityHeaders(Arc<[(HeaderName, HeaderValue)]>);

impl IdentityHeaders {
    pub(crate) fn new(cluster_id: &ClusterId) -> Self {
        let mut headers = Vec::new();
        ServerIdentity::current(cluster_id.clone()).render(|name, value| {
            if let Ok(value) = HeaderValue::from_str(value) {
                headers.push((HeaderName::from_static(name), value));
            }
        });
        Self(headers.into())
    }
}

/// Adds the router's answer metadata. `identity` is sent on every answer to
/// a `HEAD`, when the router serves streams: the client plane, whose error
/// answers all carry an error body.
#[derive(Clone)]
pub(crate) struct ResponseMetadataLayer {
    identity: Option<IdentityHeaders>,
}

impl ResponseMetadataLayer {
    pub(crate) fn new(identity: Option<IdentityHeaders>) -> Self {
        Self { identity }
    }
}

/// The prefix of the operator paths, which keep their own answers.
const OPERATOR_PATH_PREFIX: &str = "/__ursula/";

impl<S> Layer<S> for ResponseMetadataLayer {
    type Service = ResponseMetadata<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ResponseMetadata {
            inner,
            identity: self.identity.clone(),
        }
    }
}

/// The service of [`ResponseMetadataLayer`].
#[derive(Clone)]
pub(crate) struct ResponseMetadata<S> {
    inner: S,
    identity: Option<IdentityHeaders>,
}

impl<S> Service<Request<Body>> for ResponseMetadata<S>
where
    S: Service<Request<Body>, Response = Response>,
    S::Future: Send + 'static,
{
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Response, S::Error>>;
    type Response = Response;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let identity = if request.method() == Method::HEAD {
            self.identity.clone()
        } else {
            None
        };
        let error_bodies =
            self.identity.is_some() && !request.uri().path().starts_with(OPERATOR_PATH_PREFIX);
        let future = self.inner.call(request);
        Box::pin(async move {
            let mut response = future.await?;
            if let Some(identity) = identity {
                let headers = response.headers_mut();
                for (name, value) in identity.0.iter() {
                    headers.insert(name.clone(), value.clone());
                }
            }
            limits::name_unnamed_size_refusal(&mut response);
            if error_bodies {
                render::render_unrendered_error(&mut response);
            }
            Ok(response)
        })
    }
}
