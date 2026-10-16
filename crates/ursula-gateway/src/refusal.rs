//! Refusals of a program that embeds the gateway: a request it refuses
//! before the gateway forwards it, such as one of an unpaid account.
//!
//! [`Gateway::refuse`] answers such a request as the gateway answers its own
//! refusals. It reads and drops the body first (see the README's Refusals
//! Before the Body), then renders the one error answer of the gateway: the
//! JSON error body (extensions Section 11) with the refusal's typed code,
//! `Cache-Control: no-store`, and no server identity and no producer header.
//! A client's `ursula_headers::classify` reads every such answer as
//! `Refused`, so it knows the write applied nothing. A node's producer `403`
//! carries `Producer-Epoch` and `Producer-Outcome`, which an embedder's
//! `403` never does, so the two are told apart.
//!
//! The codes are typed and fixed ([`EmbedderRefusal`]): an embedder chooses
//! among them and cannot send a code that a client does not know.

use std::sync::atomic::Ordering;

use axum::body::Body;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::header::RETRY_AFTER;
use axum::response::Response as AxumResponse;
use ursula_headers::ErrorBody;
use ursula_headers::ErrorCode;

use crate::Gateway;
use crate::REFUSED_PACE;
use crate::Refusal;
use crate::error_answer;

/// A refusal an embedder sends before the gateway forwards the request.
/// Each applies nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbedderRefusal {
    /// `402 PAYMENT_REQUIRED`: the account must pay first.
    PaymentRequired,
    /// `403 ACCOUNT_REVOKED`: the account's access is revoked.
    AccountRevoked,
    /// `403 FORBIDDEN`: the embedder does not allow the request.
    Forbidden,
    /// `429 RATE_LIMITED` with `Retry-After`: a limit of the embedder's own.
    RateLimited { retry_after_secs: u64 },
}

impl EmbedderRefusal {
    pub fn status(self) -> StatusCode {
        match self {
            Self::PaymentRequired => StatusCode::PAYMENT_REQUIRED,
            Self::AccountRevoked | Self::Forbidden => StatusCode::FORBIDDEN,
            Self::RateLimited { .. } => StatusCode::TOO_MANY_REQUESTS,
        }
    }

    pub fn code(self) -> ErrorCode {
        match self {
            Self::PaymentRequired => ErrorCode::PaymentRequired,
            Self::AccountRevoked => ErrorCode::AccountRevoked,
            Self::Forbidden => ErrorCode::Forbidden,
            Self::RateLimited { .. } => ErrorCode::RateLimited,
        }
    }
}

impl EmbedderRefusal {
    /// The answer alone, without reading the body. Only [`Gateway::refuse`]
    /// sends it, after it read the body: a client that sends its body
    /// before it reads the answer would otherwise meet a reset, which says
    /// nothing about the write.
    pub(crate) fn into_response(self) -> AxumResponse {
        let mut headers = HeaderMap::new();
        if let Self::RateLimited { retry_after_secs } = self {
            headers.insert(RETRY_AFTER, HeaderValue::from(retry_after_secs));
        }
        error_answer(self.status(), ErrorBody::fixed(self.code()), headers)
    }
}

impl Gateway {
    /// Refuses `req` before forwarding it: reads and drops its body as the
    /// gateway does for its own refusals from the head, then answers
    /// `refusal`, with the CORS headers the gateway adds to every answer. A
    /// CORS preflight gets its preflight answer, as from
    /// [`Gateway::handle`], since it carries no credential to refuse. The
    /// future takes as long as the reading does, as [`Gateway::handle`]'s
    /// refusals do.
    pub async fn refuse(&self, req: Request<Body>, refusal: EmbedderRefusal) -> AxumResponse {
        let (parts, body) = req.into_parts();
        if let Some(policy) = &self.cors
            && crate::cors::CorsPolicy::is_preflight(&parts.method, &parts.headers)
            && let Some(response) = policy.preflight_response(&parts.headers)
        {
            return response;
        }
        self.metrics.requests.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .embedder_refusals
            .fetch_add(1, Ordering::Relaxed);
        let mut response = self
            .refuse_unread(Refusal::Embedder(refusal), body, REFUSED_PACE)
            .await;
        if let Some(policy) = &self.cors {
            policy.decorate(response.headers_mut(), &parts.headers);
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::time::Duration;

    use axum::body::Body;
    use axum::http::Request;
    use axum::http::StatusCode;
    use ursula_headers::ErrorBody;
    use ursula_headers::ErrorCode;

    use super::EmbedderRefusal;
    use crate::Gateway;
    use crate::GatewayConfig;

    const ORIGIN: &str = "https://app.example";

    fn gateway() -> Gateway {
        Gateway::new(GatewayConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            upstreams: vec!["http://127.0.0.1:9".to_owned()],
            response_header_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(1),
            upstream_tcp_user_timeout_secs: NonZeroU64::new(5).unwrap(),
            uploads: crate::uploads::UploadLimits::default(),
            raft_group_count: None,
            cors_allowed_origins: vec![ORIGIN.to_owned()],
            usage_chunk_bytes: None,
        })
    }

    /// The refusal reads the body, answers with the code, CORS and no
    /// identity, and counts the request.
    #[tokio::test]
    async fn a_refusal_answers_with_its_code_and_cors_after_the_body() {
        let gateway = gateway();
        let response = gateway
            .refuse(
                Request::post("/bucket/stream")
                    .header("origin", ORIGIN)
                    .body(Body::from(vec![0_u8; 4096]))
                    .unwrap(),
                EmbedderRefusal::PaymentRequired,
            )
            .await;
        assert_eq!(response.status(), StatusCode::PAYMENT_REQUIRED);
        assert_eq!(
            response.headers()["access-control-allow-origin"],
            ORIGIN,
            "CORS"
        );
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert!(!response.headers().contains_key("ursula-protocol"));
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(
            ErrorBody::parse(&body).unwrap().code,
            ErrorCode::PaymentRequired
        );
        let metrics = gateway.metrics_snapshot();
        assert_eq!((metrics.requests, metrics.embedder_refusals), (1, 1));
        assert_eq!(metrics.refusal_drains, 1, "the body was read first");
    }

    /// A CORS preflight gets its preflight answer, not the refusal.
    #[tokio::test]
    async fn a_preflight_is_answered_as_a_preflight() {
        let response = gateway()
            .refuse(
                Request::options("/bucket/stream")
                    .header("origin", ORIGIN)
                    .header("access-control-request-method", "POST")
                    .body(Body::empty())
                    .unwrap(),
                EmbedderRefusal::AccountRevoked,
            )
            .await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(response.headers()["access-control-allow-origin"], ORIGIN);
    }
}
