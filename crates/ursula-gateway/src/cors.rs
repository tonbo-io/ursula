//! Cross-origin access for browser clients.
//!
//! `public_read` makes anonymous reads a first-class feature, and a browser is
//! the client that feature exists for: without CORS the origin is unreachable
//! from JavaScript, so the feature is only nominally public. Which origins are
//! allowed is deployment policy, so this is configuration and never a default.
//!
//! Two rules carry the security weight.
//!
//! **Preflight never consults the resource.** `OPTIONS` arrives without
//! `Authorization`, so answering it per bucket would tell an unauthenticated
//! caller whether a private bucket exists, reintroducing the existence oracle
//! that concealed 404s were built to close. The reply below is identical for
//! every path, and the caller reaches it before authorization runs.
//!
//! **Credentials are never allowed.** Ursula authenticates from an explicit
//! `Authorization` header the caller sets; CORS does not classify that as
//! credentials, and cookies are not used. Leaving `Allow-Credentials` unset also
//! keeps `Expose-Headers: *` effective — which is what lets a browser read the
//! twenty-odd `stream-*` protocol headers (`stream-next-offset`,
//! `stream-record-next`, `stream-cursor`, integrity setsums) without enumerating
//! a list here that would silently drift as the protocol grows. A browser that
//! cannot read those headers cannot paginate, so the wildcard is load-bearing
//! rather than a shortcut.

use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::Method;
use axum::http::StatusCode;
use axum::http::header::VARY;
use axum::response::IntoResponse;
use axum::response::Response;

const ALLOW_ORIGIN: &str = "access-control-allow-origin";
const ALLOW_METHODS: &str = "access-control-allow-methods";
const ALLOW_HEADERS: &str = "access-control-allow-headers";
const EXPOSE_HEADERS: &str = "access-control-expose-headers";
const MAX_AGE: &str = "access-control-max-age";
const REQUEST_METHOD: &str = "access-control-request-method";
const REQUEST_HEADERS: &str = "access-control-request-headers";

/// Methods the data plane accepts. Sent verbatim rather than reflecting the
/// requested method, so a preflight cannot probe which verbs a path supports.
const ALLOWED_METHODS: &str = "GET, HEAD, POST, PUT, DELETE";
const PREFLIGHT_MAX_AGE_SECS: &str = "600";
const ANY_ORIGIN: &str = "*";

#[derive(Debug, Clone)]
pub struct CorsPolicy {
    /// Exact origins, or a single `*`.
    allowed: Vec<String>,
    any: bool,
}

impl CorsPolicy {
    /// `None` when no origin is configured, which leaves responses byte-identical
    /// to a deployment without CORS.
    pub fn new(allowed: Vec<String>) -> Option<Self> {
        if allowed.is_empty() {
            return None;
        }
        let any = allowed.iter().any(|origin| origin == ANY_ORIGIN);
        Some(Self { allowed, any })
    }

    /// The value to echo, or `None` when the origin is not allowed.
    ///
    /// A rejected origin is answered without CORS headers rather than with an
    /// error: the browser then blocks the read, and a non-browser caller sees
    /// exactly the response it would have seen anyway.
    fn allow_origin(&self, origin: &str) -> Option<&str> {
        if self.any {
            // Echoing `*` rather than the request origin keeps the response
            // cacheable and independent of who asked.
            return Some(ANY_ORIGIN);
        }
        self.allowed
            .iter()
            .find(|allowed| *allowed == origin)
            .map(String::as_str)
    }

    pub fn is_preflight(method: &Method, headers: &HeaderMap) -> bool {
        method == Method::OPTIONS && headers.contains_key(REQUEST_METHOD)
    }

    /// Identical for every path by construction — see the module note.
    pub fn preflight_response(&self, headers: &HeaderMap) -> Option<Response> {
        let origin = headers.get(axum::http::header::ORIGIN)?.to_str().ok()?;
        let allow = self.allow_origin(origin)?.to_owned();

        let mut response = StatusCode::NO_CONTENT.into_response();
        let out = response.headers_mut();
        insert(out, ALLOW_ORIGIN, &allow);
        insert(out, ALLOW_METHODS, ALLOWED_METHODS);
        insert(out, MAX_AGE, PREFLIGHT_MAX_AGE_SECS);
        // Reflected rather than enumerated: conditional reads send `if-none-match`,
        // writes send `content-type`, and every authenticated call sends
        // `authorization`. A fixed list would drift; reflection cannot, and with
        // credentials disallowed it grants nothing the caller did not already ask
        // for.
        if let Some(requested) = headers.get(REQUEST_HEADERS)
            && let Ok(requested) = requested.to_str()
        {
            insert(out, ALLOW_HEADERS, requested);
        }
        if !self.any {
            insert_vary_origin(out);
        }
        Some(response)
    }

    /// Add cross-origin headers to an already-produced response.
    pub fn decorate(&self, response_headers: &mut HeaderMap, request_headers: &HeaderMap) {
        let Some(origin) = request_headers
            .get(axum::http::header::ORIGIN)
            .and_then(|origin| origin.to_str().ok())
        else {
            return;
        };
        let Some(allow) = self.allow_origin(origin).map(str::to_owned) else {
            return;
        };
        insert(response_headers, ALLOW_ORIGIN, &allow);
        insert(response_headers, EXPOSE_HEADERS, ANY_ORIGIN);
        if !self.any {
            insert_vary_origin(response_headers);
        }
    }
}

fn insert(headers: &mut HeaderMap, name: &'static str, value: &str) {
    if let Ok(value) = HeaderValue::from_str(value) {
        headers.insert(name, value);
    }
}

/// Appends rather than replaces: the response may already vary on something else,
/// and dropping that would let a shared cache serve the wrong variant.
fn insert_vary_origin(headers: &mut HeaderMap) {
    let origin = HeaderValue::from_static("origin");
    if headers.get_all(VARY).iter().any(|value| {
        value
            .to_str()
            .is_ok_and(|value| value.eq_ignore_ascii_case("origin"))
    }) {
        return;
    }
    headers.append(VARY, origin);
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderMap;
    use axum::http::HeaderValue;
    use axum::http::header::ORIGIN;
    use axum::http::header::VARY;

    use super::CorsPolicy;

    fn request_from(origin: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(ORIGIN, HeaderValue::from_str(origin).unwrap());
        headers
    }

    #[test]
    fn an_allowed_origin_is_echoed_with_a_vary_and_the_expose_wildcard() {
        let policy = CorsPolicy::new(vec!["https://app.example".to_owned()]).unwrap();
        let mut response = HeaderMap::new();

        policy.decorate(&mut response, &request_from("https://app.example"));

        assert_eq!(
            response.get(super::ALLOW_ORIGIN).unwrap(),
            "https://app.example"
        );
        assert_eq!(response.get(super::EXPOSE_HEADERS).unwrap(), "*");
        assert_eq!(response.get(VARY).unwrap(), "origin");
    }

    #[test]
    fn an_unlisted_origin_is_left_untouched() {
        let policy = CorsPolicy::new(vec!["https://app.example".to_owned()]).unwrap();
        let mut response = HeaderMap::new();

        policy.decorate(&mut response, &request_from("https://evil.example"));

        assert!(response.is_empty());
    }

    /// `*` does not vary by caller, so advertising `Vary: origin` would only cost
    /// shared caches a hit per origin.
    #[test]
    fn the_wildcard_does_not_vary_by_origin() {
        let policy = CorsPolicy::new(vec!["*".to_owned()]).unwrap();
        let mut response = HeaderMap::new();

        policy.decorate(&mut response, &request_from("https://any.example"));

        assert_eq!(response.get(super::ALLOW_ORIGIN).unwrap(), "*");
        assert!(response.get(VARY).is_none());
    }

    /// Replacing `Vary` instead of appending would let a shared cache serve a
    /// response built for a different variant.
    #[test]
    fn vary_is_appended_to_an_existing_value() {
        let policy = CorsPolicy::new(vec!["https://app.example".to_owned()]).unwrap();
        let mut response = HeaderMap::new();
        response.insert(VARY, HeaderValue::from_static("accept-encoding"));

        policy.decorate(&mut response, &request_from("https://app.example"));

        let values: Vec<_> = response
            .get_all(VARY)
            .iter()
            .map(|value| value.to_str().unwrap())
            .collect();
        assert!(values.contains(&"accept-encoding"));
        assert!(values.contains(&"origin"));
    }

    #[test]
    fn no_configured_origin_disables_the_policy_entirely() {
        assert!(CorsPolicy::new(Vec::new()).is_none());
    }
}
