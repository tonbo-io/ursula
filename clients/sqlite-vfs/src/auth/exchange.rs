//! One exchange of an API key for a bearer token: a `POST` to the token endpoint with the key as
//! its bearer token and `{"audience": ...}` (or `{}` without an audience) as its body, answered
//! with `{"access_token": ..., "expires_in": <seconds>}` and any other members, which are skipped.
//! It goes out on the agent of appends and reads (`transport::http::agent`), so it trusts the same
//! roots (`URSULA_VFS_CA_FILE`), takes the same proxy and follows no redirect.
//!
//! Neither the key nor a token is ever printed: [`Secret`] and [`Exchanged`] print redacted, and
//! no error carries either.

use std::fmt;
use std::time::Duration;

use ureq::http::Uri;

use crate::error::ErrorKind;
use crate::json::Value;
use crate::json::parse_members;
use crate::transport::http::agent;
use crate::transport::http::in_clear;
use crate::wire::RETRY_AFTER;

/// The largest answer body read: a token answer is a few KiB.
const ANSWER_LIMIT: u64 = 64 << 10;

/// An API key: printable ASCII without spaces, never printed.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Secret(String);

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(..)")
    }
}

/// The key, the endpoint that exchanges it and the audience its tokens are for: one source of
/// tokens. Two keys are the same source when all three match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Key {
    secret: Secret,
    endpoint: String,
    audience: Option<String>,
}

/// Why a key cannot be exchanged as given.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum KeyRefusal {
    #[error("the API key must be printable ASCII without spaces")]
    Unusable,
    #[error("the token endpoint {endpoint} is not an http:// or https:// URL")]
    Endpoint { endpoint: String },
    /// The key would cross the network in clear.
    #[error(
        "the token endpoint {endpoint} would get the API key over plain http: use https:// (http:// only to a loopback host)"
    )]
    InClear { endpoint: String },
}

impl Key {
    /// The key `secret`, exchanged at `endpoint` for tokens of `audience`. The endpoint is
    /// `https://`, or `http://` to a loopback host, since its requests carry the key.
    pub(crate) fn new(
        secret: &str,
        endpoint: &str,
        audience: Option<&str>,
    ) -> Result<Self, KeyRefusal> {
        let secret = super::usable(secret).ok_or(KeyRefusal::Unusable)?;
        let uri = endpoint
            .parse::<Uri>()
            .ok()
            .filter(|uri| matches!(uri.scheme_str(), Some("http" | "https")))
            .ok_or_else(|| KeyRefusal::Endpoint {
                endpoint: endpoint.to_owned(),
            })?;
        if in_clear(&uri) {
            return Err(KeyRefusal::InClear {
                endpoint: endpoint.to_owned(),
            });
        }
        Ok(Self {
            secret: Secret(secret),
            endpoint: endpoint.to_owned(),
            audience: audience.map(str::to_owned),
        })
    }
}

/// A token the endpoint handed out, and how long it lives.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Exchanged {
    pub(crate) token: String,
    pub(crate) lifetime_ms: u64,
}

impl fmt::Debug for Exchanged {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Exchanged")
            .field("token", &"..")
            .field("lifetime_ms", &self.lifetime_ms)
            .finish()
    }
}

/// Why an exchange gave no token. None carries the key or a token.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ExchangeError {
    /// No answer: the request failed before one, or its body broke off. The reason is the
    /// transport error's text, as in `wire::NoAnswer`: the schedule keeps the last failure and
    /// status shows it, so the error is cloned and compared, which `ureq::Error` is not.
    #[error("the token endpoint did not answer: {reason}")]
    Unreachable { reason: String },
    /// `401` or `403`: the key was refused.
    #[error("the token endpoint refused the API key ({status})")]
    Refused { status: u16 },
    /// `429`, with the wait its `Retry-After` asked for in whole seconds.
    #[error("the token endpoint answered 429")]
    RateLimited { retry_after_ms: Option<u64> },
    /// `408` or `5xx`.
    #[error("the token endpoint answered {status}")]
    Unavailable { status: u16 },
    /// A status the exchange cannot use: a redirect (never followed) or another `4xx`.
    #[error("the token endpoint answered {status}, which the exchange cannot use")]
    Unexpected { status: u16 },
    /// A `2xx` without a usable `access_token` and a positive `expires_in`.
    #[error("the token endpoint answered without a usable token")]
    NoToken,
    /// A token that expires within the earliest refresh.
    #[error(
        "the token endpoint handed out a token that expires in {lifetime_ms} ms, under the {min_ms} ms a refresh needs"
    )]
    ShortLived { lifetime_ms: u64, min_ms: u64 },
    /// The exchange panicked inside the extension.
    #[error("the exchange failed inside the extension")]
    Panicked,
}

impl ExchangeError {
    /// The kind `ursula_status` reports.
    pub(crate) fn kind(&self) -> ErrorKind {
        match self {
            ExchangeError::Unreachable { .. } => ErrorKind::Unreachable,
            ExchangeError::Refused { status: 403 } => ErrorKind::Forbidden,
            ExchangeError::Refused { .. } => ErrorKind::Unauthorized,
            ExchangeError::RateLimited { .. } => ErrorKind::RateLimited,
            ExchangeError::Unavailable { .. }
            | ExchangeError::Unexpected { .. }
            | ExchangeError::NoToken
            | ExchangeError::ShortLived { .. } => ErrorKind::Unavailable,
            ExchangeError::Panicked => ErrorKind::Internal,
        }
    }

    /// Whether trying again is unlikely to help until the key or the endpoint changes: a refused
    /// key, a status the exchange cannot use, or a panic inside the exchange.
    pub(crate) fn permanent(&self) -> bool {
        matches!(
            self,
            ExchangeError::Refused { .. }
                | ExchangeError::Unexpected { .. }
                | ExchangeError::Panicked
        )
    }

    /// The wait a `429` asked for.
    pub(crate) fn retry_after_ms(&self) -> Option<u64> {
        match self {
            ExchangeError::RateLimited { retry_after_ms } => *retry_after_ms,
            _ => None,
        }
    }
}

/// Exchanges `key` once, blocking.
pub(crate) fn exchange(key: &Key) -> Result<Exchanged, ExchangeError> {
    let body = match &key.audience {
        Some(audience) => crate::json::object(&[("audience", crate::json::str(audience))]),
        None => "{}".to_owned(),
    };
    let sent = agent()
        .post(&key.endpoint)
        .header("authorization", format!("Bearer {}", key.secret.0))
        .header("content-type", "application/json")
        .header("accept", "application/json")
        .send(body.as_bytes());
    let mut response = sent.map_err(|e| ExchangeError::Unreachable {
        reason: e.to_string(),
    })?;
    let status = response.status().as_u16();
    let retry_after_ms = response
        .headers()
        .get(RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|seconds| seconds.trim().parse::<u64>().ok())
        .map(|seconds| seconds.saturating_mul(1000));
    let body = response
        .body_mut()
        .with_config()
        .limit(ANSWER_LIMIT)
        .read_to_vec()
        .map_err(|e| ExchangeError::Unreachable {
            reason: format!("the answer's body broke off: {e}"),
        })?;
    answer(status, retry_after_ms, &body)
}

/// What the endpoint's answer `status` with `body` gives, where `retry_after_ms` is its
/// `Retry-After`.
pub(crate) fn answer(
    status: u16,
    retry_after_ms: Option<u64>,
    body: &[u8],
) -> Result<Exchanged, ExchangeError> {
    match status {
        200..=299 => token_of(body).ok_or(ExchangeError::NoToken),
        401 | 403 => Err(ExchangeError::Refused { status }),
        429 => Err(ExchangeError::RateLimited { retry_after_ms }),
        408 | 500..=599 => Err(ExchangeError::Unavailable { status }),
        _ => Err(ExchangeError::Unexpected { status }),
    }
}

/// The token and its lifetime in a `2xx` body: a JSON object with a usable `access_token` and
/// `expires_in`, a positive number of seconds. Other members are skipped, nested ones included.
fn token_of(body: &[u8]) -> Option<Exchanged> {
    let text = std::str::from_utf8(body).ok()?;
    let (mut token, mut lifetime_ms) = (None, None);
    for (name, value) in parse_members(text).ok()? {
        match (name.as_str(), value) {
            ("access_token", Value::Text(t)) => token = super::usable(&t),
            ("expires_in", Value::Number(seconds)) => lifetime_ms = lifetime(seconds),
            _ => {}
        }
    }
    Some(Exchanged {
        token: token?,
        lifetime_ms: lifetime_ms?,
    })
}

/// `expires_in` in whole milliseconds, rounded down: any positive number of seconds. One that
/// gives under the earliest refresh is a short-lived token, which the schedule refuses (`key`).
fn lifetime(seconds: f64) -> Option<u64> {
    if seconds.is_nan() || seconds <= 0.0 {
        return None;
    }
    let ms = Duration::try_from_secs_f64(seconds).ok()?.as_millis();
    Some(u64::try_from(ms).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::ExchangeError;
    use super::Exchanged;
    use super::Key;
    use super::KeyRefusal;
    use super::answer;
    use crate::error::ErrorKind;

    // A token answer gives the token and its lifetime in ms. Anything else gives no token.
    #[test]
    fn a_token_answer_gives_the_token_and_its_lifetime() {
        let ok = |token: &str, lifetime_ms| {
            Ok(Exchanged {
                token: token.to_owned(),
                lifetime_ms,
            })
        };
        assert_eq!(
            answer(
                200,
                None,
                br#"{"access_token": "t.1-x", "token_type": "Bearer", "expires_in": 900}"#
            ),
            ok("t.1-x", 900_000)
        );
        // Members the exchange does not read are skipped, nested ones too.
        assert_eq!(
            answer(
                200,
                None,
                br#"{"scope": ["a", {"b": [1, {}]}], "access_token": "t1",
                     "authorization_details": {"type": "x", "n": null}, "expires_in": 60}"#
            ),
            ok("t1", 60_000)
        );
        // Any positive number of seconds, rounded down to whole milliseconds.
        assert_eq!(
            answer(200, None, br#"{"access_token": "t1", "expires_in": 1.5}"#),
            ok("t1", 1_500)
        );
        assert_eq!(
            answer(
                200,
                None,
                br#"{"access_token": "t1", "expires_in": 0.0004}"#
            ),
            ok("t1", 0)
        );
        for body in [
            &br#"{"expires_in": 900}"#[..],
            br#"{"access_token": "t1"}"#,
            br#"{"access_token": "two words", "expires_in": 900}"#,
            br#"{"access_token": "t1", "expires_in": 0}"#,
            br#"{"access_token": "t1", "expires_in": -5}"#,
            br#"{"access_token": "t1", "expires_in": "900"}"#,
            br#"{"access_token": 1, "expires_in": 900}"#,
            br#"{"access_token": {"t": "t1"}, "expires_in": 900}"#,
            br#"{"access_token": "t1", "expires_in": 900, "x": [}"#,
            b"not json",
            b"",
        ] {
            assert_eq!(answer(200, None, body), Err(ExchangeError::NoToken));
        }
    }

    // A refused key, a passing failure and a status the exchange cannot use are told apart, and
    // only the first and the last are permanent.
    #[test]
    fn failures_are_classified_by_status() {
        let ok = br#"{"access_token": "t1", "expires_in": 900}"#;
        let refused = |status| ExchangeError::Refused { status };
        let unavailable = |status| ExchangeError::Unavailable { status };
        let unexpected = |status| ExchangeError::Unexpected { status };
        let cases = [
            (401, refused(401), ErrorKind::Unauthorized),
            (403, refused(403), ErrorKind::Forbidden),
            (
                429,
                ExchangeError::RateLimited {
                    retry_after_ms: Some(3_000),
                },
                ErrorKind::RateLimited,
            ),
            (408, unavailable(408), ErrorKind::Unavailable),
            (503, unavailable(503), ErrorKind::Unavailable),
            (302, unexpected(302), ErrorKind::Unavailable),
            (404, unexpected(404), ErrorKind::Unavailable),
        ];
        for (status, error, kind) in cases {
            assert_eq!(
                answer(status, Some(3_000), ok),
                Err(error.clone()),
                "{status}"
            );
            assert_eq!(error.kind(), kind, "{status}");
            let permanent = matches!(status, 401 | 403 | 302 | 404);
            assert_eq!(error.permanent(), permanent, "{status}");
        }
        assert_eq!(ExchangeError::NoToken.kind(), ErrorKind::Unavailable);
        assert!(!ExchangeError::NoToken.permanent());
        assert_eq!(ExchangeError::Panicked.kind(), ErrorKind::Internal);
        assert!(ExchangeError::Panicked.permanent());
    }

    // The key goes only to an https:// endpoint, or an http:// one on a loopback host, and is
    // never printed.
    #[test]
    fn a_key_goes_only_to_a_confidential_endpoint_and_never_prints() {
        let key = Key::new(
            " k_secret\n",
            "https://auth.example/api/iam/token",
            Some("aud"),
        )
        .unwrap();
        assert!(!format!("{key:?}").contains("k_secret"));
        for endpoint in [
            "http://127.0.0.1:9/token",
            "http://localhost/token",
            "http://[::1]/t",
        ] {
            assert!(Key::new("k", endpoint, None).is_ok(), "{endpoint}");
        }
        assert_eq!(
            Key::new("k", "http://auth.example/token", None),
            Err(KeyRefusal::InClear {
                endpoint: "http://auth.example/token".to_owned()
            })
        );
        assert!(matches!(
            Key::new("k", "ftp://auth.example/token", None),
            Err(KeyRefusal::Endpoint { .. })
        ));
        assert!(matches!(
            Key::new("k", "not a url", None),
            Err(KeyRefusal::Endpoint { .. })
        ));
        assert_eq!(
            Key::new("two words", "https://auth.example/t", None),
            Err(KeyRefusal::Unusable)
        );
        let token = Exchanged {
            token: "tok_secret".to_owned(),
            lifetime_ms: 1,
        };
        assert!(!format!("{token:?}").contains("tok_secret"));
    }
}
