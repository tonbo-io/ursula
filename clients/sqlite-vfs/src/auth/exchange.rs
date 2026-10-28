//! One exchange of an API key for a bearer token: a `POST` to the token endpoint with the key as
//! its bearer token and `{"audience": ...}` (or `{}` without an audience) as its body, answered
//! with `{"access_token": ..., "expires_in": <seconds>}`. It goes out on the agent of appends and
//! reads (`transport::http::agent`), so it trusts the same roots (`URSULA_VFS_CA_FILE`), takes
//! the same proxy and follows no redirect.
//!
//! Neither the key nor a token is ever printed: [`Secret`] and [`Exchanged`] print redacted, and
//! no error carries either.

use std::fmt;

use ureq::http::Uri;

use crate::error::ErrorKind;
use crate::json::Value;
use crate::json::parse_object;
use crate::json::whole;
use crate::transport::http::agent;
use crate::transport::http::in_clear;

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
    /// No answer: the request failed before one, or its body broke off.
    #[error("the token endpoint did not answer: {reason}")]
    Unreachable { reason: String },
    /// `401` or `403`: the key was refused.
    #[error("the token endpoint refused the API key ({status})")]
    Refused { status: u16 },
    #[error("the token endpoint answered 429")]
    RateLimited,
    /// `408` or `5xx`.
    #[error("the token endpoint answered {status}")]
    Unavailable { status: u16 },
    /// A status the exchange cannot use: a redirect (never followed) or another `4xx`.
    #[error("the token endpoint answered {status}, which the exchange cannot use")]
    Unexpected { status: u16 },
    /// A `2xx` without a usable `access_token` and a positive whole `expires_in`.
    #[error("the token endpoint answered without a usable token")]
    NoToken,
    /// A token that expires within the earliest refresh.
    #[error(
        "the token endpoint handed out a token that expires in {lifetime_ms} ms, under the {min_ms} ms a refresh needs"
    )]
    ShortLived { lifetime_ms: u64, min_ms: u64 },
}

impl ExchangeError {
    /// The kind `ursula_status` reports.
    pub(crate) fn kind(&self) -> ErrorKind {
        match self {
            ExchangeError::Unreachable { .. } => ErrorKind::Unreachable,
            ExchangeError::Refused { status: 403 } => ErrorKind::Forbidden,
            ExchangeError::Refused { .. } => ErrorKind::Unauthorized,
            ExchangeError::RateLimited => ErrorKind::RateLimited,
            ExchangeError::Unavailable { .. }
            | ExchangeError::Unexpected { .. }
            | ExchangeError::NoToken
            | ExchangeError::ShortLived { .. } => ErrorKind::Unavailable,
        }
    }

    /// Whether trying again cannot help until the key or the endpoint changes: a refused key, or
    /// an answer the exchange cannot use.
    pub(crate) fn permanent(&self) -> bool {
        matches!(
            self,
            ExchangeError::Refused { .. } | ExchangeError::Unexpected { .. }
        )
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
    let body = response
        .body_mut()
        .with_config()
        .limit(ANSWER_LIMIT)
        .read_to_vec()
        .map_err(|e| ExchangeError::Unreachable {
            reason: format!("the answer's body broke off: {e}"),
        })?;
    answer(status, &body)
}

/// What the endpoint's answer `status` with `body` gives.
pub(crate) fn answer(status: u16, body: &[u8]) -> Result<Exchanged, ExchangeError> {
    match status {
        200..=299 => token_of(body).ok_or(ExchangeError::NoToken),
        401 | 403 => Err(ExchangeError::Refused { status }),
        429 => Err(ExchangeError::RateLimited),
        408 | 500..=599 => Err(ExchangeError::Unavailable { status }),
        _ => Err(ExchangeError::Unexpected { status }),
    }
}

/// The token and its lifetime in a `2xx` body: a flat JSON object with a usable `access_token` and
/// `expires_in`, a positive whole number of seconds. Other members are ignored.
fn token_of(body: &[u8]) -> Option<Exchanged> {
    let text = std::str::from_utf8(body).ok()?;
    let (mut token, mut lifetime_ms) = (None, None);
    for (name, value) in parse_object(text).ok()? {
        match (name.as_str(), value) {
            ("access_token", Value::Text(t)) => token = super::usable(&t),
            ("expires_in", Value::Number(n)) => {
                lifetime_ms = whole(n).filter(|&s| s > 0).map(|s| s.saturating_mul(1000));
            }
            _ => {}
        }
    }
    Some(Exchanged {
        token: token?,
        lifetime_ms: lifetime_ms?,
    })
}

#[cfg(test)]
mod tests {
    use super::ExchangeError;
    use super::Exchanged;
    use super::Key;
    use super::KeyRefusal;
    use super::answer;
    use crate::error::ErrorKind;

    // A token answer gives the token and its lifetime in ms; anything else gives no token.
    #[test]
    fn a_token_answer_gives_the_token_and_its_lifetime() {
        assert_eq!(
            answer(
                200,
                br#"{"access_token": "t.1-x", "token_type": "Bearer", "expires_in": 900}"#
            ),
            Ok(Exchanged {
                token: "t.1-x".to_owned(),
                lifetime_ms: 900_000,
            })
        );
        for body in [
            &br#"{"expires_in": 900}"#[..],
            br#"{"access_token": "t1"}"#,
            br#"{"access_token": "two words", "expires_in": 900}"#,
            br#"{"access_token": "t1", "expires_in": 0}"#,
            br#"{"access_token": "t1", "expires_in": 1.5}"#,
            br#"{"access_token": "t1", "expires_in": "900"}"#,
            br#"{"access_token": 1, "expires_in": 900}"#,
            b"not json",
            b"",
        ] {
            assert_eq!(answer(200, body), Err(ExchangeError::NoToken));
        }
    }

    // A refused key, a passing failure and an answer the exchange cannot use are told apart, and
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
            (429, ExchangeError::RateLimited, ErrorKind::RateLimited),
            (408, unavailable(408), ErrorKind::Unavailable),
            (503, unavailable(503), ErrorKind::Unavailable),
            (302, unexpected(302), ErrorKind::Unavailable),
            (404, unexpected(404), ErrorKind::Unavailable),
        ];
        for (status, error, kind) in cases {
            assert_eq!(answer(status, ok), Err(error.clone()), "{status}");
            assert_eq!(error.kind(), kind, "{status}");
            let permanent = matches!(status, 401 | 403 | 302 | 404);
            assert_eq!(error.permanent(), permanent, "{status}");
        }
        assert_eq!(ExchangeError::NoToken.kind(), ErrorKind::Unavailable);
        assert!(!ExchangeError::NoToken.permanent());
    }

    // The key goes only to an https:// endpoint, or an http:// one on a loopback host, and is
    // never printed.
    #[test]
    fn a_key_goes_only_to_a_confidential_endpoint_and_never_prints() {
        let key = Key::new(
            " tbo_secret\n",
            "https://auth.example/api/iam/token",
            Some("aud"),
        )
        .unwrap();
        assert!(!format!("{key:?}").contains("tbo_secret"));
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
