//! The Management API's lookup of a database by name (design: Tonbo): `GET {api
//! origin}/v1/databases/{name}`, the key as the bearer token, answered with `{"data": {"state",
//! "url", ...}}`, or problem details (`code`, `detail`, `request_id`) and `X-Request-Id`.
//!
//! - A `401` with `exchange_required` says the key must be exchanged first: at the token endpoint
//!   with no audience (`auth::exchange`), and the lookup goes again once with that token. The
//!   token is held per key and origin until 30 s before it expires, and a `401` to it exchanges
//!   again and retries once. The key goes only to the two configured origins, never to an
//!   endpoint an answer names.
//! - An `active` database gives its `url`. `creating`, or a state the extension does not know, is
//!   looked up again after the backoff. `deleting` and `deleted` fail `database_not_found`, as
//!   `404` does. `401` and `403` fail `invalid_api_key`.
//! - `429` waits for its `Retry-After` (whole seconds), never less than the backoff. `408`, `5xx`,
//!   an answer without a database, and a request without an answer are retried after the
//!   backoff, from 250 ms doubled up to 2 s, until the resolve's time runs out: then it fails with
//!   the last condition. Any other status, a redirect included (never followed), fails
//!   `invalid_argument` naming the origin to check.
//!
//! Requests go out on the agent of appends and reads (`transport::http::agent`): the same roots
//! (`URSULA_VFS_CA_FILE`), the same proxy, no redirect. A lookup is bounded by the time the resolve
//! has left, and an exchange by the agent's own 10 s.

use std::collections::HashMap;
use std::fmt;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;
use std::time::Instant;

use super::ApiKey;
use super::KeyFrom;
use super::TonboError;
use super::Until;
use crate::auth::exchange::ExchangeError;
use crate::auth::exchange::Key;
use crate::auth::exchange::exchange;
use crate::error::ErrorKind;
use crate::json::Value;
use crate::json::member_object;
use crate::json::parse_members;
use crate::transport::http::agent;
use crate::wire::RETRY_AFTER;

/// The first wait before a lookup goes again, doubled up to [`MAX_RETRY_MS`].
const FIRST_RETRY_MS: u64 = 250;
const MAX_RETRY_MS: u64 = 2_000;
/// A Management API token is exchanged again this long before it expires.
const EXPIRY_MARGIN_MS: u64 = 30_000;
/// The largest answer body read: a database or a problem is a few KiB.
const ANSWER_LIMIT: u64 = 64 << 10;
/// The shortest time a request is given, so one sent just before the end still goes out.
const MIN_REQUEST_MS: u64 = 1;

/// The two Tonbo APIs, as messages name them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Api {
    Management,
    TokenEndpoint,
}

impl Api {
    /// The option and the variable that name the API's origin, to check.
    pub(crate) fn fix(self) -> &'static str {
        match self {
            Api::Management => "Check api_origin (or TONBO_API_ORIGIN)",
            Api::TokenEndpoint => "Check auth_origin (or TONBO_AUTH_ORIGIN)",
        }
    }
}

impl fmt::Display for Api {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Api::Management => "the Tonbo Management API",
            Api::TokenEndpoint => "the Tonbo token endpoint",
        })
    }
}

/// A failure a resolve retries until its time runs out: the condition it reports then.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum Transient {
    #[error("{api} did not answer: {reason}")]
    Unreachable { api: Api, reason: String },
    #[error("{api} answered 429")]
    RateLimited {
        api: Api,
        retry_after_ms: Option<u64>,
    },
    #[error("{api} answered {status}")]
    Unavailable { api: Api, status: u16 },
    #[error("{} answered {status} without a database", Api::Management)]
    NoDatabase { status: u16 },
    #[error("{} answered without a usable token", Api::TokenEndpoint)]
    NoToken,
    #[error("database \"{name}\" is {state}")]
    NotActive { name: String, state: String },
}

impl Transient {
    /// The kind a resolve that gave up on it reports.
    pub(crate) fn kind(&self) -> ErrorKind {
        match self {
            Transient::Unreachable { .. } => ErrorKind::Unreachable,
            Transient::RateLimited { .. } => ErrorKind::RateLimited,
            Transient::Unavailable { .. }
            | Transient::NoDatabase { .. }
            | Transient::NoToken
            | Transient::NotActive { .. } => ErrorKind::Unavailable,
        }
    }

    /// The wait it asked for: a `429`'s `Retry-After`.
    fn retry_after_ms(&self) -> Option<u64> {
        match self {
            Transient::RateLimited { retry_after_ms, .. } => *retry_after_ms,
            _ => None,
        }
    }
}

/// How a request of the resolve failed.
#[derive(Debug)]
pub(crate) enum Failure {
    /// For good: the resolve fails with it.
    Final(TonboError),
    /// Retried after the backoff.
    Transient(Transient),
}

/// What an exchange's failure is to a resolve that holds the key from `from`.
pub(crate) fn exchange_failure(e: ExchangeError, from: &KeyFrom) -> Failure {
    let api = Api::TokenEndpoint;
    match e {
        ExchangeError::Unreachable { reason } => {
            Failure::Transient(Transient::Unreachable { api, reason })
        }
        ExchangeError::Refused { .. } => Failure::Final(TonboError::KeyRefused {
            by: api,
            from: from.clone(),
            code: None,
            request_id: None,
        }),
        ExchangeError::RateLimited { retry_after_ms } => {
            Failure::Transient(Transient::RateLimited {
                api,
                retry_after_ms,
            })
        }
        ExchangeError::Unavailable { status } => {
            Failure::Transient(Transient::Unavailable { api, status })
        }
        ExchangeError::Unexpected { status } => Failure::Final(TonboError::Unexpected {
            api,
            status,
            name: None,
            detail: None,
            request_id: None,
        }),
        ExchangeError::NoToken | ExchangeError::ShortLived { .. } => {
            Failure::Transient(Transient::NoToken)
        }
        ExchangeError::Panicked => Failure::Final(TonboError::Panicked),
    }
}

/// The stream URL of database `name`: looked up with `key` at `api` until it is active, retrying
/// what passes, until `until` ends the resolve.
pub(crate) fn look_up(
    name: &str,
    key: &ApiKey,
    api: &str,
    auth: &str,
    until: &Until,
) -> Result<String, TonboError> {
    let mut last: Option<Transient> = None;
    let mut attempt: u32 = 0;
    loop {
        let backoff = FIRST_RETRY_MS
            .checked_shl(attempt.min(8))
            .unwrap_or(MAX_RETRY_MS)
            .min(MAX_RETRY_MS);
        attempt = attempt.saturating_add(1);
        let wait = match once(name, key, api, auth, until) {
            Ok(found) => match found.state.as_str() {
                "active" => {
                    return found.url.ok_or_else(|| TonboError::NoUrl {
                        name: name.to_owned(),
                        request_id: found.request_id,
                    });
                }
                "deleting" | "deleted" => {
                    return Err(TonboError::NotFound {
                        name: name.to_owned(),
                        request_id: found.request_id,
                    });
                }
                state => {
                    last = Some(Transient::NotActive {
                        name: name.to_owned(),
                        state: state.to_owned(),
                    });
                    backoff
                }
            },
            Err(Failure::Final(e)) => return Err(e),
            // A request the end of the resolve cut short says nothing new: the last condition
            // stands.
            Err(Failure::Transient(_)) if until.over() => 0,
            Err(Failure::Transient(t)) => {
                let wait = t.retry_after_ms().unwrap_or(0).max(backoff);
                last = Some(t);
                wait
            }
        };
        if !until.sleep(Duration::from_millis(wait)) {
            return Err(TonboError::GaveUp {
                subject: super::Subject::Database(name.to_owned()),
                last,
                cancelled: until.cancelled(),
            });
        }
    }
}

/// The database the Management API answered with, as far as the lookup reads it.
#[derive(Debug)]
struct Found {
    state: String,
    url: Option<String>,
    request_id: Option<String>,
}

/// One lookup of `name`, exchanging the key first when the Management API asks for it.
fn once(name: &str, key: &ApiKey, api: &str, auth: &str, until: &Until) -> Result<Found, Failure> {
    let url = format!("{api}/v1/databases/{}", component(name));
    let mut bearer = management_bearer(key, auth, false)?;
    let mut answer = get(&url, &bearer, until)?;
    if answer.status == 401 {
        let problem = answer.problem();
        // A key that must be exchanged, or an exchanged token that expired early: exchange, and
        // look up again once.
        if problem.code.as_deref() != Some("exchange_required") && bearer == key.secret {
            return Err(Failure::Final(refused(key, problem)));
        }
        bearer = management_bearer(key, auth, true)?;
        answer = get(&url, &bearer, until)?;
    }
    let status = answer.status;
    if (200..300).contains(&status) {
        let data = std::str::from_utf8(&answer.body)
            .ok()
            .and_then(|text| member_object(text, "data").ok().flatten());
        let (mut state, mut url) = (None, None);
        for (member, value) in data.unwrap_or_default() {
            match (member.as_str(), value) {
                ("state", Value::Text(s)) => state = Some(s),
                ("url", Value::Text(u)) => url = Some(u),
                _ => {}
            }
        }
        let Some(state) = state else {
            return Err(Failure::Transient(Transient::NoDatabase { status }));
        };
        return Ok(Found {
            state,
            url,
            request_id: answer.request_id,
        });
    }
    let api = Api::Management;
    match status {
        429 => Err(Failure::Transient(Transient::RateLimited {
            api,
            retry_after_ms: answer.retry_after_ms,
        })),
        408 | 500..=599 => Err(Failure::Transient(Transient::Unavailable { api, status })),
        401 => Err(Failure::Final(refused(key, answer.problem()))),
        403 => Err(Failure::Final(TonboError::Forbidden {
            name: name.to_owned(),
            from: key.from.clone(),
            request_id: answer.problem().request_id,
        })),
        404 => Err(Failure::Final(TonboError::NotFound {
            name: name.to_owned(),
            request_id: answer.problem().request_id,
        })),
        _ => {
            let problem = answer.problem();
            Err(Failure::Final(TonboError::Unexpected {
                api,
                status,
                name: Some(name.to_owned()),
                detail: problem.detail,
                request_id: problem.request_id,
            }))
        }
    }
}

/// The Management API refused the key.
fn refused(key: &ApiKey, problem: Problem) -> TonboError {
    TonboError::KeyRefused {
        by: Api::Management,
        from: key.from.clone(),
        code: problem.code,
        request_id: problem.request_id,
    }
}

/// A Management API token exchanged for a key.
struct Held {
    token: String,
    expires_at: Instant,
}

/// The Management API tokens of keys that must be exchanged first, by the digest of the auth
/// origin and the key: an entry says the key needs the exchange, its value is the token held.
static MANAGEMENT: Mutex<Option<HashMap<[u8; 32], Option<Held>>>> = Mutex::new(None);

/// The bearer token of a Management API request with `key`: the key itself, or the token
/// exchanged for it at `auth`'s token endpoint, anew with `fresh`. The exchange runs outside the
/// lock.
fn management_bearer(key: &ApiKey, auth: &str, fresh: bool) -> Result<String, Failure> {
    let id = {
        let mut hasher = blake3::Hasher::new();
        hasher.update(auth.as_bytes());
        hasher.update(&[0]);
        hasher.update(key.secret.as_bytes());
        *hasher.finalize().as_bytes()
    };
    {
        let mut held = MANAGEMENT.lock().unwrap_or_else(PoisonError::into_inner);
        let tokens = held.get_or_insert_with(HashMap::new);
        match tokens.get(&id) {
            None if !fresh => return Ok(key.secret.clone()),
            Some(Some(h)) if !fresh && h.expires_at > Instant::now() => {
                return Ok(h.token.clone());
            }
            _ => {}
        }
        tokens.insert(id, None);
    }
    let endpoint = format!("{auth}/api/iam/token");
    let exchanger = Key::new(&key.secret, &endpoint, None).map_err(|_unsendable| {
        Failure::Final(TonboError::KeyRefused {
            by: Api::TokenEndpoint,
            from: key.from.clone(),
            code: None,
            request_id: None,
        })
    })?;
    let got = exchange(&exchanger).map_err(|e| exchange_failure(e, &key.from))?;
    let lifetime = Duration::from_millis(got.lifetime_ms.saturating_sub(EXPIRY_MARGIN_MS));
    let now = Instant::now();
    let mut held = MANAGEMENT.lock().unwrap_or_else(PoisonError::into_inner);
    held.get_or_insert_with(HashMap::new).insert(
        id,
        Some(Held {
            token: got.token.clone(),
            expires_at: now.checked_add(lifetime).unwrap_or(now),
        }),
    );
    Ok(got.token)
}

/// An answer of the Management API, as far as the lookup reads it.
struct Answer {
    status: u16,
    retry_after_ms: Option<u64>,
    request_id: Option<String>,
    body: Vec<u8>,
}

/// An answer's problem details (`application/problem+json`), as far as the lookup reads them.
#[derive(Debug, Default)]
struct Problem {
    code: Option<String>,
    detail: Option<String>,
    request_id: Option<String>,
}

impl Answer {
    /// The problem details of the body, the request id from `X-Request-Id` when the body names
    /// none. A body that is not JSON gives none: the status says it all.
    fn problem(&self) -> Problem {
        let mut problem = Problem::default();
        let members = std::str::from_utf8(&self.body)
            .ok()
            .and_then(|text| parse_members(text).ok())
            .unwrap_or_default();
        for (member, value) in members {
            let Value::Text(text) = value else { continue };
            if text.is_empty() {
                continue;
            }
            match member.as_str() {
                "code" => problem.code = Some(text),
                "detail" => problem.detail = Some(text),
                "request_id" => problem.request_id = Some(text),
                _ => {}
            }
        }
        if problem.request_id.is_none() {
            problem.request_id.clone_from(&self.request_id);
        }
        problem
    }
}

/// `GET url` with `bearer`, given the time `until` leaves it.
fn get(url: &str, bearer: &str, until: &Until) -> Result<Answer, Failure> {
    let unreachable = |reason: String| {
        Failure::Transient(Transient::Unreachable {
            api: Api::Management,
            reason,
        })
    };
    let left = until.left().max(Duration::from_millis(MIN_REQUEST_MS));
    let mut response = agent()
        .get(url)
        .header("authorization", format!("Bearer {bearer}"))
        .header("accept", "application/json")
        .config()
        .timeout_global(Some(left))
        .build()
        .call()
        .map_err(|e| unreachable(e.to_string()))?;
    let header = |name: &str| {
        response
            .headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.trim().to_owned())
            .filter(|v| !v.is_empty())
    };
    let status = response.status().as_u16();
    let retry_after_ms = header(RETRY_AFTER)
        .and_then(|seconds| seconds.parse::<u64>().ok())
        .map(|seconds| seconds.saturating_mul(1000));
    let request_id = header("x-request-id");
    let body = response
        .body_mut()
        .with_config()
        .limit(ANSWER_LIMIT)
        .read_to_vec()
        .map_err(|e| unreachable(format!("the answer's body broke off: {e}")))?;
    Ok(Answer {
        status,
        retry_after_ms,
        request_id,
        body,
    })
}

/// `name` as one path segment, as `encodeURIComponent` writes it: unreserved characters as they
/// are, every other byte of its UTF-8 percent-encoded.
fn component(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for &b in name.as_bytes() {
        if b.is_ascii_alphanumeric() || b"-_.!~*'()".contains(&b) {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}
