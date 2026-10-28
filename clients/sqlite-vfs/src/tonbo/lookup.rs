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
//! (`URSULA_VFS_CA_FILE`), the same proxy, no redirect. Each request, the exchange included, is
//! bounded by the time the resolve has left.
//!
//! What an answer says ([`classify`], [`asks_exchange`]), what a database's state says
//! ([`settle`]) and how long the lookup waits ([`backoff_ms`], [`wait_ms`]) are pure: the loop
//! around them only sends, sleeps and keeps the last condition.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;
use std::time::Instant;

use super::ApiKey;
use super::KeyFrom;
use super::Subject;
use super::TonboError;
use super::Until;
use crate::auth::exchange::ExchangeError;
use crate::auth::exchange::Key;
use crate::auth::exchange::Problem;
use crate::auth::exchange::exchange;
use crate::error::ErrorKind;
use crate::json::Value;
use crate::json::member_object;
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
    /// The option and the variable that name the API's origin, to check, in words any host's
    /// spelling of the option fits.
    pub(crate) fn fix(self) -> &'static str {
        match self {
            Api::Management => "Check the API origin option or TONBO_API_ORIGIN",
            Api::TokenEndpoint => "Check the auth origin option or TONBO_AUTH_ORIGIN",
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
#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum Transient {
    #[error("{} did not answer: {source}", Api::Management)]
    Unreachable {
        #[source]
        source: Arc<ureq::Error>,
    },
    #[error("the answer of {} broke off: {source}", Api::Management)]
    BrokeOff {
        #[source]
        source: Arc<ureq::Error>,
    },
    #[error("{} answered 429", Api::Management)]
    RateLimited { retry_after_ms: Option<u64> },
    #[error("{} answered {status}", Api::Management)]
    Unavailable { status: u16 },
    #[error("{} answered {status} without a database", Api::Management)]
    NoDatabase { status: u16 },
    #[error("database \"{name}\" is {state}")]
    NotActive { name: String, state: String },
    /// The token endpoint's failure that passes.
    #[error(transparent)]
    Exchange(ExchangeError),
}

impl Transient {
    /// The kind a resolve that gave up on it reports.
    pub(crate) fn kind(&self) -> ErrorKind {
        match self {
            Transient::Unreachable { .. } | Transient::BrokeOff { .. } => ErrorKind::Unreachable,
            Transient::RateLimited { .. } => ErrorKind::RateLimited,
            Transient::Unavailable { .. }
            | Transient::NoDatabase { .. }
            | Transient::NotActive { .. } => ErrorKind::Unavailable,
            Transient::Exchange(e) => e.kind(),
        }
    }

    /// The wait it asked for: a `429`'s `Retry-After`.
    fn retry_after_ms(&self) -> Option<u64> {
        match self {
            Transient::RateLimited { retry_after_ms } => *retry_after_ms,
            Transient::Exchange(e) => e.retry_after_ms(),
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

/// What an exchange's failure is to a resolve that holds the key from `from`: a refusal and a
/// status outside the contract fail for good, with the answer's problem details, and the rest
/// passes.
pub(crate) fn exchange_failure(e: ExchangeError, from: &KeyFrom) -> Failure {
    let api = Api::TokenEndpoint;
    match e {
        ExchangeError::Refused { problem, .. } => {
            let Problem {
                code, request_id, ..
            } = *problem;
            Failure::Final(TonboError::KeyRefused {
                by: api,
                from: from.clone(),
                code,
                request_id,
            })
        }
        ExchangeError::Unexpected { status, problem } => {
            let Problem {
                detail, request_id, ..
            } = *problem;
            Failure::Final(TonboError::Unexpected {
                api,
                status,
                name: None,
                detail,
                request_id,
            })
        }
        ExchangeError::Panicked => Failure::Final(TonboError::Panicked),
        e @ (ExchangeError::Unreachable { .. }
        | ExchangeError::RateLimited { .. }
        | ExchangeError::Unavailable { .. }
        | ExchangeError::NoToken
        | ExchangeError::ShortLived { .. }) => Failure::Transient(Transient::Exchange(e)),
    }
}

/// The wait before the lookup's try after `attempt` failed ones: from 250 ms, doubled up to 2 s.
fn backoff_ms(attempt: u32) -> u64 {
    FIRST_RETRY_MS
        .checked_shl(attempt.min(8))
        .unwrap_or(MAX_RETRY_MS)
        .min(MAX_RETRY_MS)
}

/// The wait after `t`: its `Retry-After`, never less than `backoff_ms`.
fn wait_ms(t: &Transient, backoff_ms: u64) -> u64 {
    t.retry_after_ms().unwrap_or(0).max(backoff_ms)
}

/// What a database's state says to the lookup.
#[derive(Debug)]
enum Settled {
    /// Active: its stream URL.
    Url(String),
    Failed(TonboError),
    /// Not yet: looked up again after the backoff.
    Again(Transient),
}

/// What the database `found` of `name` says: `active` gives its URL, `deleting` and `deleted`
/// are not found, and any other state is looked up again.
fn settle(name: &str, found: Found) -> Settled {
    match found.state.as_str() {
        "active" => match found.url {
            Some(url) => Settled::Url(url),
            None => Settled::Failed(TonboError::NoUrl {
                name: name.to_owned(),
                request_id: found.request_id,
            }),
        },
        "deleting" | "deleted" => Settled::Failed(TonboError::NotFound {
            name: name.to_owned(),
            request_id: found.request_id,
        }),
        state => Settled::Again(Transient::NotActive {
            name: name.to_owned(),
            state: state.to_owned(),
        }),
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
        let backoff = backoff_ms(attempt);
        attempt = attempt.saturating_add(1);
        let passed = match once(name, key, api, auth, until).map(|found| settle(name, found)) {
            Ok(Settled::Url(url)) => return Ok(url),
            Ok(Settled::Failed(e)) | Err(Failure::Final(e)) => return Err(e),
            Ok(Settled::Again(t)) => Some((t, backoff)),
            // A request the end of the resolve cut short says nothing new: the last condition
            // stands.
            Err(Failure::Transient(_)) if until.over() => None,
            Err(Failure::Transient(t)) => {
                let wait = wait_ms(&t, backoff);
                Some((t, wait))
            }
        };
        let wait = match passed {
            Some((t, wait)) => {
                until.note(&t);
                last = Some(t);
                wait
            }
            None => 0,
        };
        if !until.sleep(Duration::from_millis(wait)) {
            return Err(TonboError::GaveUp {
                subject: Subject::Database(name.to_owned()),
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
    let bearer = management_bearer(key, auth, false, until)?;
    let mut answer = get(&url, &bearer.token, until)?;
    if asks_exchange(&answer, bearer.is_key) {
        let bearer = management_bearer(key, auth, true, until)?;
        answer = get(&url, &bearer.token, until)?;
    }
    classify(&answer, name, &key.from)
}

/// Whether `answer` asks the lookup to exchange the key and look up again once: a `401` with
/// `exchange_required`, or one to a token exchanged before (`is_key` false), which expired early.
fn asks_exchange(answer: &Answer, is_key: bool) -> bool {
    answer.status == 401
        && (!is_key || answer.problem().code.as_deref() == Some("exchange_required"))
}

/// What `answer` to the lookup of `name` with the key from `from` says.
fn classify(answer: &Answer, name: &str, from: &KeyFrom) -> Result<Found, Failure> {
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
            request_id: answer.request_id.clone(),
        });
    }
    let problem = answer.problem();
    Err(match status {
        429 => Failure::Transient(Transient::RateLimited {
            retry_after_ms: answer.retry_after_ms,
        }),
        408 | 500..=599 => Failure::Transient(Transient::Unavailable { status }),
        401 => Failure::Final(TonboError::KeyRefused {
            by: Api::Management,
            from: from.clone(),
            code: problem.code,
            request_id: problem.request_id,
        }),
        403 => Failure::Final(TonboError::Forbidden {
            name: name.to_owned(),
            from: from.clone(),
            request_id: problem.request_id,
        }),
        404 => Failure::Final(TonboError::NotFound {
            name: name.to_owned(),
            request_id: problem.request_id,
        }),
        _ => Failure::Final(TonboError::Unexpected {
            api: Api::Management,
            status,
            name: Some(name.to_owned()),
            detail: problem.detail,
            request_id: problem.request_id,
        }),
    })
}

/// A Management API token exchanged for a key.
struct Held {
    token: String,
    expires_at: Instant,
}

/// The Management API tokens of keys that must be exchanged first, by the digest of the auth
/// origin and the key: an entry says the key needs the exchange, its value is the token held.
static MANAGEMENT: Mutex<Option<HashMap<[u8; 32], Option<Held>>>> = Mutex::new(None);

/// The bearer token of a Management API request.
struct Bearer {
    token: String,
    /// The key itself, not a token exchanged for it.
    is_key: bool,
}

/// The bearer token of a Management API request with `key`: the key itself, or the token
/// exchanged for it at `auth`'s token endpoint, anew with `fresh`. The exchange runs outside the
/// lock, within the time `until` leaves it.
fn management_bearer(
    key: &ApiKey,
    auth: &str,
    fresh: bool,
    until: &Until,
) -> Result<Bearer, Failure> {
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
            None if !fresh => {
                return Ok(Bearer {
                    token: key.secret.clone(),
                    is_key: true,
                });
            }
            Some(Some(h)) if !fresh && h.expires_at > Instant::now() => {
                return Ok(Bearer {
                    token: h.token.clone(),
                    is_key: false,
                });
            }
            _ => {}
        }
        tokens.insert(id, None);
    }
    let endpoint = format!("{auth}/api/iam/token");
    let exchanger = Key::new(&key.secret, &endpoint, None)
        .map_err(|e| Failure::Final(TonboError::Unsendable(e)))?;
    let left = until.left().max(Duration::from_millis(MIN_REQUEST_MS));
    let got = exchange(&exchanger, Some(left)).map_err(|e| exchange_failure(e, &key.from))?;
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
    Ok(Bearer {
        token: got.token,
        is_key: false,
    })
}

/// An answer of the Management API, as far as the lookup reads it.
struct Answer {
    status: u16,
    retry_after_ms: Option<u64>,
    request_id: Option<String>,
    body: Vec<u8>,
}

impl Answer {
    /// The problem details of the body, the request id from `X-Request-Id` when the body names
    /// none.
    fn problem(&self) -> Problem {
        Problem::read(&self.body, self.request_id.as_deref())
    }
}

/// `GET url` with `bearer`, given the time `until` leaves it.
fn get(url: &str, bearer: &str, until: &Until) -> Result<Answer, Failure> {
    let left = until.left().max(Duration::from_millis(MIN_REQUEST_MS));
    let mut response = agent()
        .get(url)
        .header("authorization", format!("Bearer {bearer}"))
        .header("accept", "application/json")
        .config()
        .timeout_global(Some(left))
        .build()
        .call()
        .map_err(|e| {
            Failure::Transient(Transient::Unreachable {
                source: Arc::new(e),
            })
        })?;
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
        .map_err(|e| {
            Failure::Transient(Transient::BrokeOff {
                source: Arc::new(e),
            })
        })?;
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
