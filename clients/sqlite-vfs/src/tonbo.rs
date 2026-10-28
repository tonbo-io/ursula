//! Tonbo databases by name (design: Tonbo): `tonbo_resolve(name_or_url, options_json, timeout_ms)`
//! turns the name of a database of a Tonbo account into its stream URL, with the account's API
//! key, and fetches the first Streams token for the key before it returns, so a host opens the URL
//! with `ursula_open` next. Only `extension` calls into this module: the open, the registry and the
//! driver know nothing of Tonbo, and a host that never calls `tonbo_resolve` never runs it.
//!
//! - A name is anything without a scheme (`https://...`): an empty one fails `invalid_name`. A URL
//!   is taken as it is, and no lookup runs.
//! - `options_json` holds `api_key`, `api_origin`, `auth_origin`, `own_token`, `timeout_ms` and
//!   `agent`. `own_token: true` says the host gives the Streams requests a token of its own
//!   (`ursula_open`'s `token`, `ursula_set_token`): no Streams token is exchanged for the key, and
//!   a URL needs no key. So does `URSULA_VFS_TOKEN_FILE`.
//! - The key is the `api_key` option, else `TONBO_API_KEY`, else the login `tonbo login` saves
//!   (`credentials`). The Management API is `api_origin`, else `TONBO_API_ORIGIN`, else
//!   `https://api.tonbo.dev`, and the token endpoint's origin `auth_origin`, else
//!   `TONBO_AUTH_ORIGIN`, else `https://tonbo.dev`. An origin is `https://`, or `http://` to a
//!   loopback host, since requests to it carry the key.
//! - The lookup (`lookup`) runs until the database is active, retrying what passes until the
//!   time runs out. The Streams token is the process's key source (`auth`): the key goes to
//!   `{auth origin}/api/iam/token` with the audience [`STREAMS_AUDIENCE`], and its tokens are
//!   refreshed in the background while requests may carry them. They go only to a stream URL that
//!   is `https://`, or `http://` to a loopback host: another fails `invalid_url`.
//! - With `timeout_ms` 0 the call returns `{done: false, request}` at once, and the resolve goes on
//!   on a thread of its own, for `timeout_ms` of the options (30 s by default). Its end is a
//!   `resolved` event (`ursula_events`), after which the host calls again with the same arguments
//!   and takes the outcome: the answer, or the error. A call with the same arguments while it goes
//!   on joins it, and `ursula_cancel(request)` gives it up. An outcome no call takes goes after a
//!   minute.
//!
//! Every failure is a [`TonboError`]: the message reads `<kind>: <what>`, then ` (request <id>)`
//! for the Tonbo API's request id when an answer failed, then `. <fix>` when there is something
//! to run or set. No message carries a key or a token, and nothing here logs.
//!
//! Module map:
//!
//! - `credentials`: the login `tonbo login` saves, read as the Tonbo CLI's credentials contract,
//!   version 1, says.
//! - `lookup`: the Management API's lookup of a database, its token for a key that must be
//!   exchanged first, and the retries of what passes.

mod credentials;
mod lookup;

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use ureq::http::Uri;

use self::credentials::LoginError;
use self::lookup::Api;
use self::lookup::Transient;
use crate::auth::KeyError;
use crate::driver::runtime::driver;
use crate::error::ErrorKind;
use crate::json::Value;
use crate::json::parse_object;
use crate::json::whole;
use crate::options::OptionsError;
use crate::transport::http::in_clear;

/// The audience of a Streams token.
pub(crate) const STREAMS_AUDIENCE: &str = "https://streams.tonbo.dev";
const DEFAULT_API_ORIGIN: &str = "https://api.tonbo.dev";
const DEFAULT_AUTH_ORIGIN: &str = "https://tonbo.dev";
/// How long a resolve started with `timeout_ms` 0 may take without a `timeout_ms` option.
const DEFAULT_RESOLVE_MS: u64 = 30_000;
/// How much longer than the resolve's own time a blocking call waits, so the resolve's own failure
/// (the last condition) is what it reports.
const GRACE_MS: u64 = 1_000;
/// How long an outcome no call took is kept.
const UNTAKEN_MS: u64 = 60_000;
/// How long `ursula_cancel` waits for what a resolve ended with.
const CANCEL_REPORT_MS: u64 = 1_000;

/// What a resolve hands out: the stream URL, and the database's name when it was looked up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Resolved {
    pub(crate) url: String,
    pub(crate) name: Option<String>,
}

/// What `tonbo_resolve` came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Resolution {
    Done(Resolved),
    /// Still resolving: the request to wait on (a `resolved` event, then a call again) or cancel.
    Pending(u64),
}

/// The API key of a resolve, and where it came from, which says how to fix it. Never printed.
#[derive(Clone)]
pub(crate) struct ApiKey {
    secret: String,
    from: KeyFrom,
}

impl fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApiKey")
            .field("secret", &"..")
            .field("from", &self.from)
            .finish()
    }
}

/// Where a key came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KeyFrom {
    Option,
    Env,
    Login(PathBuf),
}

impl KeyFrom {
    /// What to do about a key that failed.
    fn fix(&self) -> &'static str {
        match self {
            KeyFrom::Option => "Pass a valid api_key",
            KeyFrom::Env => ENV_FIX,
            KeyFrom::Login(_) => credentials::LOGIN_FIX,
        }
    }
}

impl fmt::Display for KeyFrom {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeyFrom::Option => f.write_str("the api_key option"),
            KeyFrom::Env => f.write_str("TONBO_API_KEY"),
            KeyFrom::Login(path) => write!(f, "the tonbo login at {}", path.display()),
        }
    }
}

const ENV_FIX: &str = "Set TONBO_API_KEY to a valid key, or unset it and run: tonbo login";
const MISSING_FIX: &str = "Set TONBO_API_KEY, or run: tonbo login";

/// Why an origin cannot be used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum OriginRefusal {
    #[error("not a URL")]
    NotAUrl,
    #[error("an origin has no path, query or credentials")]
    NotAnOrigin,
    #[error("use https://, since requests carry the API key (http:// only to a loopback host)")]
    InClear,
}

/// What a resolve gave up on when its time ran out or it was cancelled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Subject {
    /// The lookup of a database.
    Database(String),
    /// The Streams token for the stream at a URL.
    Token(String),
}

impl fmt::Display for Subject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Subject::Database(name) => write!(f, "the lookup of database \"{name}\""),
            Subject::Token(url) => write!(f, "the Streams token for {url}"),
        }
    }
}

/// Why `tonbo_resolve` failed. No variant carries a key or a token.
#[derive(Debug)]
pub(crate) enum TonboError {
    /// The name is empty.
    InvalidName,
    Options(OptionsError),
    /// The `api_key` option cannot be sent.
    ApiKeyOption,
    Origin {
        option: &'static str,
        env: &'static str,
        fallback: &'static str,
        given: String,
        why: OriginRefusal,
    },
    /// No `api_key`, no `TONBO_API_KEY` and no `tonbo login`.
    MissingCredentials,
    /// `TONBO_API_KEY` holds no key that can be sent.
    EnvKey,
    Login(LoginError),
    /// `401`, or `403` at the token endpoint: the key was refused.
    KeyRefused {
        by: Api,
        from: KeyFrom,
        code: Option<String>,
        request_id: Option<String>,
    },
    /// `403` to the lookup: the key cannot read the database.
    Forbidden {
        name: String,
        from: KeyFrom,
        request_id: Option<String>,
    },
    /// `404`, or a database being deleted.
    NotFound {
        name: String,
        request_id: Option<String>,
    },
    /// A status outside the API's contract: a redirect (never followed) or another one, for
    /// database `name` when the lookup asked for one.
    Unexpected {
        api: Api,
        status: u16,
        name: Option<String>,
        detail: Option<String>,
        request_id: Option<String>,
    },
    /// An active database without a `url`.
    NoUrl {
        name: String,
        request_id: Option<String>,
    },
    /// The stream at `url` (of database `name`) would get the key's Streams tokens over plain
    /// http to another host.
    Exposed {
        url: String,
        name: Option<String>,
    },
    /// The time ran out, or the resolve was cancelled: the last condition it met, if any.
    GaveUp {
        subject: Subject,
        last: Option<Transient>,
        cancelled: bool,
    },
    /// The process's key source gave no Streams token: another key holds the process, or its
    /// refresher thread failed.
    StreamsToken(KeyError),
    /// The resolve's thread could not be started.
    NoThread(std::io::Error),
    /// The resolve panicked inside the extension.
    Panicked,
}

impl TonboError {
    /// The kind a host reports.
    pub(crate) fn kind(&self) -> ErrorKind {
        match self {
            TonboError::InvalidName => ErrorKind::InvalidName,
            TonboError::Options(_)
            | TonboError::ApiKeyOption
            | TonboError::Origin { .. }
            | TonboError::Unexpected { .. }
            | TonboError::NoUrl { .. } => ErrorKind::InvalidArgument,
            TonboError::MissingCredentials => ErrorKind::MissingCredentials,
            TonboError::EnvKey | TonboError::KeyRefused { .. } | TonboError::Forbidden { .. } => {
                ErrorKind::InvalidApiKey
            }
            TonboError::Login(e) => e.kind(),
            TonboError::NotFound { .. } => ErrorKind::DatabaseNotFound,
            TonboError::Exposed { .. } => ErrorKind::InvalidUrl,
            TonboError::GaveUp { last, .. } => {
                last.as_ref().map_or(ErrorKind::Cancelled, Transient::kind)
            }
            TonboError::StreamsToken(e) => e.kind(),
            TonboError::NoThread(_) | TonboError::Panicked => ErrorKind::Internal,
        }
    }

    /// What to do: the command to run, or the option or variable to set. The message ends with
    /// it.
    pub(crate) fn fix(&self) -> Option<String> {
        match self {
            TonboError::MissingCredentials => Some(MISSING_FIX.to_owned()),
            TonboError::EnvKey => Some(ENV_FIX.to_owned()),
            TonboError::Login(e) => Some(e.fix()),
            TonboError::KeyRefused { from, .. } => Some(from.fix().to_owned()),
            TonboError::Forbidden { name, .. } => {
                Some(format!("Use an API key that can read database \"{name}\""))
            }
            TonboError::NotFound { name, .. } => Some(format!("Run: tonbo database create {name}")),
            TonboError::Unexpected { api, .. } => Some(api.fix().to_owned()),
            TonboError::NoUrl { .. } => Some(Api::Management.fix().to_owned()),
            TonboError::Exposed { .. } => {
                Some("Open an https:// URL, or give the open a token of its own".to_owned())
            }
            TonboError::InvalidName
            | TonboError::Options(_)
            | TonboError::ApiKeyOption
            | TonboError::Origin { .. }
            | TonboError::GaveUp { .. }
            | TonboError::StreamsToken(_)
            | TonboError::NoThread(_)
            | TonboError::Panicked => None,
        }
    }

    /// The Tonbo API's id of the answer that failed, for support.
    pub(crate) fn request_id(&self) -> Option<&str> {
        match self {
            TonboError::KeyRefused { request_id, .. }
            | TonboError::Forbidden { request_id, .. }
            | TonboError::NotFound { request_id, .. }
            | TonboError::Unexpected { request_id, .. }
            | TonboError::NoUrl { request_id, .. } => request_id.as_deref(),
            _ => None,
        }
    }

    /// What failed, after the kind and before the request id and the fix.
    fn what(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TonboError::InvalidName => f.write_str("the database name is empty"),
            TonboError::Options(e) => write!(f, "{e}"),
            TonboError::ApiKeyOption => {
                f.write_str("api_key must be a non-empty string of printable ASCII without spaces")
            }
            TonboError::Origin {
                option,
                env,
                fallback,
                given,
                why,
            } => write!(
                f,
                "{option} (or {env}) must be an origin such as {fallback}, not {given}: {why}"
            ),
            TonboError::MissingCredentials => f.write_str("no Tonbo API key"),
            TonboError::EnvKey => f.write_str("TONBO_API_KEY holds no usable key"),
            TonboError::Login(e) => write!(f, "{e}"),
            TonboError::KeyRefused { by, from, code, .. } => {
                write!(f, "{by} refused the API key from {from}")?;
                match code {
                    Some(code) => write!(f, " ({code})"),
                    None => Ok(()),
                }
            }
            TonboError::Forbidden { name, from, .. } => {
                write!(f, "this key, from {from}, can't read database \"{name}\"")
            }
            TonboError::NotFound { name, .. } => write!(f, "database \"{name}\" not found"),
            TonboError::Unexpected {
                api,
                status,
                name,
                detail,
                ..
            } => {
                write!(f, "{api} answered {status}")?;
                if (300..400).contains(status) {
                    f.write_str(" (a redirect, never followed)")?;
                }
                if let Some(name) = name {
                    write!(f, " for database \"{name}\"")?;
                }
                match detail {
                    Some(detail) => write!(f, ": {detail}"),
                    None => Ok(()),
                }
            }
            TonboError::NoUrl { name, .. } => write!(
                f,
                "{} answered database \"{name}\" active without a url",
                Api::Management
            ),
            TonboError::Exposed { url, name } => {
                write!(f, "the stream URL {url}")?;
                if let Some(name) = name {
                    write!(f, " of database \"{name}\"")?;
                }
                f.write_str(
                    " gets no Streams token for the API key, since it is not https:// (http:// only to a loopback host)",
                )
            }
            TonboError::GaveUp {
                subject,
                last,
                cancelled,
            } => {
                let when = if *cancelled {
                    "it was cancelled"
                } else {
                    "its time ran out"
                };
                write!(f, "{subject} gave up when {when}")?;
                match last {
                    Some(last) => write!(f, ": {last}"),
                    None => f.write_str(", before any answer"),
                }
            }
            TonboError::StreamsToken(e) => write!(f, "{e}"),
            TonboError::NoThread(e) => write!(f, "the resolve's thread could not be started: {e}"),
            TonboError::Panicked => f.write_str("the resolve failed inside the extension"),
        }
    }
}

impl fmt::Display for TonboError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: ", self.kind().as_str())?;
        self.what(f)?;
        if let Some(id) = self.request_id() {
            write!(f, " (request {id})")?;
        }
        match self.fix() {
            Some(fix) => write!(f, ". {fix}"),
            None => Ok(()),
        }
    }
}

impl std::error::Error for TonboError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TonboError::Options(e) => Some(e),
            TonboError::Login(e) => Some(e),
            TonboError::GaveUp { last: Some(t), .. } => Some(t),
            TonboError::StreamsToken(e) => Some(e),
            TonboError::NoThread(e) => Some(e),
            _ => None,
        }
    }
}

/// `tonbo_resolve`'s `options_json`. Not `Debug`: it holds the key.
#[derive(Default)]
struct ResolveOptions {
    api_key: Option<String>,
    api_origin: Option<String>,
    auth_origin: Option<String>,
    own_token: bool,
    timeout_ms: Option<u64>,
}

impl TryFrom<&str> for ResolveOptions {
    type Error = OptionsError;

    /// An empty text, `null` or `{}` asks for the defaults. `agent` is accepted, and not sent.
    fn try_from(text: &str) -> Result<Self, OptionsError> {
        let mut options = ResolveOptions::default();
        let text = text.trim();
        if text.is_empty() || text == "null" {
            return Ok(options);
        }
        let wrong = |name, expected| OptionsError::Type { name, expected };
        for (key, value) in parse_object(text)? {
            match (key.as_str(), value) {
                (_, Value::Null) => {}
                ("api_key", Value::Text(t)) => options.api_key = Some(t),
                ("api_origin", Value::Text(t)) => options.api_origin = Some(t),
                ("auth_origin", Value::Text(t)) => options.auth_origin = Some(t),
                ("agent", Value::Text(_)) => {}
                ("own_token", Value::Bool(b)) => options.own_token = b,
                ("timeout_ms", Value::Number(n)) => {
                    options.timeout_ms = Some(whole(n).filter(|&ms| ms > 0).ok_or_else(|| {
                        wrong("timeout_ms", "a positive whole number of milliseconds")
                    })?);
                }
                ("api_key", _) => return Err(wrong("api_key", "a string")),
                ("api_origin", _) => return Err(wrong("api_origin", "a string")),
                ("auth_origin", _) => return Err(wrong("auth_origin", "a string")),
                ("agent", _) => return Err(wrong("agent", "a string")),
                ("own_token", _) => return Err(wrong("own_token", "a boolean")),
                ("timeout_ms", _) => return Err(wrong("timeout_ms", "a number")),
                (other, _) => return Err(OptionsError::Unknown(other.to_owned())),
            }
        }
        Ok(options)
    }
}

/// What a resolve does, decided before any request: the database to look up or the URL, the key
/// whose tokens serve the Streams requests, and the origins.
#[derive(Debug)]
struct Plan {
    target: Target,
    /// The key whose Streams token the resolve fetches: none with `own_token` or
    /// `URSULA_VFS_TOKEN_FILE`.
    streams: Option<ApiKey>,
    api: String,
    auth: String,
    timeout_ms: Option<u64>,
}

#[derive(Debug)]
enum Target {
    /// A database to look up with the key.
    Name { name: String, key: ApiKey },
    /// A stream URL, taken as it is.
    Url(String),
}

impl Plan {
    /// The plan of `tonbo_resolve(name_or_url, options_json, ..)`: the options, the origins and
    /// the key, in that order. Reads the environment and the login file, and sends nothing.
    fn new(name_or_url: &str, options: &str) -> Result<Self, TonboError> {
        let url = has_scheme(name_or_url);
        if !url && name_or_url.is_empty() {
            return Err(TonboError::InvalidName);
        }
        let options = ResolveOptions::try_from(options).map_err(TonboError::Options)?;
        let api = origin(
            options.api_origin.as_deref(),
            "api_origin",
            "TONBO_API_ORIGIN",
            DEFAULT_API_ORIGIN,
        )?;
        let auth = origin(
            options.auth_origin.as_deref(),
            "auth_origin",
            "TONBO_AUTH_ORIGIN",
            DEFAULT_AUTH_ORIGIN,
        )?;
        let streams_key = !options.own_token && crate::config::token_file().is_none();
        let key = if !url || streams_key {
            Some(api_key(options.api_key.as_deref())?)
        } else {
            None
        };
        let target = match (url, key.clone()) {
            (false, Some(key)) => Target::Name {
                name: name_or_url.to_owned(),
                key,
            },
            (false, None) => return Err(TonboError::MissingCredentials),
            (true, _) => Target::Url(name_or_url.to_owned()),
        };
        Ok(Plan {
            target,
            streams: key.filter(|_| streams_key),
            api,
            auth,
            timeout_ms: options.timeout_ms,
        })
    }

    /// Whether the resolve sends any request.
    fn sends(&self) -> bool {
        matches!(self.target, Target::Name { .. }) || self.streams.is_some()
    }

    /// Carries the plan out until `until` ends it.
    fn run(&self, until: &Until) -> Result<Resolved, TonboError> {
        let (url, name) = match &self.target {
            Target::Name { name, key } => (
                lookup::look_up(name, key, &self.api, &self.auth, until)?,
                Some(name.clone()),
            ),
            Target::Url(url) => (url.clone(), None),
        };
        if let Some(key) = &self.streams {
            refuse_exposed(&url, name.as_deref())?;
            streams_token(key, &self.auth, &url, until)?;
        }
        Ok(Resolved { url, name })
    }
}

/// Whether `s` starts with a URL scheme and `://`.
fn has_scheme(s: &str) -> bool {
    let Some((scheme, _)) = s.split_once("://") else {
        return false;
    };
    let mut chars = scheme.chars();
    chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '.' | '-'))
}

/// The variable `name`, trimmed: `None` when it is unset or empty.
fn env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty())
}

/// The origin `option`, else the variable `env_name`, else `fallback`, as `scheme://host[:port]`.
fn origin(
    option: Option<&str>,
    option_name: &'static str,
    env_name: &'static str,
    fallback: &'static str,
) -> Result<String, TonboError> {
    let given = option
        .map(str::to_owned)
        .or_else(|| env(env_name))
        .unwrap_or_else(|| fallback.to_owned());
    let refuse = |why| TonboError::Origin {
        option: option_name,
        env: env_name,
        fallback,
        given: given.clone(),
        why,
    };
    let uri = given
        .parse::<Uri>()
        .map_err(|_not_a_uri| refuse(OriginRefusal::NotAUrl))?;
    let (Some(scheme), Some(authority)) = (uri.scheme_str(), uri.authority()) else {
        return Err(refuse(OriginRefusal::NotAUrl));
    };
    let path = uri.path_and_query().map_or("", |p| p.as_str());
    if !matches!(path, "" | "/") || authority.as_str().contains('@') || given.contains('#') {
        return Err(refuse(OriginRefusal::NotAnOrigin));
    }
    if !matches!(scheme, "http" | "https") || in_clear(&uri) {
        return Err(refuse(OriginRefusal::InClear));
    }
    Ok(format!(
        "{}://{}",
        scheme.to_ascii_lowercase(),
        authority.as_str().to_ascii_lowercase()
    ))
}

/// The key: the option, else `TONBO_API_KEY`, else the login file.
fn api_key(option: Option<&str>) -> Result<ApiKey, TonboError> {
    let sendable = |key: &str| crate::auth::usable(key).as_deref() == Some(key);
    if let Some(key) = option {
        if !sendable(key) {
            return Err(TonboError::ApiKeyOption);
        }
        return Ok(ApiKey {
            secret: key.to_owned(),
            from: KeyFrom::Option,
        });
    }
    if let Some(key) = env("TONBO_API_KEY") {
        if !sendable(&key) {
            return Err(TonboError::EnvKey);
        }
        return Ok(ApiKey {
            secret: key,
            from: KeyFrom::Env,
        });
    }
    let path = credentials::path().ok_or(TonboError::MissingCredentials)?;
    match credentials::read(&path, now_ms()) {
        Ok(Some(secret)) => Ok(ApiKey {
            secret,
            from: KeyFrom::Login(path),
        }),
        Ok(None) => Err(TonboError::MissingCredentials),
        Err(e) => Err(TonboError::Login(e)),
    }
}

/// Milliseconds since the Unix epoch, on the system clock.
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// Refuses the key's Streams tokens to the stream at `url` (of database `name`) over plain http to
/// another host. A URL that does not parse is `ursula_open`'s to refuse, before any request.
fn refuse_exposed(url: &str, name: Option<&str>) -> Result<(), TonboError> {
    match url.parse::<Uri>() {
        Ok(uri) if in_clear(&uri) => Err(TonboError::Exposed {
            url: url.to_owned(),
            name: name.map(str::to_owned),
        }),
        _ => Ok(()),
    }
}

/// Makes `key` the process's key source for Streams tokens, exchanged at `auth`'s token endpoint,
/// and waits until it holds one, at most until `until` ends: a bad key fails here, before the open.
fn streams_token(key: &ApiKey, auth: &str, url: &str, until: &Until) -> Result<(), TonboError> {
    let endpoint = format!("{auth}/api/iam/token");
    let outcome =
        crate::auth::use_key(&key.secret, &endpoint, Some(STREAMS_AUDIENCE), until.left());
    let gave_up = |last| TonboError::GaveUp {
        subject: Subject::Token(url.to_owned()),
        last,
        cancelled: until.cancelled(),
    };
    match outcome {
        Ok(()) => Ok(()),
        Err(KeyError::Exchange(e)) => match lookup::exchange_failure(e, &key.from) {
            lookup::Failure::Final(e) => Err(e),
            lookup::Failure::Transient(t) => Err(gave_up(Some(t))),
        },
        Err(KeyError::TimedOut { last, .. }) => {
            match last.map(|e| lookup::exchange_failure(e, &key.from)) {
                Some(lookup::Failure::Final(e)) => Err(e),
                Some(lookup::Failure::Transient(t)) => Err(gave_up(Some(t))),
                None => Err(gave_up(None)),
            }
        }
        Err(e) => Err(TonboError::StreamsToken(e)),
    }
}

/// When a resolve ends: its deadline, or a cancel.
#[derive(Debug)]
pub(crate) struct Until {
    deadline: Instant,
    cancel: Arc<Cancel>,
}

/// A resolve's cancel, which wakes its waits.
#[derive(Debug, Default)]
struct Cancel {
    cancelled: Mutex<bool>,
    changed: Condvar,
}

impl Cancel {
    fn cancel(&self) {
        *lock(&self.cancelled) = true;
        self.changed.notify_all();
    }
}

impl Until {
    fn new(timeout: Duration, cancel: Arc<Cancel>) -> Self {
        let now = Instant::now();
        Self {
            deadline: now.checked_add(timeout).unwrap_or(now),
            cancel,
        }
    }

    /// The time left, zero once it ended.
    pub(crate) fn left(&self) -> Duration {
        if self.cancelled() {
            return Duration::ZERO;
        }
        self.deadline.saturating_duration_since(Instant::now())
    }

    pub(crate) fn cancelled(&self) -> bool {
        *lock(&self.cancel.cancelled)
    }

    /// Whether the resolve ended: its time ran out, or it was cancelled.
    pub(crate) fn over(&self) -> bool {
        self.left().is_zero()
    }

    /// Waits `wait`, or until the resolve ends: whether it may go on afterwards.
    pub(crate) fn sleep(&self, wait: Duration) -> bool {
        let end = Instant::now()
            .checked_add(wait)
            .map_or(self.deadline, |at| at.min(self.deadline));
        let mut cancelled = lock(&self.cancel.cancelled);
        loop {
            let left = end.saturating_duration_since(Instant::now());
            if *cancelled || left.is_zero() {
                break;
            }
            cancelled = self
                .cancel
                .changed
                .wait_timeout(cancelled, left)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        drop(cancelled);
        !self.over()
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A resolve's outcome, shared by the call that takes it and `ursula_cancel`.
type Outcome = Result<Resolved, Arc<TonboError>>;

/// A resolve started by a call, by the digest of its arguments.
struct Entry {
    request: u64,
    subject: Subject,
    cancel: Arc<Cancel>,
    outcome: Option<Outcome>,
    /// When the outcome arrived: one no call takes goes after [`UNTAKEN_MS`].
    done_at: Option<Instant>,
    /// Cancelled: no call takes its outcome, and the resolve drops its entry when it ends.
    abandoned: bool,
    worker: Option<JoinHandle<()>>,
}

/// The resolves in progress, and outcomes not taken yet.
static RESOLVES: Mutex<Option<HashMap<[u8; 32], Entry>>> = Mutex::new(None);
/// Signalled when a resolve's outcome arrives.
static FINISHED: Condvar = Condvar::new();

/// The digest of a call's arguments: the key of its resolve. The arguments may hold a key, which
/// the table keeps no copy of.
fn digest(name_or_url: &str, options: &str) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(name_or_url.as_bytes());
    hasher.update(&[0]);
    hasher.update(options.as_bytes());
    *hasher.finalize().as_bytes()
}

/// `tonbo_resolve(name_or_url, options_json, timeout_ms)`: the resolve of those arguments in
/// progress or done, taken or waited for, or a new one. A resolve that sends nothing answers at
/// once.
pub(crate) fn resolve(
    name_or_url: &str,
    options: &str,
    timeout_ms: u64,
) -> Result<Resolution, Arc<TonboError>> {
    let id = digest(name_or_url, options);
    let request = {
        let mut table = lock(&RESOLVES);
        let entries = table.get_or_insert_with(HashMap::new);
        sweep(entries);
        entries.get(&id).filter(|e| !e.abandoned).map(|e| e.request)
    };
    let request = match request {
        Some(request) => request,
        None => {
            let plan = Plan::new(name_or_url, options).map_err(Arc::new)?;
            if !plan.sends() {
                return plan
                    .run(&Until::new(Duration::ZERO, Arc::default()))
                    .map(Resolution::Done)
                    .map_err(Arc::new);
            }
            let ms = if timeout_ms > 0 {
                timeout_ms
            } else {
                plan.timeout_ms.unwrap_or(DEFAULT_RESOLVE_MS)
            };
            start(id, subject(name_or_url), plan, Duration::from_millis(ms))?
        }
    };
    if timeout_ms == 0 {
        return Ok(take(id, request)?.map_or(Resolution::Pending(request), Resolution::Done));
    }
    let wait = Duration::from_millis(timeout_ms.saturating_add(GRACE_MS));
    let deadline = Instant::now().checked_add(wait);
    let mut table = lock(&RESOLVES);
    loop {
        let entries = table.get_or_insert_with(HashMap::new);
        let waiting = entries
            .get(&id)
            .is_some_and(|e| e.request == request && e.outcome.is_none());
        if !waiting {
            break;
        }
        let left = deadline
            .and_then(|d| d.checked_duration_since(Instant::now()))
            .unwrap_or_default();
        if left.is_zero() {
            if let Some(entry) = entries.get_mut(&id) {
                entry.abandoned = true;
                entry.cancel.cancel();
            }
            return Err(Arc::new(TonboError::GaveUp {
                subject: subject(name_or_url),
                last: None,
                cancelled: false,
            }));
        }
        table = FINISHED
            .wait_timeout(table, left)
            .unwrap_or_else(PoisonError::into_inner)
            .0;
    }
    drop(table);
    // Gone without an outcome: another call took it, or it was cancelled.
    take(id, request)?.map(Resolution::Done).ok_or_else(|| {
        Arc::new(TonboError::GaveUp {
            subject: subject(name_or_url),
            last: None,
            cancelled: true,
        })
    })
}

/// What a resolve of `name_or_url` is about.
fn subject(name_or_url: &str) -> Subject {
    if has_scheme(name_or_url) {
        Subject::Token(name_or_url.to_owned())
    } else {
        Subject::Database(name_or_url.to_owned())
    }
}

/// Starts the resolve of `plan` (of `subject`) under `id`, for at most `timeout`, on a thread of
/// its own: its request id. A call with the same arguments that started one meanwhile is joined.
fn start(
    id: [u8; 32],
    subject: Subject,
    plan: Plan,
    timeout: Duration,
) -> Result<u64, Arc<TonboError>> {
    let request = driver().request_id();
    let cancel = Arc::new(Cancel::default());
    let until = Until::new(timeout, Arc::clone(&cancel));
    let mut table = lock(&RESOLVES);
    let entries = table.get_or_insert_with(HashMap::new);
    if let Some(running) = entries.get(&id).filter(|e| !e.abandoned) {
        return Ok(running.request);
    }
    entries.insert(id, Entry {
        request,
        subject,
        cancel,
        outcome: None,
        done_at: None,
        abandoned: false,
        worker: None,
    });
    let spawned = std::thread::Builder::new()
        .name("ursula-vfs-tonbo".to_owned())
        .spawn(move || {
            let outcome = crate::guard::contain(
                "tonbo_resolve",
                || Err(Arc::new(TonboError::Panicked)),
                || plan.run(&until).map_err(Arc::new),
            );
            finish(id, request, outcome);
        });
    match spawned {
        Ok(handle) => {
            if let Some(entry) = entries.get_mut(&id).filter(|e| e.request == request) {
                entry.worker = Some(handle);
            }
            Ok(request)
        }
        Err(e) => {
            entries.remove(&id);
            Err(Arc::new(TonboError::NoThread(e)))
        }
    }
}

/// The resolve `request` under `id` ended with `outcome`: it waits for a call to take it, and a
/// `resolved` event says so. A cancelled one drops its entry.
fn finish(id: [u8; 32], request: u64, outcome: Outcome) {
    let ok = outcome.is_ok();
    let kept = {
        let mut table = lock(&RESOLVES);
        let entries = table.get_or_insert_with(HashMap::new);
        let abandoned = entries
            .get(&id)
            .filter(|e| e.request == request)
            .map(|e| e.abandoned);
        match abandoned {
            Some(false) => {
                if let Some(entry) = entries.get_mut(&id) {
                    entry.outcome = Some(outcome);
                    entry.done_at = Some(Instant::now());
                }
                true
            }
            Some(true) => {
                // Its own handle goes with it: the thread is ending.
                entries.remove(&id);
                false
            }
            None => false,
        }
    };
    FINISHED.notify_all();
    if kept {
        driver().resolved(request, ok);
    }
}

/// Takes the outcome of resolve `request` under `id` when it arrived: `Ok(None)` while it goes on.
fn take(id: [u8; 32], request: u64) -> Result<Option<Resolved>, Arc<TonboError>> {
    let entry = {
        let mut table = lock(&RESOLVES);
        let entries = table.get_or_insert_with(HashMap::new);
        let done = entries
            .get(&id)
            .is_some_and(|e| e.request == request && e.outcome.is_some());
        if !done {
            return Ok(None);
        }
        entries.remove(&id)
    };
    let Some(mut entry) = entry else {
        return Ok(None);
    };
    join(entry.worker.take());
    match entry.outcome.take() {
        Some(Ok(resolved)) => Ok(Some(resolved)),
        Some(Err(e)) => Err(e),
        None => Ok(None),
    }
}

/// Joins a resolve's thread, which has stored its outcome and is ending.
fn join(worker: Option<JoinHandle<()>>) {
    if let Some(handle) = worker
        && handle.join().is_err()
    {
        crate::log::emit(crate::log::Level::Warn, "tonbo_resolve_panicked", &[]);
    }
}

/// Drops the outcomes no call took within [`UNTAKEN_MS`].
fn sweep(entries: &mut HashMap<[u8; 32], Entry>) {
    let limit = Duration::from_millis(UNTAKEN_MS);
    entries.retain(|_, e| e.done_at.is_none_or(|at| at.elapsed() < limit));
}

/// `ursula_cancel(request)` of a resolve: `None` when `request` names none. Otherwise what it ended
/// with, waiting for it at most [`CANCEL_REPORT_MS`]: `Some(None)` when it resolved before the
/// cancel, the failure it ended with, or `cancelled` with the last condition it met.
pub(crate) fn cancel(request: u64) -> Option<Option<Arc<TonboError>>> {
    let mut table = lock(&RESOLVES);
    let entries = table.get_or_insert_with(HashMap::new);
    let (id, subject) = entries
        .iter()
        .find(|(_, e)| e.request == request && !e.abandoned)
        .map(|(id, e)| (*id, e.subject.clone()))?;
    if let Some(entry) = entries.get(&id) {
        entry.cancel.cancel();
    }
    let deadline = Instant::now().checked_add(Duration::from_millis(CANCEL_REPORT_MS));
    loop {
        let entries = table.get_or_insert_with(HashMap::new);
        let done = entries
            .get(&id)
            .is_none_or(|e| e.request != request || e.outcome.is_some());
        let left = deadline
            .and_then(|d| d.checked_duration_since(Instant::now()))
            .unwrap_or_default();
        if done || left.is_zero() {
            break;
        }
        table = FINISHED
            .wait_timeout(table, left)
            .unwrap_or_else(PoisonError::into_inner)
            .0;
    }
    let entries = table.get_or_insert_with(HashMap::new);
    let arrived = entries
        .get(&id)
        .filter(|e| e.request == request)
        .map(|e| e.outcome.is_some());
    let ended = match arrived {
        Some(true) => entries
            .remove(&id)
            .map(|mut e| (e.worker.take(), e.outcome.take())),
        Some(false) => {
            if let Some(entry) = entries.get_mut(&id) {
                entry.abandoned = true;
            }
            None
        }
        None => None,
    };
    drop(table);
    Some(match ended {
        Some((worker, outcome)) => {
            join(worker);
            match outcome {
                Some(Err(e)) => Some(e),
                Some(Ok(_)) | None => None,
            }
        }
        None => Some(Arc::new(TonboError::GaveUp {
            subject,
            last: None,
            cancelled: true,
        })),
    })
}
