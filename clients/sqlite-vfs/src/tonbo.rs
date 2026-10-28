//! Tonbo databases by name (design: Tonbo): `tonbo_resolve(name_or_url, options_json, timeout_ms)`
//! turns the name of a database of a Tonbo account into its stream URL, with the account's API
//! key, and fetches the first Streams token for the key before it returns, so a host opens the URL
//! with `ursula_open` next. Only `extension` calls into this module, and `status` and `error`
//! render what it returns: the open, the registry and the driver know nothing of Tonbo, and a host
//! that never calls `tonbo_resolve` never runs it.
//!
//! - A name is anything without a scheme (`https://...`): an empty one fails `invalid_name`. A URL
//!   is taken as it is, and no lookup runs. A stream URL `ursula_open` would refuse fails
//!   `invalid_url` before any request, as the open's own check (`generation::canonical_path`)
//!   says.
//! - `options_json` holds `api_key`, `api_origin`, `auth_origin`, `own_token`, `timeout_ms` and
//!   `agent`. `own_token: true` says the host gives the Streams requests a token of its own
//!   (`ursula_open`'s `token`, `ursula_set_token`): no Streams token is exchanged for the key, and
//!   a URL needs no key. So does `URSULA_VFS_TOKEN_FILE`.
//! - The key is the `api_key` option, else `TONBO_API_KEY`, else the login `tonbo login` saves
//!   (`credentials`). The Management API is `api_origin`, else `TONBO_API_ORIGIN`, else
//!   `https://api.tonbo.dev`, and the token endpoint's origin `auth_origin`, else
//!   `TONBO_AUTH_ORIGIN`, else `https://tonbo.dev`. An origin is `https://`, or `http://` to a
//!   loopback host, since requests to it carry the key. An origin option other than the one the
//!   environment gives takes only a key passed beside it: any SQL may name options, so a key from
//!   the environment or the login goes only where the environment says.
//! - The lookup (`lookup`) runs until the database is active, retrying what passes until the
//!   time runs out. The Streams token is the process's key source (`auth`): the key goes to
//!   `{auth origin}/api/iam/token` with the audience [`STREAMS_AUDIENCE`], and its tokens are
//!   refreshed in the background while requests may carry them. They go only to a stream URL that
//!   is `https://`, or `http://` to a loopback host: another fails `invalid_url`. Every request
//!   and wait is bounded by the time the resolve has left, and a cancel ends them.
//! - With `timeout_ms` 0 the call returns `{done: false, request}` at once, and the resolve goes on
//!   on a thread of its own, for `timeout_ms` of the options (30 s by default). Its end is a
//!   `resolved` event (`ursula_events`), always, after which the host calls again with the same
//!   arguments and takes the outcome: the answer, or the error. Calls with the same arguments
//!   join one resolve, and `ursula_cancel(request)` gives it up for the host that cancels
//!   (`requests` says which call takes what). A call that finds its resolve given up starts one,
//!   with another request to wait for.
//! - A timeout too large for the clock waits a year: a host's "forever".
//!
//! Every failure is a [`TonboError`], reported as [`TonboError::report`] writes it: `<kind>:
//! <what>`, then ` (request <id>)` for the Tonbo API's request id when an answer failed, then
//! `. <fix>` when there is something to run or set. Messages name options in words every host's
//! spelling of them fits ("the API key option"): only a malformed `options_json` names its
//! members. No message carries a key or a token, and nothing here logs.
//!
//! Module map:
//!
//! - `credentials`: the login `tonbo login` saves, read as the Tonbo CLI's credentials contract,
//!   version 1, says.
//! - `lookup`: the Management API's lookup of a database, its token for a key that must be
//!   exchanged first, and the retries of what passes.
//! - `requests`: the resolves in progress as a pure table: which call waits for which, and which
//!   takes its outcome.

mod credentials;
mod lookup;
mod requests;

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::OnceLock;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use ureq::http::Uri;
use ureq::http::uri::InvalidUri;

use self::credentials::LoginError;
use self::lookup::Api;
use self::lookup::Transient;
use self::requests::Arrival;
use self::requests::Cancelled;
use self::requests::GiveUp;
use self::requests::Id;
use self::requests::Poll;
use self::requests::Table;
use crate::auth::KeyError;
use crate::auth::exchange::KeyRefusal;
use crate::driver::runtime::driver;
use crate::error::ErrorKind;
use crate::generation::PathError;
use crate::generation::canonical_path;
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
/// How long `ursula_cancel` waits for what a resolve it gave up ended with.
const CANCEL_REPORT_MS: u64 = 1_000;
/// The longest wait, a year: a timeout too large for the clock waits this long.
const FOREVER: Duration = Duration::from_secs(31_536_000);

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
            KeyFrom::Option => "Pass a valid API key",
            KeyFrom::Env => ENV_FIX,
            KeyFrom::Login(_) => credentials::LOGIN_FIX,
        }
    }
}

impl fmt::Display for KeyFrom {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeyFrom::Option => f.write_str("the API key option"),
            KeyFrom::Env => f.write_str("TONBO_API_KEY"),
            KeyFrom::Login(path) => write!(f, "the tonbo login at {}", path.display()),
        }
    }
}

const ENV_FIX: &str = "Set TONBO_API_KEY to a valid key, or unset it and run: tonbo login";
const MISSING_FIX: &str = "Set TONBO_API_KEY, or run: tonbo login";

/// Text as one shell word in a fix: as it is when it needs no quoting, single-quoted otherwise.
struct ShellWord<'a>(&'a str);

impl fmt::Display for ShellWord<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let plain = !self.0.is_empty()
            && self
                .0
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "_@%+=:,./-".contains(c));
        if plain {
            f.write_str(self.0)
        } else {
            write!(f, "'{}'", self.0.replace('\'', "'\\''"))
        }
    }
}

/// The origin options, and the variables and defaults behind them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OriginOption {
    /// `api_origin`: the Management API.
    Api,
    /// `auth_origin`: the token endpoint.
    Auth,
}

impl OriginOption {
    fn env(self) -> &'static str {
        match self {
            OriginOption::Api => "TONBO_API_ORIGIN",
            OriginOption::Auth => "TONBO_AUTH_ORIGIN",
        }
    }

    fn fallback(self) -> &'static str {
        match self {
            OriginOption::Api => DEFAULT_API_ORIGIN,
            OriginOption::Auth => DEFAULT_AUTH_ORIGIN,
        }
    }
}

impl fmt::Display for OriginOption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            OriginOption::Api => "the API origin option",
            OriginOption::Auth => "the auth origin option",
        })
    }
}

/// Why an origin cannot be used.
#[derive(Debug, thiserror::Error)]
pub(crate) enum OriginRefusal {
    #[error("not a URL: {0}")]
    NotAUrl(#[source] InvalidUri),
    #[error("not an absolute URL")]
    NotAbsolute,
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

/// Why `tonbo_resolve` failed: its message is what failed, which [`TonboError::report`] frames
/// with the kind, the request id and the fix. No variant carries a key or a token.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TonboError {
    #[error("the database name is empty")]
    InvalidName,
    #[error(transparent)]
    Options(OptionsError),
    /// The API key option cannot be sent.
    #[error("the API key option must be a non-empty string of printable ASCII without spaces")]
    ApiKeyOption,
    #[error(
        "{option} (or {}) must be an origin such as {}, not {given}: {why}",
        .option.env(),
        .option.fallback()
    )]
    Origin {
        option: OriginOption,
        given: String,
        #[source]
        why: OriginRefusal,
    },
    /// An origin option other than the environment's, without the API key option beside it.
    #[error(
        "{option} names {given}, which gets only an API key passed beside it: TONBO_API_KEY and the tonbo login go only to {ambient}"
    )]
    AmbientKeyElsewhere {
        option: OriginOption,
        given: String,
        ambient: String,
    },
    /// No API key option, no `TONBO_API_KEY` and no `tonbo login`.
    #[error("no Tonbo API key")]
    MissingCredentials,
    /// `TONBO_API_KEY` holds no key that can be sent.
    #[error("TONBO_API_KEY holds no usable key")]
    EnvKey,
    #[error(transparent)]
    Login(LoginError),
    /// `401`, or `403` at the token endpoint: the key was refused.
    #[error(
        "{by} refused the API key from {from}{}",
        .code.as_ref().map(|code| format!(" ({code})")).unwrap_or_default()
    )]
    KeyRefused {
        by: Api,
        from: KeyFrom,
        code: Option<String>,
        request_id: Option<String>,
    },
    /// `403` to the lookup: the key cannot read the database.
    #[error("this key, from {from}, can't read database \"{name}\"")]
    Forbidden {
        name: String,
        from: KeyFrom,
        request_id: Option<String>,
    },
    /// `404`, or a database being deleted.
    #[error("database \"{name}\" not found")]
    NotFound {
        name: String,
        request_id: Option<String>,
    },
    /// A status outside the API's contract: a redirect (never followed) or another one, for
    /// database `name` when the lookup asked for one.
    #[error("{api} answered {status}{}", unexpected(.status, .name.as_deref(), .detail.as_deref()))]
    Unexpected {
        api: Api,
        status: u16,
        name: Option<String>,
        detail: Option<String>,
        request_id: Option<String>,
    },
    /// An active database without a `url`.
    #[error(
        "{} answered database \"{name}\" active without a url",
        Api::Management
    )]
    NoUrl {
        name: String,
        request_id: Option<String>,
    },
    /// A stream URL (of database `name`) `ursula_open` refuses.
    #[error("{source}{}", of_database(.name.as_deref()))]
    InvalidUrl {
        name: Option<String>,
        #[source]
        source: PathError,
    },
    /// A stream URL (of database `name`) that does not parse as one.
    #[error("the stream URL {url}{} is not a URL: {source}", of_database(.name.as_deref()))]
    UnparsableUrl {
        url: String,
        name: Option<String>,
        #[source]
        source: InvalidUri,
    },
    /// The stream at `url` (of database `name`) would get the key's Streams tokens over plain
    /// http to another host.
    #[error(
        "the stream URL {url}{} gets no Streams token for the API key, since it is not https:// (http:// only to a loopback host)",
        of_database(.name.as_deref())
    )]
    Exposed { url: String, name: Option<String> },
    /// The time ran out, or the resolve was cancelled: the last condition it met, if any.
    #[error("{subject} gave up when {}", gave_up_text(.cancelled, .last.as_ref()))]
    GaveUp {
        subject: Subject,
        #[source]
        last: Option<Transient>,
        cancelled: bool,
    },
    /// The process's key source gave no Streams token: another key holds the process, or its
    /// refresher thread failed.
    #[error(transparent)]
    StreamsToken(KeyError),
    /// The key cannot go to the token endpoint as given.
    #[error(transparent)]
    Unsendable(KeyRefusal),
    /// The resolve's thread could not be started.
    #[error("the resolve's thread could not be started: {0}")]
    NoThread(#[source] std::io::Error),
    /// The resolve panicked inside the extension.
    #[error("the resolve failed inside the extension")]
    Panicked,
}

/// ` of database "name"`, or nothing.
fn of_database(name: Option<&str>) -> String {
    name.map(|name| format!(" of database \"{name}\""))
        .unwrap_or_default()
}

/// What follows an unexpected `status`: that a redirect was not followed, the database `name`
/// asked for, and the problem's `detail`.
fn unexpected(status: &u16, name: Option<&str>, detail: Option<&str>) -> String {
    let mut text = String::new();
    if (300..400).contains(status) {
        text.push_str(" (a redirect, never followed)");
    }
    if let Some(name) = name {
        text.push_str(&format!(" for database \"{name}\""));
    }
    if let Some(detail) = detail {
        text.push_str(&format!(": {detail}"));
    }
    text
}

/// What follows `gave up when`: why, and the last condition.
fn gave_up_text(cancelled: &bool, last: Option<&Transient>) -> String {
    let when = if *cancelled {
        "it was cancelled"
    } else {
        "its time ran out"
    };
    match last {
        Some(last) => format!("{when}: {last}"),
        None => format!("{when}, before any answer"),
    }
}

impl TonboError {
    /// The kind a host reports.
    pub(crate) fn kind(&self) -> ErrorKind {
        match self {
            TonboError::InvalidName => ErrorKind::InvalidName,
            TonboError::Options(_)
            | TonboError::ApiKeyOption
            | TonboError::Origin { .. }
            | TonboError::AmbientKeyElsewhere { .. }
            | TonboError::Unexpected { .. }
            | TonboError::NoUrl { .. }
            | TonboError::Unsendable(_) => ErrorKind::InvalidArgument,
            TonboError::MissingCredentials => ErrorKind::MissingCredentials,
            TonboError::EnvKey | TonboError::KeyRefused { .. } | TonboError::Forbidden { .. } => {
                ErrorKind::InvalidApiKey
            }
            TonboError::Login(e) => e.kind(),
            TonboError::NotFound { .. } => ErrorKind::DatabaseNotFound,
            TonboError::InvalidUrl { .. }
            | TonboError::UnparsableUrl { .. }
            | TonboError::Exposed { .. } => ErrorKind::InvalidUrl,
            TonboError::GaveUp { last, .. } => {
                last.as_ref().map_or(ErrorKind::Cancelled, Transient::kind)
            }
            TonboError::StreamsToken(e) => e.kind(),
            TonboError::NoThread(_) | TonboError::Panicked => ErrorKind::Internal,
        }
    }

    /// What to do: the command to run, or the option or variable to set. The report ends with
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
            TonboError::NotFound { name, .. } => {
                Some(format!("Run: tonbo database create {}", ShellWord(name)))
            }
            TonboError::Unexpected { api, .. } => Some(api.fix().to_owned()),
            TonboError::NoUrl { .. } => Some(Api::Management.fix().to_owned()),
            TonboError::AmbientKeyElsewhere { option, .. } => Some(format!(
                "Pass the API key option with it, or set {} instead",
                option.env()
            )),
            TonboError::Exposed { .. } => {
                Some("Open an https:// URL, or give the open a token of its own".to_owned())
            }
            TonboError::InvalidName
            | TonboError::Options(_)
            | TonboError::ApiKeyOption
            | TonboError::Origin { .. }
            | TonboError::InvalidUrl { .. }
            | TonboError::UnparsableUrl { .. }
            | TonboError::GaveUp { .. }
            | TonboError::StreamsToken(_)
            | TonboError::Unsendable(_)
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

    /// The error as a host reports it: `<kind>: <what>`, then ` (request <id>)`, then `. <fix>`.
    pub(crate) fn report(&self) -> Report<'_> {
        Report(self)
    }
}

/// [`TonboError::report`].
pub(crate) struct Report<'a>(&'a TonboError);

impl fmt::Display for Report<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let e = self.0;
        write!(f, "{}: {e}", e.kind().as_str())?;
        if let Some(id) = e.request_id() {
            write!(f, " (request {id})")?;
        }
        match e.fix() {
            Some(fix) => write!(f, ". {fix}"),
            None => Ok(()),
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
    /// The plan of `tonbo_resolve(name_or_url, options_json, ..)`: the options, the origins, the
    /// URL and the key, in that order. Reads the environment and the login file, and sends
    /// nothing.
    fn new(name_or_url: &str, options: &str) -> Result<Self, TonboError> {
        let url = has_scheme(name_or_url);
        if !url && name_or_url.is_empty() {
            return Err(TonboError::InvalidName);
        }
        let options = ResolveOptions::try_from(options).map_err(TonboError::Options)?;
        let api = origin(options.api_origin.as_deref(), OriginOption::Api)?;
        let auth = origin(options.auth_origin.as_deref(), OriginOption::Auth)?;
        let streams_key = !options.own_token && crate::config::token_file().is_none();
        if url {
            check_url(name_or_url, None, streams_key)?;
        }
        let key = if !url || streams_key {
            Some(api_key(options.api_key.as_deref())?)
        } else {
            None
        };
        // Any SQL may name the origins: a key it did not pass goes only where the environment
        // says.
        if key.as_ref().is_some_and(|key| key.from != KeyFrom::Option) {
            for (given, option, chosen) in [
                (&options.api_origin, OriginOption::Api, &api),
                (&options.auth_origin, OriginOption::Auth, &auth),
            ] {
                if given.is_none() {
                    continue;
                }
                let ambient = origin(None, option)?;
                if *chosen != ambient {
                    return Err(TonboError::AmbientKeyElsewhere {
                        option,
                        given: chosen.clone(),
                        ambient,
                    });
                }
            }
        }
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
            Target::Name { name, key } => {
                let url = lookup::look_up(name, key, &self.api, &self.auth, until)?;
                check_url(&url, Some(name.as_str()), self.streams.is_some())?;
                (url, Some(name.clone()))
            }
            // Checked when planned.
            Target::Url(url) => (url.clone(), None),
        };
        if let Some(key) = &self.streams {
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

/// The origin `option` names, else its variable, else its default, as `scheme://host[:port]`.
fn origin(given: Option<&str>, option: OriginOption) -> Result<String, TonboError> {
    let given = given
        .map(str::to_owned)
        .or_else(|| env(option.env()))
        .unwrap_or_else(|| option.fallback().to_owned());
    let refuse = |why| TonboError::Origin {
        option,
        given: given.clone(),
        why,
    };
    let uri = given
        .parse::<Uri>()
        .map_err(|e| refuse(OriginRefusal::NotAUrl(e)))?;
    let (Some(scheme), Some(authority)) = (uri.scheme_str(), uri.authority()) else {
        return Err(refuse(OriginRefusal::NotAbsolute));
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

/// Refuses the stream URL `url` (of database `name`) before any request: one `ursula_open` would
/// refuse, by the open's own check, and with `streams`, one that would get the key's Streams
/// tokens over plain http to another host.
fn check_url(url: &str, name: Option<&str>, streams: bool) -> Result<(), TonboError> {
    let name = || name.map(str::to_owned);
    canonical_path(url).map_err(|source| TonboError::InvalidUrl {
        name: name(),
        source,
    })?;
    let uri = url
        .parse::<Uri>()
        .map_err(|source| TonboError::UnparsableUrl {
            url: url.to_owned(),
            name: name(),
            source,
        })?;
    if streams && in_clear(&uri) {
        return Err(TonboError::Exposed {
            url: url.to_owned(),
            name: name(),
        });
    }
    Ok(())
}

/// Makes `key` the process's key source for Streams tokens, exchanged at `auth`'s token endpoint,
/// and waits until it holds one, at most until `until` ends: a bad key fails here, before the open.
/// A cancel ends the wait at once, and with it the resolve's claim on the key.
fn streams_token(key: &ApiKey, auth: &str, url: &str, until: &Until) -> Result<(), TonboError> {
    let endpoint = format!("{auth}/api/iam/token");
    let cancelled = || until.cancelled();
    let outcome = crate::auth::use_key(
        &key.secret,
        &endpoint,
        Some(STREAMS_AUDIENCE),
        until.left(),
        &cancelled,
    );
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
        Err(KeyError::TimedOut { last, .. } | KeyError::Cancelled { last }) => {
            match last.map(|e| lookup::exchange_failure(e, &key.from)) {
                Some(lookup::Failure::Final(e)) => Err(e),
                Some(lookup::Failure::Transient(t)) => Err(gave_up(Some(t))),
                None => Err(gave_up(None)),
            }
        }
        Err(e) => Err(TonboError::StreamsToken(e)),
    }
}

/// `wait` after `now`, at most [`FOREVER`]: a deadline the clock can hold.
fn after(now: Instant, wait: Duration) -> Instant {
    now.checked_add(wait.min(FOREVER)).unwrap_or(now)
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A resolve's outcome, shared by the calls that take it and `ursula_cancel`.
type Outcome = Result<Resolved, Arc<TonboError>>;

/// A resolve in progress, shared by its thread, the calls waiting for it and `ursula_cancel`: its
/// cancel, the last condition it met, and how it ended.
#[derive(Debug)]
struct Run {
    subject: Subject,
    cancelled: AtomicBool,
    state: Mutex<RunState>,
    /// Signalled under `state`'s lock on a cancel and at the end.
    changed: Condvar,
}

#[derive(Debug, Default)]
struct RunState {
    last: Option<Transient>,
    ended: Option<Outcome>,
}

impl Run {
    fn new(subject: Subject) -> Arc<Self> {
        Arc::new(Self {
            subject,
            cancelled: AtomicBool::new(false),
            state: Mutex::new(RunState::default()),
            changed: Condvar::new(),
        })
    }

    /// Gives the resolve up: its sleeps and its wait for a Streams token end at once.
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        drop(lock(&self.state));
        self.changed.notify_all();
        crate::auth::wake();
    }

    fn end(&self, outcome: Outcome) {
        lock(&self.state).ended = Some(outcome);
        self.changed.notify_all();
    }

    /// How it ended, waiting at most `wait`.
    fn ended_within(&self, wait: Duration) -> Option<Outcome> {
        let deadline = after(Instant::now(), wait);
        let mut state = lock(&self.state);
        loop {
            if let Some(outcome) = &state.ended {
                return Some(outcome.clone());
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return None;
            }
            state = self
                .changed
                .wait_timeout(state, left)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }

    /// The failure of a call that gave up on it, with the last condition it met.
    fn gave_up(&self, cancelled: bool) -> Arc<TonboError> {
        Arc::new(TonboError::GaveUp {
            subject: self.subject.clone(),
            last: lock(&self.state).last.clone(),
            cancelled,
        })
    }
}

/// When a resolve ends: its deadline, or a cancel.
#[derive(Debug)]
pub(crate) struct Until {
    deadline: Instant,
    run: Arc<Run>,
}

impl Until {
    fn new(timeout: Duration, run: Arc<Run>) -> Self {
        Self {
            deadline: after(Instant::now(), timeout),
            run,
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
        self.run.cancelled.load(Ordering::Acquire)
    }

    /// Whether the resolve ended: its time ran out, or it was cancelled.
    pub(crate) fn over(&self) -> bool {
        self.left().is_zero()
    }

    /// The condition the resolve met last, which a call that gives up on it reports.
    pub(crate) fn note(&self, last: &Transient) {
        lock(&self.run.state).last = Some(last.clone());
    }

    /// Waits `wait`, or until the resolve ends: whether it may go on afterwards.
    pub(crate) fn sleep(&self, wait: Duration) -> bool {
        let end = after(Instant::now(), wait).min(self.deadline);
        let mut state = lock(&self.run.state);
        loop {
            let left = end.saturating_duration_since(Instant::now());
            if self.cancelled() || left.is_zero() {
                break;
            }
            state = self
                .run
                .changed
                .wait_timeout(state, left)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        drop(state);
        !self.over()
    }
}

/// The resolves in progress and the outcomes owed.
fn table() -> MutexGuard<'static, Table<Outcome, Arc<Run>>> {
    static TABLE: OnceLock<Mutex<Table<Outcome, Arc<Run>>>> = OnceLock::new();
    lock(TABLE.get_or_init(Mutex::default))
}

/// Signalled when a resolve's outcome arrives.
static FINISHED: Condvar = Condvar::new();

/// The digest of a call's arguments: the key of its resolve. The arguments may hold a key, which
/// the table keeps no copy of.
fn digest(name_or_url: &str, options: &str) -> Id {
    let mut hasher = blake3::Hasher::new();
    hasher.update(name_or_url.as_bytes());
    hasher.update(&[0]);
    hasher.update(options.as_bytes());
    *hasher.finalize().as_bytes()
}

/// `tonbo_resolve(name_or_url, options_json, timeout_ms)`: the resolve of those arguments joined,
/// its outcome taken or waited for, or a new one. A resolve that sends nothing answers at once.
pub(crate) fn resolve(
    name_or_url: &str,
    options: &str,
    timeout_ms: u64,
) -> Result<Resolution, Arc<TonboError>> {
    let id = digest(name_or_url, options);
    let blocking = timeout_ms > 0;
    let mut planned: Option<(Plan, u64)> = None;
    loop {
        let request = {
            let mut table = table();
            table.sweep(Instant::now());
            match table.arrive(id, blocking) {
                Arrival::Pending(request) => return Ok(Resolution::Pending(request)),
                Arrival::Take(outcome) => return outcome.map(Resolution::Done),
                Arrival::Wait(request) => Some(request),
                Arrival::Start => match planned.take() {
                    Some((plan, request)) => {
                        let ms = if blocking {
                            timeout_ms
                        } else {
                            plan.timeout_ms.unwrap_or(DEFAULT_RESOLVE_MS)
                        };
                        let run = Run::new(subject(name_or_url));
                        start(id, request, plan, Duration::from_millis(ms), &run)?;
                        table.start(id, request, run, blocking);
                        if !blocking {
                            return Ok(Resolution::Pending(request));
                        }
                        Some(request)
                    }
                    None => None,
                },
            }
        };
        let Some(request) = request else {
            // Planned outside the table's lock: it reads the environment and the login file.
            let plan = Plan::new(name_or_url, options).map_err(Arc::new)?;
            if !plan.sends() {
                let until = Until::new(Duration::ZERO, Run::new(subject(name_or_url)));
                return plan.run(&until).map(Resolution::Done).map_err(Arc::new);
            }
            planned = Some((plan, driver().request_id()));
            continue;
        };
        if let Some(outcome) = wait(id, request, timeout_ms) {
            return outcome.map(Resolution::Done);
        }
        // Its resolve left the table: the call starts again.
    }
}

/// What a resolve of `name_or_url` is about.
fn subject(name_or_url: &str) -> Subject {
    if has_scheme(name_or_url) {
        Subject::Token(name_or_url.to_owned())
    } else {
        Subject::Database(name_or_url.to_owned())
    }
}

/// Starts resolve `request` of `plan` under `id`, for at most `timeout`, on a thread of its own.
/// The caller holds the table's lock, so the thread's end finds its entry.
fn start(
    id: Id,
    request: u64,
    plan: Plan,
    timeout: Duration,
    run: &Arc<Run>,
) -> Result<(), Arc<TonboError>> {
    let until = Until::new(timeout, Arc::clone(run));
    let ended = Arc::clone(run);
    // Detached: it ends once the resolve ended, cancelled or not.
    std::thread::Builder::new()
        .name("ursula-vfs-tonbo".to_owned())
        .spawn(move || {
            let outcome = crate::guard::contain(
                "tonbo_resolve",
                || Err(Arc::new(TonboError::Panicked)),
                || plan.run(&until).map_err(Arc::new),
            );
            finish(id, request, &ended, outcome);
        })
        .map(drop)
        .map_err(|e| Arc::new(TonboError::NoThread(e)))
}

/// Resolve `request` under `id` ended with `outcome`: the calls owed it take it, and a `resolved`
/// event says so, for a resolve given up too.
fn finish(id: Id, request: u64, run: &Run, outcome: Outcome) {
    let ok = outcome.is_ok();
    run.end(outcome.clone());
    table().finish(id, request, outcome, Instant::now());
    FINISHED.notify_all();
    driver().resolved(request, ok);
}

/// A blocking call's wait for resolve `request` under `id`, at most `timeout_ms` and the grace:
/// its outcome, or `None` when it left the table.
fn wait(id: Id, request: u64, timeout_ms: u64) -> Option<Outcome> {
    let deadline = after(
        Instant::now(),
        Duration::from_millis(timeout_ms.saturating_add(GRACE_MS)),
    );
    let mut table = table();
    loop {
        match table.poll(id, request) {
            Poll::Take(outcome) => return Some(outcome),
            Poll::Gone => return None,
            Poll::Wait => {}
        }
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            let given_up = table.give_up(id, request);
            drop(table);
            return given_up.map(|step| match step {
                GiveUp::Take(outcome) => outcome,
                GiveUp::Leave(run) => Err(run.gave_up(false)),
                GiveUp::Abandon(run) => {
                    run.cancel();
                    Err(run.gave_up(false))
                }
            });
        }
        table = FINISHED
            .wait_timeout(table, left)
            .unwrap_or_else(PoisonError::into_inner)
            .0;
    }
}

/// `ursula_cancel(request)` of a resolve: `None` when `request` names none. Otherwise what it
/// came to for the host that cancels: `Some(None)` when it resolved before the cancel, the failure
/// it ended with, or `cancelled` with the last condition it met. A resolve a blocking call waits
/// for goes on for that call. One given up is waited for at most [`CANCEL_REPORT_MS`].
pub(crate) fn cancel(request: u64) -> Option<Option<Arc<TonboError>>> {
    let step = table().cancel(request)?;
    Some(match step {
        Cancelled::Ended(outcome) => outcome.err(),
        Cancelled::Left(run) => Some(run.gave_up(true)),
        Cancelled::Abandoned(run) => {
            run.cancel();
            match run.ended_within(Duration::from_millis(CANCEL_REPORT_MS)) {
                Some(outcome) => outcome.err(),
                None => Some(run.gave_up(true)),
            }
        }
    })
}
