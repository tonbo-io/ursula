//! What callers outside the extension see of a failure (design: Resets, Errors): a stable kind
//! token ([`ErrorKind`]) with what to do about it ([`Action`]), in every SQL function's error
//! (`<function>: <kind>: <message>`) and in `ursula_status`.
//!
//! A refused write ([`WriteRefusal`]) fails its transaction alone with `SQLITE_IOERR_AUTH`, nothing
//! written. A reset ([`ResetKind`]) fails every statement of the binding: a read with
//! `SQLITE_IOERR_READ`, a write with `SQLITE_IOERR_WRITE`. [`Error`] is a SQL function's failure.
//!
//! Every kind also names its [`ErrorClass`], the error a host throws for it, and [`classify`] is the
//! one mapping from a failure a host met (a SQL function's error, or a statement's I/O error) to
//! its kind, action and class, which `ursula_classify` returns (design: Errors).

use std::sync::Arc;

use libsqlite3_sys as ffi;

use crate::driver::Kept;
use crate::driver::SyncError;
use crate::driver::runtime::DriverGone;
use crate::host::HostError;
use crate::import::ImportError;
use crate::open::OpenError;
use crate::options::OptionsError;
use crate::replicator::ResetKind;
use crate::salvage::SalvageError;
use crate::snapshotter::SnapshotFailure;

/// What a caller does about an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    /// Try again later: after `synced()` for `backlog_full`, once the stream answers for an open.
    Retry,
    /// Close every connection of the database and open it again.
    Reopen,
    /// Another owner holds the stream: stop writing.
    Stop,
    /// It cannot succeed as asked: fix the URL, the setup, the stream or the transaction.
    Fix,
}

impl Action {
    /// The stable token.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Action::Retry => "retry",
            Action::Reopen => "reopen",
            Action::Stop => "stop",
            Action::Fix => "fix",
        }
    }
}

/// The stable kind of a failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ErrorKind {
    /// A write refused at admission (design: Backpressure).
    BacklogFull,
    /// A write the extension does not capture: EXCLUSIVE locking mode, a commit leaving WAL mode
    /// or 4 KiB pages.
    Unsupported,
    /// A commit of more bytes of pages than the commit limit (design: Capture).
    TooLarge,
    Reset(ResetKind),
    InvalidArgument,
    InvalidUrl,
    NotFound,
    InUse,
    /// The stream is open in this process under another cache root, which the host holds.
    BoundElsewhere,
    LocalSetup,
    StreamFormat,
    /// A HEAD answer without the server's identity (`Ursula-Protocol`, `Ursula-Cluster-Id`).
    NoServerIdentity,
    /// A server of another release line.
    ProtocolMismatch,
    WrongCluster,
    StreamState,
    ClaimRaces,
    Unreachable,
    Unavailable,
    RateLimited,
    Unauthorized,
    PaymentRequired,
    Forbidden,
    Cancelled,
    /// The path names no database bound in this process.
    NotBound,
    Internal,
}

/// [`ErrorKind::as_str`] and its inverse [`ErrorKind::parse`] from one list of the kinds that are no
/// reset (a reset's token is its [`ResetKind::kind`]): a kind added without its token fails to build
/// `as_str`, and a token listed twice fails the lint of an unreachable pattern in `parse`.
macro_rules! kind_tokens {
    ($($kind:ident => $token:literal,)*) => {
        impl ErrorKind {
            /// The stable token.
            pub(crate) fn as_str(self) -> &'static str {
                match self {
                    ErrorKind::Reset(kind) => kind.kind(),
                    $(ErrorKind::$kind => $token,)*
                }
            }

            /// The kind of `token`. `stream_state` and `internal` read as the kinds that are no
            /// reset, whose class and action are the same as their resets' at every [`Surface`].
            fn parse(token: &str) -> Option<ErrorKind> {
                match token {
                    $($token => Some(ErrorKind::$kind),)*
                    token => ResetKind::from_kind(token).map(ErrorKind::Reset),
                }
            }
        }
    };
}

kind_tokens! {
    BacklogFull => "backlog_full",
    Unsupported => "unsupported",
    TooLarge => "too_large",
    InvalidArgument => "invalid_argument",
    InvalidUrl => "invalid_url",
    NotFound => "not_found",
    InUse => "in_use",
    BoundElsewhere => "bound_elsewhere",
    LocalSetup => "local_setup",
    StreamFormat => "stream_format",
    NoServerIdentity => "no_server_identity",
    ProtocolMismatch => "protocol_mismatch",
    WrongCluster => "wrong_cluster",
    StreamState => "stream_state",
    ClaimRaces => "claim_races",
    Unreachable => "unreachable",
    Unavailable => "unavailable",
    RateLimited => "rate_limited",
    Unauthorized => "unauthorized",
    PaymentRequired => "payment_required",
    Forbidden => "forbidden",
    Cancelled => "cancelled",
    NotBound => "not_bound",
    Internal => "internal",
}

impl ErrorKind {
    /// What a caller does about an error of this kind.
    pub(crate) fn action(self) -> Action {
        match self {
            ErrorKind::BacklogFull
            | ErrorKind::ClaimRaces
            | ErrorKind::InUse
            | ErrorKind::Unreachable
            | ErrorKind::Unavailable
            | ErrorKind::RateLimited
            | ErrorKind::Cancelled => Action::Retry,
            ErrorKind::Reset(kind) => reset_action(kind),
            ErrorKind::Internal => Action::Reopen,
            ErrorKind::Unsupported
            | ErrorKind::TooLarge
            | ErrorKind::InvalidArgument
            | ErrorKind::InvalidUrl
            | ErrorKind::NotFound
            | ErrorKind::LocalSetup
            | ErrorKind::StreamFormat
            | ErrorKind::NoServerIdentity
            | ErrorKind::ProtocolMismatch
            | ErrorKind::WrongCluster
            | ErrorKind::StreamState
            | ErrorKind::Unauthorized
            | ErrorKind::PaymentRequired
            | ErrorKind::Forbidden
            | ErrorKind::NotBound
            | ErrorKind::BoundElsewhere => Action::Fix,
        }
    }
}

impl Action {
    /// What to do, in words: the end of each message the extension writes for a host about a
    /// reset or a failed `synced()`, which every host shows as its error's message.
    pub(crate) fn advice(self) -> &'static str {
        match self {
            Action::Retry => "try again later.",
            Action::Reopen => "close every connection of the database and open() it again.",
            Action::Stop => {
                "another owner took the stream over: stop using this database, and never reopen it \
                 in a loop."
            }
            Action::Fix => "fix the cause, then open() the database again.",
        }
    }
}

/// The error a host throws for a failure (design: Errors).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ErrorClass {
    /// A write or COMMIT refused with nothing committed: the database stays usable.
    WriteRefused,
    /// The database was reset: every statement of it fails from now on.
    Reset,
    /// An open or an import met a stream that does not answer as it must.
    Unavailable,
    /// It cannot succeed as asked.
    Setup,
}

impl ErrorClass {
    /// The stable token.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            ErrorClass::WriteRefused => "write_refused",
            ErrorClass::Reset => "reset",
            ErrorClass::Unavailable => "unavailable",
            ErrorClass::Setup => "setup",
        }
    }
}

/// Where a failure surfaced, which decides the class of `stream_state` and `internal`: an open's
/// setup errors, a bound database's resets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Surface {
    /// `ursula_open`, `ursula_cancel`, `ursula_import`, an `opened` event.
    Open,
    /// A bound database: its statements, `ursula_synced`, `ursula_status`, its events.
    Bound,
}

impl ErrorKind {
    /// The error a host throws for a failure of this kind that surfaced at `surface`.
    pub(crate) fn class(self, surface: Surface) -> ErrorClass {
        match self {
            ErrorKind::BacklogFull | ErrorKind::Unsupported | ErrorKind::TooLarge => {
                ErrorClass::WriteRefused
            }
            ErrorKind::StreamState
            | ErrorKind::Internal
            | ErrorKind::Reset(ResetKind::StreamState | ResetKind::Internal) => match surface {
                Surface::Open => ErrorClass::Setup,
                Surface::Bound => ErrorClass::Reset,
            },
            ErrorKind::Reset(_) => ErrorClass::Reset,
            ErrorKind::Unreachable
            | ErrorKind::Unavailable
            | ErrorKind::RateLimited
            | ErrorKind::Unauthorized
            | ErrorKind::PaymentRequired
            | ErrorKind::Forbidden
            | ErrorKind::Cancelled => ErrorClass::Unavailable,
            ErrorKind::InvalidArgument
            | ErrorKind::InvalidUrl
            | ErrorKind::NotFound
            | ErrorKind::InUse
            | ErrorKind::BoundElsewhere
            | ErrorKind::LocalSetup
            | ErrorKind::StreamFormat
            | ErrorKind::NoServerIdentity
            | ErrorKind::ProtocolMismatch
            | ErrorKind::WrongCluster
            | ErrorKind::ClaimRaces
            | ErrorKind::NotBound => ErrorClass::Setup,
        }
    }
}

/// What [`classify`] made of a failure: the kind, what to do, the error to throw and its message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Classified {
    pub(crate) kind: ErrorKind,
    pub(crate) action: Action,
    pub(crate) class: ErrorClass,
    /// The message of the error to throw: a SQL function's own, or for a statement's I/O error,
    /// whose message says nothing, what happened and what to do.
    pub(crate) message: String,
}

/// What a bound database says about a statement's I/O error: its reset, else the last write it
/// refused, which only the next refusal replaces (`Commit::refusal`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FailureState {
    pub(crate) reset: Option<ResetKind>,
    pub(crate) refused: Option<WriteRefusal>,
}

/// The failure a host met, with SQLite's `message` and result code `errcode`, as the extension's
/// (design: Errors), or `None` when it is not the extension's (a constraint, a syntax error).
///
/// - A SQL function's error (`SQLITE_ERROR`, `<function>: <kind>: <message>`) names its kind. Its
///   function names the [`Surface`]: `ursula_open`, `ursula_cancel` and `ursula_import` fail an
///   open, every other function a bound database. An import that met a stream not answering as it
///   must says `fix`, not `retry`: it may have claimed the stream, which then refuses opens and
///   imports, so a retry into it fails.
/// - A statement's I/O error says nothing, so `state` reads the state of the database the
///   statement ran on, when there is one: its reset fails every statement (`SQLITE_IOERR_READ`
///   for a read, `SQLITE_IOERR_WRITE` for a write), and its last refused write is the
///   `SQLITE_IOERR_AUTH` of a write or COMMIT. A database that names neither failed another
///   database's statement, one used through SQL `ATTACH DATABASE`.
pub(crate) fn classify(
    message: &str,
    errcode: i64,
    state: impl FnOnce() -> Option<FailureState>,
) -> Option<Classified> {
    if errcode == i64::from(ffi::SQLITE_ERROR) {
        return function_failure(message);
    }
    if errcode & 0xff != i64::from(ffi::SQLITE_IOERR) {
        return None;
    }
    let state = state()?;
    if let Some(reset) = state.reset {
        let kind = ErrorKind::Reset(reset);
        let what = match errcode {
            code if code == i64::from(ffi::SQLITE_IOERR_WRITE) => "write",
            code if code == i64::from(ffi::SQLITE_IOERR_READ) => "read",
            _ => "statement",
        };
        return Some(Classified {
            kind,
            action: kind.action(),
            class: kind.class(Surface::Bound),
            message: format!("{what} refused, {}", reset_message(reset)),
        });
    }
    let refused = state
        .refused
        .filter(|_| errcode == i64::from(ffi::SQLITE_IOERR_AUTH))?;
    let kind = refused.kind();
    Some(Classified {
        kind,
        action: kind.action(),
        class: kind.class(Surface::Bound),
        message: format!("{refused}. {}", refused.advice()),
    })
}

/// A SQL function's error, `<function>: <kind>: <message>` as [`Error`] reaches a host, classified.
fn function_failure(message: &str) -> Option<Classified> {
    let rest = message.strip_prefix("ursula_")?;
    let (function, rest) = rest.split_once(": ")?;
    let (token, _) = rest.split_once(": ")?;
    let word = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_lowercase() || b == b'_');
    if !word(function) || !word(token) {
        return None;
    }
    let kind = ErrorKind::parse(token)?;
    let surface = match function {
        "open" | "cancel" | "import" => Surface::Open,
        _ => Surface::Bound,
    };
    let class = kind.class(surface);
    let action = match (function, class) {
        ("import", ErrorClass::Unavailable) => Action::Fix,
        _ => kind.action(),
    };
    Some(Classified {
        kind,
        action,
        class,
        message: message.to_owned(),
    })
}

/// What a host says of a reset of `kind`, with what to do: the `message` of a `reset` event and of
/// `status().reset`, the error `synced()` and the `reset` listeners get.
pub(crate) fn reset_message(kind: ResetKind) -> String {
    format!(
        "the database was reset ({}): {}",
        kind.kind(),
        reset_action(kind).advice()
    )
}

/// What a host says of a `synced()` that failed with `kind`, with what to do: the `message` of a
/// `sync_failed` event.
pub(crate) fn sync_failed_message(kind: ErrorKind) -> String {
    format!(
        "synced() failed: {}: {}",
        kind.as_str(),
        kind.action().advice()
    )
}

/// The action of a reset (design: Resets).
pub(crate) fn reset_action(kind: ResetKind) -> Action {
    match kind {
        ResetKind::Fenced => Action::Stop,
        ResetKind::Stale | ResetKind::Rewound | ResetKind::LocalDamage | ResetKind::Internal => {
            Action::Reopen
        }
        ResetKind::Deleted | ResetKind::StreamState => Action::Fix,
    }
}

impl OpenError {
    /// The kind callers see (design: Errors).
    pub(crate) fn error_kind(&self) -> ErrorKind {
        use crate::open::Unavailable;
        match self {
            OpenError::InvalidUrl(_) => ErrorKind::InvalidUrl,
            OpenError::InUse { .. } => ErrorKind::InUse,
            OpenError::BoundElsewhere { kept, .. } => kept.error_kind(),
            OpenError::LocalSetup(_) => ErrorKind::LocalSetup,
            OpenError::Internal(_) | OpenError::Panicked => ErrorKind::Internal,
            OpenError::StreamFormat(_) => ErrorKind::StreamFormat,
            OpenError::NoServerIdentity => ErrorKind::NoServerIdentity,
            OpenError::ProtocolMismatch { .. } => ErrorKind::ProtocolMismatch,
            OpenError::WrongCluster { .. } => ErrorKind::WrongCluster,
            OpenError::StreamState(_)
            | OpenError::Snapshot(_)
            | OpenError::ImportIncomplete { .. } => ErrorKind::StreamState,
            OpenError::NotFound => ErrorKind::NotFound,
            OpenError::ClaimRaces => ErrorKind::ClaimRaces,
            OpenError::Unavailable(condition) => match condition {
                Unavailable::Unreachable => ErrorKind::Unreachable,
                Unavailable::RateLimited => ErrorKind::RateLimited,
                Unavailable::Unauthorized => ErrorKind::Unauthorized,
                Unavailable::PaymentRequired => ErrorKind::PaymentRequired,
                Unavailable::Forbidden => ErrorKind::Forbidden,
                Unavailable::WrongCluster => ErrorKind::WrongCluster,
                Unavailable::NoServerIdentity => ErrorKind::NoServerIdentity,
                Unavailable::ProtocolMismatch { .. } => ErrorKind::ProtocolMismatch,
                Unavailable::Retrying
                | Unavailable::Redirected { .. }
                | Unavailable::Rejected { .. }
                | Unavailable::RequestTooLarge => ErrorKind::Unavailable,
            },
            OpenError::GaveUp(_) => ErrorKind::Unavailable,
            OpenError::Reset(kind) => ErrorKind::Reset(*kind),
            OpenError::Cancelled => ErrorKind::Cancelled,
        }
    }
}

impl Kept {
    /// The kind of an open refused for it: `bound_elsewhere` (`fix`: use the cache root the stream
    /// is open under, or let go of it there first) while the host holds the binding, `in_use`
    /// (`retry`) while it only drains.
    pub(crate) fn error_kind(self) -> ErrorKind {
        match self {
            Kept::Connections(_) | Kept::HandedOut | Kept::Opening => ErrorKind::BoundElsewhere,
            Kept::Pending { .. } | Kept::Draining => ErrorKind::InUse,
        }
    }
}

impl ImportError {
    /// The kind callers see.
    pub(crate) fn kind(&self) -> ErrorKind {
        match self {
            ImportError::InvalidArgument { .. } | ImportError::Options(_) => {
                ErrorKind::InvalidArgument
            }
            ImportError::InvalidUrl(_) => ErrorKind::InvalidUrl,
            ImportError::Source(_) => ErrorKind::InvalidArgument,
            ImportError::Io { .. } => ErrorKind::LocalSetup,
            ImportError::NoServerIdentity => ErrorKind::NoServerIdentity,
            ImportError::ProtocolMismatch { .. } => ErrorKind::ProtocolMismatch,
            ImportError::ContentType { .. } | ImportError::Configuration => ErrorKind::StreamFormat,
            ImportError::NotNew(_) => ErrorKind::StreamState,
            ImportError::WrongCluster { .. } => ErrorKind::WrongCluster,
            ImportError::Rejected { status, .. } => status_kind(*status),
            ImportError::Unavailable { .. } | ImportError::TimedOut { .. } => {
                ErrorKind::Unavailable
            }
            ImportError::Publish(e) => match e {
                // The file is too large for the stream as it is set up: no write was refused.
                SnapshotFailure::TooLarge { .. } => ErrorKind::InvalidArgument,
                SnapshotFailure::Conflict { .. } | SnapshotFailure::TokenChanged => {
                    ErrorKind::StreamState
                }
                SnapshotFailure::WrongCluster { .. } => ErrorKind::WrongCluster,
                SnapshotFailure::NoServerIdentity => ErrorKind::NoServerIdentity,
                SnapshotFailure::ProtocolMismatch { .. } => ErrorKind::ProtocolMismatch,
                SnapshotFailure::Status { status, .. } => status_kind(*status),
                SnapshotFailure::Io { .. } | SnapshotFailure::Body(_) => ErrorKind::LocalSetup,
                SnapshotFailure::Host(_) | SnapshotFailure::Driver(_) => ErrorKind::Internal,
                SnapshotFailure::Malformed { .. }
                | SnapshotFailure::Outage { .. }
                | SnapshotFailure::StagedPartsFull { .. }
                | SnapshotFailure::Reset => ErrorKind::Unavailable,
            },
        }
    }
}

/// The kind of a definite answer an import does not take.
fn status_kind(status: u16) -> ErrorKind {
    match status {
        401 => ErrorKind::Unauthorized,
        402 => ErrorKind::PaymentRequired,
        403 => ErrorKind::Forbidden,
        404 | 409 | 410 | 412 => ErrorKind::StreamState,
        _ => ErrorKind::Unavailable,
    }
}

/// Why a write transaction failed alone, nothing of it written or logged.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum WriteRefusal {
    #[error("backlog_full: {pending} bytes logged and not acknowledged, at most {max}")]
    BacklogFull { pending: u64, max: u64 },
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("too_large: a commit of {bytes} bytes of pages, at most {max}")]
    TooLarge { bytes: u64, max: u64 },
}

impl WriteRefusal {
    pub(crate) fn kind(&self) -> ErrorKind {
        match self {
            WriteRefusal::BacklogFull { .. } => ErrorKind::BacklogFull,
            WriteRefusal::Unsupported(_) => ErrorKind::Unsupported,
            WriteRefusal::TooLarge { .. } => ErrorKind::TooLarge,
        }
    }

    /// What to do, in words.
    fn advice(&self) -> &'static str {
        match self {
            WriteRefusal::BacklogFull { .. } => {
                "Nothing was committed: wait for synced(), then run the transaction again."
            }
            WriteRefusal::TooLarge { .. } => {
                "Nothing was committed: split the transaction, or raise \
                 URSULA_VFS_MAX_COMMIT_BYTES."
            }
            WriteRefusal::Unsupported(_) => "Nothing was committed.",
        }
    }
}

/// A SQL function's failure.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("{0}")]
    Open(#[source] Arc<OpenError>),
    #[error("the database was reset ({})", .0.kind())]
    Reset(ResetKind),
    #[error("{path} is not a database ursula_open bound in this process")]
    NotBound { path: String },
    #[error("the request {request} is not an open in progress")]
    NoRequest { request: u64 },
    #[error("{name}: {why}")]
    InvalidArgument { name: &'static str, why: String },
    #[error(transparent)]
    Options(#[from] OptionsError),
    #[error(transparent)]
    Host(#[from] HostError),
    #[error(transparent)]
    DriverGone(#[from] DriverGone),
    #[error("{entry} panicked")]
    Panicked { entry: &'static str },
    /// The root's `salvage/` could not be listed.
    #[error(transparent)]
    Salvage(#[from] SalvageError),
    #[error(transparent)]
    Import(#[from] ImportError),
}

impl Error {
    pub(crate) fn kind(&self) -> ErrorKind {
        match self {
            Error::Open(e) => e.error_kind(),
            Error::Reset(kind) => ErrorKind::Reset(*kind),
            Error::NotBound { .. } | Error::NoRequest { .. } => ErrorKind::NotBound,
            Error::InvalidArgument { .. } | Error::Options(_) => ErrorKind::InvalidArgument,
            Error::Host(e) => e.kind(),
            Error::DriverGone(_) | Error::Panicked { .. } => ErrorKind::Internal,
            Error::Salvage(_) => ErrorKind::LocalSetup,
            Error::Import(e) => e.kind(),
        }
    }
}

impl From<SyncError> for Error {
    fn from(e: SyncError) -> Self {
        match e {
            SyncError::Reset(kind) => Error::Reset(kind),
            SyncError::NotBound => Error::Reset(ResetKind::Internal),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Action;
    use super::Classified;
    use super::ErrorClass;
    use super::ErrorKind;
    use super::FailureState;
    use super::Surface;
    use super::WriteRefusal;
    use super::classify;
    use super::reset_action;
    use crate::replicator::ResetKind;

    const IOERR_READ: i64 = 266;
    const IOERR_WRITE: i64 = 778;
    const IOERR_AUTH: i64 = 7178;

    /// [`classify`] of a SQL function's error, which reads no database.
    fn function(message: &str) -> Option<(ErrorKind, Action, ErrorClass)> {
        classify(message, 1, || {
            panic!("a SQL function's error reads no database")
        })
        .map(|c| (c.kind, c.action, c.class))
    }

    // Every kind's token reads back as a kind with the same token, action and class, so a host
    // learns from a SQL function's error what the extension meant by it. The kinds that are no
    // reset read back by construction (`kind_tokens!`), and so does every reset's token
    // (`reset_tokens!`): this checks that `stream_state` and `internal`, which read as the kinds
    // that are no reset, mean the same as their resets, and that no other reset's token is taken.
    #[test]
    fn every_kind_reads_back_from_its_token() {
        let resets = [
            ResetKind::Fenced,
            ResetKind::Stale,
            ResetKind::Rewound,
            ResetKind::Deleted,
            ResetKind::LocalDamage,
            ResetKind::StreamState,
            ResetKind::Internal,
        ];
        let others = [
            ErrorKind::BacklogFull,
            ErrorKind::StreamState,
            ErrorKind::Internal,
            ErrorKind::NotBound,
        ];
        for kind in resets.into_iter().map(ErrorKind::Reset).chain(others) {
            let read = ErrorKind::parse(kind.as_str()).unwrap();
            assert_eq!(read.as_str(), kind.as_str());
            assert_eq!(read.action(), kind.action(), "{}", kind.as_str());
            for surface in [Surface::Open, Surface::Bound] {
                assert_eq!(
                    read.class(surface),
                    kind.class(surface),
                    "{}",
                    kind.as_str()
                );
            }
        }
        for reset in resets {
            let read = ErrorKind::parse(reset.kind());
            let expected = match reset {
                ResetKind::StreamState => ErrorKind::StreamState,
                ResetKind::Internal => ErrorKind::Internal,
                other => ErrorKind::Reset(other),
            };
            assert_eq!(read, Some(expected), "{}", reset.kind());
        }
        assert_eq!(ErrorKind::parse("no_such_kind"), None);
    }

    // The function names where the failure surfaced: `stream_state` and `internal` fail an open
    // as setup errors and a bound database as resets. Every other reset is one wherever it
    // surfaced.
    #[test]
    fn a_function_error_is_classified_where_it_surfaced() {
        assert_eq!(
            function("ursula_open: stream_state: the stream holds foreign bytes"),
            Some((ErrorKind::StreamState, Action::Fix, ErrorClass::Setup))
        );
        assert_eq!(
            function("ursula_synced: stream_state: the database was reset (stream_state)"),
            Some((ErrorKind::StreamState, Action::Fix, ErrorClass::Reset))
        );
        assert_eq!(
            function("ursula_cancel: internal: ursula_cancel panicked"),
            Some((ErrorKind::Internal, Action::Reopen, ErrorClass::Setup))
        );
        assert_eq!(
            function("ursula_status: internal: the driver is gone"),
            Some((ErrorKind::Internal, Action::Reopen, ErrorClass::Reset))
        );
        assert_eq!(
            function("ursula_open: fenced: the database was reset (fenced)"),
            Some((
                ErrorKind::Reset(ResetKind::Fenced),
                Action::Stop,
                ErrorClass::Reset
            ))
        );
        assert_eq!(
            function("ursula_status: not_bound: /x/db is not a database ursula_open bound"),
            Some((ErrorKind::NotBound, Action::Fix, ErrorClass::Setup))
        );
        assert_eq!(
            function("ursula_open: in_use: another process holds the root"),
            Some((ErrorKind::InUse, Action::Retry, ErrorClass::Setup))
        );
        assert_eq!(
            function("ursula_open: unreachable: no answer"),
            Some((
                ErrorKind::Unreachable,
                Action::Retry,
                ErrorClass::Unavailable
            ))
        );
        assert_eq!(
            function("ursula_open: unauthorized: 401"),
            Some((
                ErrorKind::Unauthorized,
                Action::Fix,
                ErrorClass::Unavailable
            ))
        );
    }

    // An import that got no definite answer may have claimed the stream: a retry into it fails.
    #[test]
    fn an_unavailable_import_says_fix() {
        assert_eq!(
            function("ursula_import: unavailable: the import ran out of its 5 ms"),
            Some((ErrorKind::Unavailable, Action::Fix, ErrorClass::Unavailable))
        );
        assert_eq!(
            function("ursula_import: invalid_argument: timeout_ms: give it a positive timeout"),
            Some((ErrorKind::InvalidArgument, Action::Fix, ErrorClass::Setup))
        );
    }

    // Only a SQL function's error in its format is the extension's, and its message is kept.
    #[test]
    fn only_a_function_error_in_its_format_is_the_extensions() {
        let message = "ursula_open: invalid_url: no host";
        assert_eq!(
            classify(message, 1, || None).map(|c| c.message),
            Some(message.to_owned())
        );
        for other in [
            "near \"SELEC\": syntax error",
            "UNIQUE constraint failed: t.x",
            "ursula_open: invalid_url",
            "ursula_: invalid_url: no host",
            "ursula_open: Invalid_url: no host",
            "ursula_open: no_such_kind: no host",
            "wrong number of arguments to function ursula_open()",
        ] {
            assert_eq!(function(other), None, "{other}");
        }
        // A function's message under another result code: not one of its errors.
        assert_eq!(classify(message, 19, || panic!("not an I/O error")), None);
    }

    fn reset(kind: ResetKind) -> Option<FailureState> {
        Some(FailureState {
            reset: Some(kind),
            refused: None,
        })
    }

    // A statement's I/O error is the reset of its database, the read or write it refused named
    // by its code.
    #[test]
    fn a_statement_of_a_reset_database_fails_with_its_reset() {
        let fenced = classify("disk I/O error", IOERR_WRITE, || reset(ResetKind::Fenced));
        assert_eq!(
            fenced,
            Some(Classified {
                kind: ErrorKind::Reset(ResetKind::Fenced),
                action: Action::Stop,
                class: ErrorClass::Reset,
                message: "write refused, the database was reset (fenced): another owner took the \
                          stream over: stop using this database, and never reopen it in a loop."
                    .to_owned(),
            })
        );
        let read = classify("disk I/O error", IOERR_READ, || {
            reset(ResetKind::StreamState)
        })
        .unwrap();
        assert_eq!(
            (read.kind, read.action, read.class),
            (
                ErrorKind::Reset(ResetKind::StreamState),
                Action::Fix,
                ErrorClass::Reset
            )
        );
        assert_eq!(
            read.message,
            "read refused, the database was reset (stream_state): fix the cause, then open() the \
             database again."
        );
        // Any other I/O error of a reset database, a refused write's included: the reset.
        let other = classify("disk I/O error", IOERR_AUTH, || {
            Some(FailureState {
                reset: Some(ResetKind::LocalDamage),
                refused: Some(WriteRefusal::Unsupported(
                    "EXCLUSIVE locking mode".to_owned(),
                )),
            })
        })
        .unwrap();
        assert_eq!(other.kind, ErrorKind::Reset(ResetKind::LocalDamage));
        assert!(
            other
                .message
                .starts_with("statement refused, the database was reset")
        );
    }

    // SQLITE_IOERR_AUTH is the database's last refused write, with what to do about it.
    #[test]
    fn a_refused_write_is_the_last_one_its_database_refused() {
        let refused = |refusal: WriteRefusal| {
            classify("authorization denied", IOERR_AUTH, || {
                Some(FailureState {
                    reset: None,
                    refused: Some(refusal),
                })
            })
        };
        assert_eq!(
            refused(WriteRefusal::BacklogFull {
                pending: 10,
                max: 5
            }),
            Some(Classified {
                kind: ErrorKind::BacklogFull,
                action: Action::Retry,
                class: ErrorClass::WriteRefused,
                message: "backlog_full: 10 bytes logged and not acknowledged, at most 5. Nothing \
                          was committed: wait for synced(), then run the transaction again."
                    .to_owned(),
            })
        );
        let large = refused(WriteRefusal::TooLarge {
            bytes: 12_288,
            max: 8192,
        })
        .unwrap();
        assert_eq!(
            (large.kind, large.action, large.class),
            (ErrorKind::TooLarge, Action::Fix, ErrorClass::WriteRefused)
        );
        assert!(
            large
                .message
                .ends_with("raise URSULA_VFS_MAX_COMMIT_BYTES.")
        );
        assert_eq!(
            refused(WriteRefusal::Unsupported(
                "EXCLUSIVE locking mode".to_owned()
            ))
            .map(|c| c.message),
            Some("unsupported: EXCLUSIVE locking mode. Nothing was committed.".to_owned())
        );
    }

    // An I/O error the database does not name, or of no database the extension knows, is
    // SQLite's own: another database's, one used through ATTACH DATABASE.
    #[test]
    fn an_io_error_the_database_does_not_name_stays_sqlites() {
        let clean = || {
            Some(FailureState {
                reset: None,
                refused: None,
            })
        };
        assert_eq!(classify("disk I/O error", IOERR_READ, clean), None);
        assert_eq!(classify("disk I/O error", IOERR_AUTH, clean), None);
        assert_eq!(classify("disk I/O error", IOERR_WRITE, || None), None);
        // A refused write explains only SQLITE_IOERR_AUTH.
        let refused = || {
            Some(FailureState {
                reset: None,
                refused: Some(WriteRefusal::BacklogFull { pending: 1, max: 0 }),
            })
        };
        assert_eq!(classify("disk I/O error", IOERR_WRITE, refused), None);
        // Not an I/O error: no database is read.
        assert_eq!(
            classify("UNIQUE constraint failed: t.x", 2067, || panic!("read")),
            None
        );
    }

    // Every reset kind has the action of its reset (design: Resets), and its token is the reset's.
    #[test]
    fn resets_say_what_to_do() {
        let cases = [
            (ResetKind::Fenced, "fenced", Action::Stop),
            (ResetKind::Stale, "stale", Action::Reopen),
            (ResetKind::Rewound, "rewound", Action::Reopen),
            (ResetKind::Deleted, "deleted", Action::Fix),
            (ResetKind::LocalDamage, "local_damage", Action::Reopen),
            (ResetKind::StreamState, "stream_state", Action::Fix),
            (ResetKind::Internal, "internal", Action::Reopen),
        ];
        for (kind, token, action) in cases {
            assert_eq!(ErrorKind::Reset(kind).as_str(), token);
            assert_eq!(reset_action(kind), action);
            assert_eq!(ErrorKind::Reset(kind).action(), action);
        }
    }

    // The write refusals (design: Errors): `backlog_full` is retried after `synced()`, an
    // unsupported write and a commit above the limit are fixed.
    #[test]
    fn write_refusals_say_what_to_do() {
        assert_eq!(ErrorKind::BacklogFull.as_str(), "backlog_full");
        assert_eq!(ErrorKind::BacklogFull.action(), Action::Retry);
        assert_eq!(ErrorKind::Unsupported.as_str(), "unsupported");
        assert_eq!(ErrorKind::Unsupported.action(), Action::Fix);
        assert_eq!(ErrorKind::TooLarge.as_str(), "too_large");
        assert_eq!(ErrorKind::TooLarge.action(), Action::Fix);
        let refusal = WriteRefusal::TooLarge {
            bytes: 12_288,
            max: 8192,
        };
        assert_eq!(refusal.kind(), ErrorKind::TooLarge);
        assert_eq!(
            refusal.to_string(),
            "too_large: a commit of 12288 bytes of pages, at most 8192"
        );
    }
}
