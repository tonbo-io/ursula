//! What callers outside the extension see of a failure (design §8.3, §8.4): a stable kind token
//! ([`ErrorKind`]) with what to do about it ([`Action`]), in every SQL function's error
//! (`<function>: <kind>: <message>`) and in `ursula_status`.
//!
//! A refused write ([`WriteRefusal`]) fails its transaction alone with `SQLITE_IOERR_AUTH`, nothing
//! written. A reset ([`ResetKind`]) fails every statement of the binding: a read with
//! `SQLITE_IOERR_READ`, a write with `SQLITE_IOERR_WRITE`. [`Error`] is a SQL function's failure.

use std::sync::Arc;

use crate::driver::SyncError;
use crate::driver::runtime::DriverGone;
use crate::host::HostError;
use crate::open::OpenError;
use crate::options::OptionsError;
use crate::replicator::ResetKind;

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
    /// A write refused at admission (design §4.4).
    BacklogFull,
    /// A write the extension does not capture: EXCLUSIVE locking mode, a commit leaving WAL mode
    /// or 4 KiB pages, a frame above the batch limit (M3 sends parts).
    Unsupported,
    Reset(ResetKind),
    InvalidArgument,
    InvalidUrl,
    NotFound,
    InUse,
    LocalSetup,
    StreamFormat,
    ServerTooOld,
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

impl ErrorKind {
    /// The stable token.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            ErrorKind::BacklogFull => "backlog_full",
            ErrorKind::Unsupported => "unsupported",
            ErrorKind::Reset(kind) => kind.kind(),
            ErrorKind::InvalidArgument => "invalid_argument",
            ErrorKind::InvalidUrl => "invalid_url",
            ErrorKind::NotFound => "not_found",
            ErrorKind::InUse => "in_use",
            ErrorKind::LocalSetup => "local_setup",
            ErrorKind::StreamFormat => "stream_format",
            ErrorKind::ServerTooOld => "server_too_old",
            ErrorKind::WrongCluster => "wrong_cluster",
            ErrorKind::StreamState => "stream_state",
            ErrorKind::ClaimRaces => "claim_races",
            ErrorKind::Unreachable => "unreachable",
            ErrorKind::Unavailable => "unavailable",
            ErrorKind::RateLimited => "rate_limited",
            ErrorKind::Unauthorized => "unauthorized",
            ErrorKind::PaymentRequired => "payment_required",
            ErrorKind::Forbidden => "forbidden",
            ErrorKind::Cancelled => "cancelled",
            ErrorKind::NotBound => "not_bound",
            ErrorKind::Internal => "internal",
        }
    }

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
            | ErrorKind::InvalidArgument
            | ErrorKind::InvalidUrl
            | ErrorKind::NotFound
            | ErrorKind::LocalSetup
            | ErrorKind::StreamFormat
            | ErrorKind::ServerTooOld
            | ErrorKind::WrongCluster
            | ErrorKind::StreamState
            | ErrorKind::Unauthorized
            | ErrorKind::PaymentRequired
            | ErrorKind::Forbidden
            | ErrorKind::NotBound => Action::Fix,
        }
    }
}

/// The action of a reset (design §8.3).
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
    /// The kind callers see (design §8.4).
    pub(crate) fn error_kind(&self) -> ErrorKind {
        use crate::open::Unavailable;
        match self {
            OpenError::InvalidUrl(_) => ErrorKind::InvalidUrl,
            OpenError::InUse { .. } => ErrorKind::InUse,
            OpenError::LocalSetup(_) => ErrorKind::LocalSetup,
            OpenError::Internal(_) | OpenError::Panicked => ErrorKind::Internal,
            OpenError::StreamFormat(_) => ErrorKind::StreamFormat,
            OpenError::ServerTooOld { .. } => ErrorKind::ServerTooOld,
            OpenError::WrongCluster { .. } => ErrorKind::WrongCluster,
            OpenError::StreamState(_) | OpenError::Snapshot(_) => ErrorKind::StreamState,
            OpenError::NotFound => ErrorKind::NotFound,
            OpenError::ClaimRaces => ErrorKind::ClaimRaces,
            OpenError::Unavailable(condition) => match condition {
                Unavailable::Unreachable => ErrorKind::Unreachable,
                Unavailable::RateLimited => ErrorKind::RateLimited,
                Unavailable::Unauthorized => ErrorKind::Unauthorized,
                Unavailable::PaymentRequired => ErrorKind::PaymentRequired,
                Unavailable::Forbidden => ErrorKind::Forbidden,
                Unavailable::WrongCluster => ErrorKind::WrongCluster,
                Unavailable::Retrying
                | Unavailable::Redirected { .. }
                | Unavailable::Rejected { .. }
                | Unavailable::RequestTooLarge => ErrorKind::Unavailable,
            },
            OpenError::Reset(kind) => ErrorKind::Reset(*kind),
            OpenError::Cancelled => ErrorKind::Cancelled,
        }
    }
}

/// Why a write transaction failed alone, nothing of it written or logged.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum WriteRefusal {
    #[error("backlog_full: {pending} bytes logged and not acknowledged, at most {max}")]
    BacklogFull { pending: u64, max: u64 },
    #[error("unsupported: {0}")]
    Unsupported(String),
}

impl WriteRefusal {
    pub(crate) fn kind(&self) -> ErrorKind {
        match self {
            WriteRefusal::BacklogFull { .. } => ErrorKind::BacklogFull,
            WriteRefusal::Unsupported(_) => ErrorKind::Unsupported,
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
    #[error(
        "{path}: this process holds the database's files open outside the extension, whose \
         commits would never reach the stream"
    )]
    OpenOutside { path: String },
    #[error(transparent)]
    DriverGone(#[from] DriverGone),
    #[error("{entry} panicked")]
    Panicked { entry: &'static str },
}

impl Error {
    pub(crate) fn kind(&self) -> ErrorKind {
        match self {
            Error::Open(e) => e.error_kind(),
            Error::Reset(kind) => ErrorKind::Reset(*kind),
            Error::NotBound { .. } | Error::NoRequest { .. } => ErrorKind::NotBound,
            Error::InvalidArgument { .. } | Error::Options(_) => ErrorKind::InvalidArgument,
            Error::Host(e) => e.kind(),
            Error::OpenOutside { .. } => ErrorKind::InUse,
            Error::DriverGone(_) | Error::Panicked { .. } => ErrorKind::Internal,
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
    use super::ErrorKind;
    use super::reset_action;
    use crate::replicator::ResetKind;

    // Every reset kind has the action of design §8.3, and its token is the reset's.
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

    // The write refusals of design §8.4: `backlog_full` is retried after `synced()`, an
    // unsupported write is fixed.
    #[test]
    fn write_refusals_say_what_to_do() {
        assert_eq!(ErrorKind::BacklogFull.as_str(), "backlog_full");
        assert_eq!(ErrorKind::BacklogFull.action(), Action::Retry);
        assert_eq!(ErrorKind::Unsupported.as_str(), "unsupported");
        assert_eq!(ErrorKind::Unsupported.action(), Action::Fix);
    }
}
