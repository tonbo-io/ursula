//! A host's drain of the orphan roots on its disk (design: Hosts, Draining): `ursula-vfs-host
//! drain` runs [`drain`] and prints its [`Drained`] as one line of JSON.
//!
//! A drain is a process of the extension. It reads the extension's settings from its environment,
//! `URSULA_VFS_TOKEN_FILE` included, and never loads SQLite: it opens each root on the driver
//! directly and never registers the binding, so no connection, snapshot thread or hand-out exists.
//!
//! 1. The pass, on a worker: the inspection's walk and per-root read
//!    (`inspection::walk_for_drain`) of the host directory's records and of every `--scan`,
//!    without the lock check. A root reached through two paths is listed once, at its first path.
//! 2. The plan, a pure function of each root's read (`plan`): a root that could not be read, or
//!    holds nothing, is not opened, nor is one another user owns (`not_owner`) or one whose name
//!    is not its stream's (`misnamed`).
//! 3. The open as a drain (`OwnerMode::Drain`): it only continues the root's own claim behind
//!    the pre-check and never takes the stream over (design: Pre-check). Its deadline ends
//!    [`GRACE`] before the drain's.
//! 4. The binding is given back at once, as the driver's cancel of an untaken result gives it
//!    back, and retires once every commit it held is acknowledged, after an exact, fsynced ACK.
//!    The drain waits until it released its root.
//! 5. The root is read again, and its bytes decide its outcome (`decide`).
//!
//! Up to `--concurrency` streams drain at once, the roots of one stream one after another. The
//! main thread owns the deadline: it waits until every root has an outcome or the deadline passes,
//! and prints what the workers recorded. It never joins them.

use std::collections::VecDeque;
use std::fs;
use std::net::Ipv6Addr;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

use crate::config;
use crate::db;
use crate::driver::Bound;
use crate::driver::runtime::Driver;
use crate::driver::runtime::driver;
use crate::error::ErrorKind;
use crate::generation::canonical_path;
use crate::generation::root_name;
use crate::guard;
use crate::hostdir;
use crate::ids::Activation;
use crate::inspection;
use crate::inspection::Listed;
use crate::inspection::PassError;
use crate::inspection::ReadAgain;
use crate::inspection::RootError;
use crate::inspection::RootState;
use crate::inspection::WalkError;
use crate::json;
use crate::log;
use crate::log::Level;
use crate::open::OpenError;
use crate::open::OpenSpec;
use crate::open::table::Declined;
use crate::options::Access;
use crate::options::DEFAULT_MAX_PENDING_BYTES;
use crate::options::OwnerMode;
use crate::replicator::BlockedReason;
use crate::replicator::Condition;
use crate::replicator::ResetKind;

/// The version of the drain's JSON.
const VERSION: u64 = 1;
/// How long before the drain's deadline its opens end, and its waits for a binding: the time its
/// last reads and the print take.
pub const GRACE: Duration = Duration::from_millis(250);
/// The most streams that drain at once.
pub const MAX_CONCURRENCY: u64 = 16;
/// The streams that drain at once by default.
pub const DEFAULT_CONCURRENCY: u64 = 4;

/// What [`drain`] drains.
#[derive(Debug, Clone)]
pub struct Drain {
    pub origin: Origin,
    /// The drain prints once every root has an outcome, and at this after it started at the
    /// latest.
    pub budget: Duration,
    /// Directories to read besides the recorded ones, as for an inspection.
    pub scans: Vec<PathBuf>,
    pub concurrency: Concurrency,
}

/// Where the deployment answers: `http://` or `https://`, a host and an optional port, in the form
/// a stream URL starts with (no trailing `/`, the scheme in lowercase).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Origin(String);

/// A `--origin` that is not an origin.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "{origin:?} is not an origin ({why}): it takes http:// or https://, a host, an optional port \
     and at most one trailing /"
)]
pub struct OriginError {
    origin: String,
    why: NotAnOrigin,
}

/// How a `--origin` is not an origin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NotAnOrigin {
    #[error("no http:// or https://")]
    Scheme,
    #[error("it has userinfo")]
    Userinfo,
    #[error("it has a path")]
    Path,
    #[error("it has a query or a fragment")]
    QueryOrFragment,
    #[error("it has no host")]
    NoHost,
    #[error("its host is not a name, an IPv4 address or a bracketed IPv6 literal")]
    Host,
    #[error("its port is not a number from 0 to 65535")]
    Port,
}

impl TryFrom<&str> for Origin {
    type Error = OriginError;

    fn try_from(text: &str) -> Result<Self, Self::Error> {
        let fail = |why| OriginError {
            origin: text.to_owned(),
            why,
        };
        let (scheme, rest) = text
            .split_once("://")
            .ok_or_else(|| fail(NotAnOrigin::Scheme))?;
        if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
            return Err(fail(NotAnOrigin::Scheme));
        }
        if rest.contains(['?', '#']) {
            return Err(fail(NotAnOrigin::QueryOrFragment));
        }
        // Without the removal the stream URL would hold an empty segment, and its canonical path
        // another one.
        let authority = rest.strip_suffix('/').unwrap_or(rest);
        if authority.contains('/') {
            return Err(fail(NotAnOrigin::Path));
        }
        if authority.contains('@') {
            return Err(fail(NotAnOrigin::Userinfo));
        }
        let (host, port) = match authority.strip_prefix('[') {
            Some(literal) => {
                let (v6, after) = literal
                    .split_once(']')
                    .ok_or_else(|| fail(NotAnOrigin::Host))?;
                if v6.parse::<Ipv6Addr>().is_err() {
                    return Err(fail(NotAnOrigin::Host));
                }
                match after {
                    "" => (v6, None),
                    after => (
                        v6,
                        Some(
                            after
                                .strip_prefix(':')
                                .ok_or_else(|| fail(NotAnOrigin::Host))?,
                        ),
                    ),
                }
            }
            None => {
                let (host, port) = match authority.split_once(':') {
                    Some((host, port)) => (host, Some(port)),
                    None => (authority, None),
                };
                if host.is_empty() {
                    return Err(fail(NotAnOrigin::NoHost));
                }
                let name = |b: u8| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_');
                if !host.bytes().all(name) {
                    return Err(fail(NotAnOrigin::Host));
                }
                (host, port)
            }
        };
        if host.is_empty() {
            return Err(fail(NotAnOrigin::NoHost));
        }
        if let Some(port) = port
            && (port.is_empty()
                || !port.bytes().all(|b| b.is_ascii_digit())
                || port.parse::<u16>().is_err())
        {
            return Err(fail(NotAnOrigin::Port));
        }
        Ok(Origin(format!(
            "{}://{authority}",
            scheme.to_ascii_lowercase()
        )))
    }
}

impl Origin {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The URL of the stream at the canonical path `stream`.
    fn url(&self, stream: &str) -> String {
        format!("{}{stream}", self.0)
    }
}

/// The number of streams that drain at once: 1 to [`MAX_CONCURRENCY`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Concurrency(u64);

/// A `--concurrency` out of range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{0} streams at once is not 1 to {MAX_CONCURRENCY}")]
pub struct ConcurrencyError(u64);

impl TryFrom<u64> for Concurrency {
    type Error = ConcurrencyError;

    fn try_from(n: u64) -> Result<Self, Self::Error> {
        if (1..=MAX_CONCURRENCY).contains(&n) {
            Ok(Self(n))
        } else {
            Err(ConcurrencyError(n))
        }
    }
}

impl Default for Concurrency {
    fn default() -> Self {
        Self(DEFAULT_CONCURRENCY)
    }
}

/// Whether a later drain can help a root that still holds bytes (`later`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Later {
    /// The cause passes by itself.
    Yes,
    /// A later drain may drain it once something outside it changes.
    Fix,
    /// No drain can send these commits to this stream.
    No,
}

impl Later {
    fn as_str(self) -> &'static str {
        match self {
            Later::Yes => "yes",
            Later::Fix => "fix",
            Later::No => "no",
        }
    }
}

/// Why a root still holds unacknowledged commits after its drain (`kind`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Undrained {
    /// The resolved root belongs to another user than the drain's.
    NotOwner,
    /// The root's name is not its stream's root name.
    Misnamed,
    /// The open refused the root (`OpenError::error_kind`).
    Open(ErrorKind),
    /// The open declined the root.
    Declined(Declined),
    /// The binding reset.
    Reset(ResetKind),
    /// The binding's condition at the deadline.
    Condition(Condition),
    /// The drain could not check or open the root: its effective uid unread, the driver gone, or a
    /// panic.
    Internal,
    /// The deadline came with no condition to name, or before the root's turn.
    Budget,
    /// The binding drained what it held and released the root, and the root holds commits after
    /// its latest ACK: another process logged commits in it since, or the retire could not write
    /// its exact ACK (the driver logs `ack_lost`).
    Reopened,
}

impl Undrained {
    /// The kind's token, from the existing token functions for every kind the extension already
    /// names.
    pub(crate) fn token(&self) -> &'static str {
        match self {
            Undrained::NotOwner => "not_owner",
            Undrained::Misnamed => "misnamed",
            Undrained::Open(kind) => kind.as_str(),
            Undrained::Declined(declined) => declined.kind(),
            Undrained::Reset(kind) => kind.kind(),
            Undrained::Condition(condition) => condition.token(),
            Undrained::Internal => ErrorKind::Internal.as_str(),
            Undrained::Budget => "budget",
            Undrained::Reopened => "reopened",
        }
    }

    /// Whether a later drain can help: one value per token, whatever produced it.
    pub(crate) fn later(&self) -> Later {
        match self {
            Undrained::NotOwner => Later::Fix,
            Undrained::Misnamed => Later::No,
            Undrained::Open(kind) => later_of(*kind),
            Undrained::Declined(Declined::Replaced) => Later::No,
            Undrained::Declined(Declined::NoClaim | Declined::NothingPending)
            | Undrained::Internal => later_of(ErrorKind::Internal),
            Undrained::Reset(kind) => later_of(ErrorKind::Reset(*kind)),
            Undrained::Condition(condition) => match condition {
                Condition::Retrying => Later::Yes,
                Condition::Blocked(reason) => match reason {
                    BlockedReason::Unauthorized => later_of(ErrorKind::Unauthorized),
                    BlockedReason::PaymentRequired => later_of(ErrorKind::PaymentRequired),
                    BlockedReason::Forbidden => later_of(ErrorKind::Forbidden),
                    BlockedReason::WrongCluster => later_of(ErrorKind::WrongCluster),
                    BlockedReason::NoServerIdentity => later_of(ErrorKind::NoServerIdentity),
                    BlockedReason::ProtocolMismatch { .. } => later_of(ErrorKind::ProtocolMismatch),
                    BlockedReason::RequestTooLarge
                    | BlockedReason::Redirected { .. }
                    | BlockedReason::Rejected { .. } => Later::Fix,
                },
            },
            Undrained::Budget | Undrained::Reopened => Later::Yes,
        }
    }
}

/// `later` of an [`ErrorKind`]. A drain cannot meet the kinds of SQL, of commits, of the registry,
/// of takeovers, of `synced()` or of a forked child, and they map to `no`, so the function is
/// total.
fn later_of(kind: ErrorKind) -> Later {
    match kind {
        ErrorKind::InUse
        | ErrorKind::Unreachable
        | ErrorKind::Unavailable
        | ErrorKind::RateLimited => Later::Yes,
        ErrorKind::LocalSetup
        | ErrorKind::WrongCluster
        | ErrorKind::ProtocolMismatch
        | ErrorKind::NoServerIdentity
        | ErrorKind::Unauthorized
        | ErrorKind::PaymentRequired
        | ErrorKind::Forbidden
        | ErrorKind::Reset(ResetKind::LocalDamage) => Later::Fix,
        ErrorKind::NotFound
        | ErrorKind::StreamFormat
        | ErrorKind::StreamState
        | ErrorKind::Internal
        | ErrorKind::Reset(
            ResetKind::Fenced
            | ResetKind::Stale
            | ResetKind::Rewound
            | ResetKind::Deleted
            | ResetKind::StreamState
            | ResetKind::Internal,
        )
        | ErrorKind::BacklogFull
        | ErrorKind::Unsupported
        | ErrorKind::TooLarge
        | ErrorKind::InvalidArgument
        | ErrorKind::InvalidUrl
        | ErrorKind::BoundElsewhere
        | ErrorKind::ClaimRaces
        | ErrorKind::Cancelled
        | ErrorKind::NotBound
        | ErrorKind::Forked => Later::No,
    }
}

/// What a root's read holds, as an inspection counts it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Counts {
    /// The frame bytes of its commits after the latest ACK.
    pub(crate) bytes: u64,
    pub(crate) commits: u64,
    pub(crate) salvaged: u64,
}

/// A root's first read, by the pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FirstRead {
    pub(crate) stream: String,
    pub(crate) cluster: String,
    pub(crate) counts: Counts,
}

/// A root's read: what it holds, or why the inspection's read of it failed.
pub(crate) type Read = Result<FirstRead, Arc<RootError>>;

/// Why a root is pending as it is, for a person: the source of its `error`, rendered only in the
/// JSON.
#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum Note {
    /// The open's refusal or decline, whose message starts with its own token.
    #[error(transparent)]
    Open(Arc<OpenError>),
    #[error("the drain could not read its effective uid, so it opens nothing")]
    NoUid,
    #[error("the root belongs to {}, and the drain runs as uid {euid}", owned_by(.owner))]
    NotOwner { owner: Option<u32>, euid: u32 },
    #[error(
        "the root's name is not the root name of its stream {stream:?}: it was renamed or copied"
    )]
    Misnamed { stream: String },
    #[error("the deadline came before the root's turn")]
    BeforeTurn,
    #[error("the open had not resolved at the deadline")]
    Unresolved,
    #[error("the binding's condition at the deadline, its commits not all acknowledged")]
    Condition,
    #[error("the binding had not drained what it held at the deadline")]
    Holding,
    #[error("the deadline came before the drain of the root ended")]
    Unfinished,
    #[error("the deadline cut the read after the drain")]
    ReadCut,
    #[error("the drain's binding reset with its commits not sent")]
    Reset,
    #[error(
        "the drain's binding drained what it held and released the root, and the root holds \
         commits after its latest ACK: another process logged commits in it since, or the retire \
         could not write its exact ACK"
    )]
    Reopened,
    #[error("the driver did not start, or is gone")]
    DriverGone,
    #[error("no worker could drain it")]
    NoWorker,
    #[error("the drain of the root panicked")]
    Panicked,
    #[error("the drain of an earlier root of its stream panicked, so it was not opened")]
    StreamPanicked,
    #[error("the drain's pass panicked")]
    PassPanicked,
}

/// Who owns a root, for [`Note::NotOwner`].
fn owned_by(owner: &Option<u32>) -> String {
    owner.map_or_else(
        || "an owner it could not read".to_owned(),
        |u| format!("uid {u}"),
    )
}

/// A root's outcome (`outcome`, with its `kind` for a pending one).
#[derive(Debug, Clone)]
pub(crate) enum Outcome {
    /// The drain bound the root, its binding held commits, and the last read found 0 bytes.
    Drained(Counts),
    /// The last read found 0 bytes and the drain drained nothing.
    Clean(Counts),
    /// The last completed read found bytes: why, and a note for a person.
    Pending {
        why: Undrained,
        counts: Counts,
        note: Note,
    },
    /// The last read failed.
    Unreadable { error: Arc<RootError> },
}

impl Outcome {
    fn pending(why: Undrained, counts: Counts, note: Note) -> Self {
        Outcome::Pending { why, counts, note }
    }
}

/// The `error` of a pending root: its note, after its kind's token unless the note starts with
/// it.
fn pending_error(why: &Undrained, note: &Note) -> String {
    match note {
        Note::Open(e) => e.to_string(),
        other => format!("{}: {other}", why.token()),
    }
}

/// What the plan does with a root.
#[derive(Debug, Clone)]
pub(crate) enum Plan {
    /// Not opened: its outcome now.
    Done(Outcome),
    /// Drained: opened as a drain at `url` in the cache root `dir`, with what its first read found.
    Drain {
        url: String,
        dir: PathBuf,
        stream: String,
        first: Counts,
    },
}

/// What the plan reads of a root besides its first read.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Facts<'a> {
    /// The root directory's real path.
    pub(crate) resolved: &'a Path,
    /// Its owner, `None` when it could not be looked at.
    pub(crate) owner: Option<u32>,
    /// The drain's effective uid, `None` when it could not be read.
    pub(crate) euid: Option<u32>,
}

/// The plan of a root, a pure function of its first read and of `facts`: a failed read is
/// `unreadable` and 0 bytes `clean`, neither opened. A root with bytes is drained unless the
/// drain cannot check its owner, another user owns it (a file the drain creates in it must stay
/// the owner's), or its name is not its stream's root name (an open of a renamed or copied root
/// would lock, and build, another root).
pub(crate) fn plan(first: &Read, facts: Facts<'_>, origin: &Origin) -> Plan {
    let read = match first {
        Err(error) => {
            return Plan::Done(Outcome::Unreadable {
                error: Arc::clone(error),
            });
        }
        Ok(read) => read,
    };
    if read.counts.bytes == 0 {
        return Plan::Done(Outcome::Clean(read.counts));
    }
    let Some(euid) = facts.euid else {
        return Plan::Done(Outcome::pending(
            Undrained::Internal,
            read.counts,
            Note::NoUid,
        ));
    };
    if facts.owner != Some(euid) {
        return Plan::Done(Outcome::pending(
            Undrained::NotOwner,
            read.counts,
            Note::NotOwner {
                owner: facts.owner,
                euid,
            },
        ));
    }
    let url = origin.url(&read.stream);
    let name = facts.resolved.file_name().and_then(|n| n.to_str());
    let named = canonical_path(&url)
        .ok()
        .filter(|c| c.as_str() == read.stream)
        .map(|c| root_name(&c));
    match (name, named, facts.resolved.parent()) {
        (Some(name), Some(named), Some(dir)) if name == named => Plan::Drain {
            url,
            dir: dir.to_owned(),
            stream: read.stream.clone(),
            first: read.counts,
        },
        _ => Plan::Done(Outcome::pending(
            Undrained::Misnamed,
            read.counts,
            Note::Misnamed {
                stream: read.stream.clone(),
            },
        )),
    }
}

/// What came of a root's open, before its last read.
#[derive(Debug, Clone)]
pub(crate) enum Opened {
    /// Refused or declined, with nothing claimed: why, and the open's error.
    Refused {
        why: Undrained,
        error: Arc<OpenError>,
    },
    /// Bound, given back, and the root released, with the binding's reset if it reset. A drain
    /// binds only a root that holds commits after its latest ACK (`table::decide_drain`).
    Released { reset: Option<ResetKind> },
}

/// What a root's last read found.
#[derive(Debug, Clone)]
pub(crate) enum LastRead {
    Read(Counts),
    Unreadable(Arc<RootError>),
    /// The deadline cut it.
    Cut,
}

/// A drained root's outcome, a pure function of its open, its last read and its first read's
/// numbers: the bytes of the last read that completed decide. A refusal keeps its kind when the
/// read after it fails or is cut, with the first read's numbers, since nothing was claimed.
pub(crate) fn decide(opened: Opened, last: LastRead, first: Counts) -> Outcome {
    match (opened, last) {
        (Opened::Refused { .. }, LastRead::Read(counts)) if counts.bytes == 0 => {
            Outcome::Clean(counts)
        }
        (Opened::Refused { why, error }, LastRead::Read(counts)) => {
            Outcome::pending(why, counts, Note::Open(error))
        }
        (Opened::Refused { why, error }, LastRead::Unreadable(_) | LastRead::Cut) => {
            Outcome::pending(why, first, Note::Open(error))
        }
        (Opened::Released { .. }, LastRead::Unreadable(error)) => Outcome::Unreadable { error },
        (Opened::Released { .. }, LastRead::Cut) => {
            Outcome::pending(Undrained::Budget, first, Note::ReadCut)
        }
        (Opened::Released { .. }, LastRead::Read(counts)) if counts.bytes == 0 => {
            Outcome::Drained(counts)
        }
        (Opened::Released { reset: Some(kind) }, LastRead::Read(counts)) => {
            Outcome::pending(Undrained::Reset(kind), counts, Note::Reset)
        }
        (Opened::Released { reset: None }, LastRead::Read(counts)) => {
            Outcome::pending(Undrained::Reopened, counts, Note::Reopened)
        }
    }
}

/// A root the pass listed, with its outcome once it has one.
#[derive(Debug, Clone)]
struct Entry {
    id: String,
    /// The path the pass listed it at.
    root: PathBuf,
    /// The resolved root directory's owner.
    uid: Option<u32>,
    first: Read,
    outcome: Option<Outcome>,
}

/// What kept the drain from reading everything it should have, or from running (the top-level
/// `error`). The roots it listed stay valid.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Narrowed {
    /// The pass could not read the host directory, a name in it, a directory, or ran out of time.
    #[error(transparent)]
    Walk(#[from] WalkError),
    /// The drain's effective uid could not be read, so it opens nothing.
    #[error(transparent)]
    Uid(PassError),
    #[error("the drain could not start its pass: {0}")]
    Pass(#[source] std::io::Error),
    #[error("the drain could not start a worker: {0}")]
    Worker(#[source] std::io::Error),
    #[error("the pass ran past the drain's budget of {} ms", .0.as_millis())]
    PassBudget(Duration),
    #[error("the drain's pass panicked")]
    PassPanicked,
    #[error("the driver did not start, or is gone")]
    DriverGone,
}

/// What the workers recorded, which the main thread prints.
#[derive(Debug, Default)]
struct Board {
    entries: Vec<Entry>,
    /// The pass and the plan are done, and every root to drain is queued.
    planned: bool,
    /// What the pass could not read, or what kept the drain from running, the first such.
    error: Option<Narrowed>,
}

/// The board and its signal.
#[derive(Debug, Default)]
struct Shared {
    board: Mutex<Board>,
    changed: Condvar,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, Board> {
        self.board.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records `outcome` for entry `index`.
    fn record(&self, index: usize, outcome: Outcome) {
        let token = outcome_token(&outcome);
        let kind = kind_of(&outcome).map_or("null", Undrained::token);
        let root = self.lock().entries.get_mut(index).map(|entry| {
            entry.outcome = Some(outcome);
            entry.root.clone()
        });
        self.changed.notify_all();
        if let Some(root) = root {
            log::emit(Level::Info, "drain", &[
                ("root", &root.display()),
                ("outcome", &token),
                ("kind", &kind),
            ]);
        }
    }

    /// Records what kept the drain from reading or running, the first time.
    fn narrowed(&self, error: Narrowed) {
        let mut board = self.lock();
        if board.error.is_none() {
            board.error = Some(error);
        }
    }

    /// After the pass panicked: every root it listed without an outcome is `internal`, or
    /// `unreadable` when its read failed, and the plan is over.
    fn pass_panicked(&self) {
        self.narrowed(Narrowed::PassPanicked);
        let mut board = self.lock();
        for entry in board.entries.iter_mut().filter(|e| e.outcome.is_none()) {
            entry.outcome = Some(match &entry.first {
                Ok(read) => Outcome::pending(Undrained::Internal, read.counts, Note::PassPanicked),
                Err(error) => Outcome::Unreadable {
                    error: Arc::clone(error),
                },
            });
        }
        board.planned = true;
        drop(board);
        self.changed.notify_all();
    }
}

/// A root to drain, by a worker.
#[derive(Debug)]
struct Job {
    index: usize,
    stream: String,
    /// The resolved root.
    resolved: PathBuf,
    url: String,
    dir: PathBuf,
    first: Counts,
}

/// The result of [`drain`], as [`Drained::to_json`] prints it.
#[derive(Debug)]
pub struct Drained {
    origin: Origin,
    uid: Option<u32>,
    dir: Option<PathBuf>,
    entries: Vec<Entry>,
    error: Option<Narrowed>,
}

/// Drains every root of the host directory's records and of `request.scans` that holds commits
/// the stream has not acknowledged, and returns once every root has an outcome, or once
/// `request.budget` passed (design: Hosts, Draining).
pub fn drain(request: &Drain) -> Drained {
    // The drain makes the driver and its HTTP agents, which live on in the process, so it marks
    // the process started first, as the SQL functions that make them do (design: Forks).
    db::start_process();
    let start = Instant::now();
    let deadline = start.checked_add(request.budget);
    let shared = Arc::new(Shared::default());
    let dir = hostdir::host_dir();
    let euid = match inspection::effective_uid() {
        Ok(euid) => Some(euid),
        Err(e) => {
            shared.narrowed(Narrowed::Uid(e));
            None
        }
    };
    let pass = {
        let (shared, request, dir) = (Arc::clone(&shared), request.clone(), dir.clone());
        std::thread::Builder::new()
            .name("ursula-vfs-drain".to_owned())
            .spawn(move || {
                guard::contain(
                    "drain_pass",
                    || {
                        shared.pass_panicked();
                        Vec::new()
                    },
                    || run(&shared, &request, dir.as_deref(), euid, deadline),
                )
            })
    };
    // The workers are never joined: the process exits at the deadline whatever they do, as a
    // process of the extension exits with its driver running.
    let _pass: Option<JoinHandle<Vec<JoinHandle<()>>>> = match pass {
        Ok(handle) => Some(handle),
        Err(e) => {
            shared.narrowed(Narrowed::Pass(e));
            let mut board = shared.lock();
            board.planned = true;
            None
        }
    };
    let mut board = shared.lock();
    loop {
        if board.planned && board.entries.iter().all(|e| e.outcome.is_some()) {
            break;
        }
        let left = deadline.map_or(Duration::MAX, |d| {
            d.saturating_duration_since(Instant::now())
        });
        if left.is_zero() {
            if !board.planned && board.error.is_none() {
                board.error = Some(Narrowed::PassBudget(request.budget));
            }
            break;
        }
        board = shared
            .changed
            .wait_timeout(board, left)
            .unwrap_or_else(PoisonError::into_inner)
            .0;
    }
    Drained {
        origin: request.origin.clone(),
        uid: euid,
        dir,
        entries: board.entries.clone(),
        error: board.error.take(),
    }
}

/// The pass, the plan and the drains, on the pass's worker. Returns the stream workers it
/// started.
fn run(
    shared: &Arc<Shared>,
    request: &Drain,
    dir: Option<&Path>,
    euid: Option<u32>,
    deadline: Option<Instant>,
) -> Vec<JoinHandle<()>> {
    let mut seen = std::collections::HashSet::new();
    let mut jobs: Vec<Job> = Vec::new();
    let narrowed = inspection::walk_for_drain(
        dir,
        &request.scans,
        deadline,
        request.budget,
        &mut |listed: Listed| {
            let resolved =
                fs::canonicalize(&listed.root).unwrap_or_else(|_gone| listed.root.clone());
            if !seen.insert(resolved.clone()) {
                return;
            }
            let owner = fs::metadata(&resolved).ok().map(|m| m.uid());
            let first = first_read(&listed.state);
            let facts = Facts {
                resolved: &resolved,
                owner,
                euid,
            };
            let planned = plan(&first, facts, &request.origin);
            let mut board = shared.lock();
            let index = board.entries.len();
            let outcome = match planned {
                Plan::Done(outcome) => Some(outcome),
                Plan::Drain {
                    url,
                    dir,
                    stream,
                    first: counts,
                } => {
                    jobs.push(Job {
                        index,
                        stream,
                        resolved: resolved.clone(),
                        url,
                        dir,
                        first: counts,
                    });
                    None
                }
            };
            board.entries.push(Entry {
                id: listed.id,
                root: listed.root,
                uid: owner,
                first,
                outcome,
            });
        },
    );
    if let Some(error) = narrowed {
        shared.narrowed(error.into());
    }
    let workers = start_workers(shared, request.concurrency, jobs, deadline);
    shared.lock().planned = true;
    shared.changed.notify_all();
    workers
}

/// A root's first read, as the plan reads it.
fn first_read(state: &RootState) -> Read {
    match state {
        RootState::Read {
            stream,
            cluster,
            bytes,
            commits,
            salvaged,
        } => Ok(FirstRead {
            stream: stream.clone(),
            cluster: cluster.clone(),
            counts: Counts {
                bytes: *bytes,
                commits: *commits,
                salvaged: *salvaged,
            },
        }),
        RootState::Unreadable { error, .. } => Err(Arc::clone(error)),
    }
}

/// Groups `jobs` by stream, in the pass's order, and starts up to `concurrency` workers that drain
/// one stream at a time, its roots one after another.
fn start_workers(
    shared: &Arc<Shared>,
    concurrency: Concurrency,
    jobs: Vec<Job>,
    deadline: Option<Instant>,
) -> Vec<JoinHandle<()>> {
    let mut streams: Vec<Vec<Job>> = Vec::new();
    for job in jobs {
        match streams
            .iter_mut()
            .find(|s| s.first().is_some_and(|j| j.stream == job.stream))
        {
            Some(of) => of.push(job),
            None => streams.push(vec![job]),
        }
    }
    let count = usize::try_from(concurrency.0)
        .unwrap_or(1)
        .min(streams.len());
    let queue = Arc::new(Mutex::new(VecDeque::from(streams)));
    let mut workers = Vec::new();
    for _ in 0..count {
        let (board, queue) = (Arc::clone(shared), Arc::clone(&queue));
        let spawned = std::thread::Builder::new()
            .name("ursula-vfs-drain".to_owned())
            .spawn(move || {
                loop {
                    let next = queue
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .pop_front();
                    let Some(stream) = next else {
                        break;
                    };
                    drain_stream(&board, stream, deadline);
                }
            });
        match spawned {
            Ok(worker) => workers.push(worker),
            Err(e) => shared.narrowed(Narrowed::Worker(e)),
        }
    }
    if count > 0 && workers.is_empty() {
        // No worker took the streams.
        let stranded = std::mem::take(&mut *queue.lock().unwrap_or_else(PoisonError::into_inner));
        for job in stranded.into_iter().flatten() {
            shared.record(
                job.index,
                Outcome::pending(Undrained::Internal, job.first, Note::NoWorker),
            );
        }
    }
    workers
}

/// Drains the roots of one stream one after another. A drain that panicked may have left its
/// binding bound, so the stream's later roots are not opened: a process binds a stream under one
/// root at a time.
fn drain_stream(shared: &Shared, stream: Vec<Job>, deadline: Option<Instant>) {
    let mut panicked = false;
    for job in stream {
        let outcome = if panicked {
            Outcome::pending(Undrained::Internal, job.first, Note::StreamPanicked)
        } else {
            match guard::contain(
                "drain_root",
                || None,
                || Some(drain_root(shared, &job, deadline)),
            ) {
                Some(outcome) => outcome,
                None => {
                    panicked = true;
                    Outcome::pending(Undrained::Internal, job.first, Note::Panicked)
                }
            }
        };
        shared.record(job.index, outcome);
    }
}

/// The time left until `deadline` less [`GRACE`], `None` once nothing is left.
fn left_before_grace(deadline: Option<Instant>) -> Option<Duration> {
    match deadline {
        None => Some(Duration::MAX),
        Some(deadline) => deadline
            .saturating_duration_since(Instant::now())
            .checked_sub(GRACE)
            .filter(|left| !left.is_zero()),
    }
}

/// Drains the root of `job` (steps 3 to 5): opens it as a drain, gives the binding back at once,
/// waits until it released the root, and reads the root again. A driver that did not start, or is
/// gone, also narrows the whole drain.
fn drain_root(shared: &Shared, job: &Job, deadline: Option<Instant>) -> Outcome {
    let budget = |note| Outcome::pending(Undrained::Budget, job.first, note);
    let Some(left) = left_before_grace(deadline) else {
        return budget(Note::BeforeTurn);
    };
    let spec = OpenSpec {
        url: job.url.clone(),
        dir: job.dir.clone(),
        // Its own, never one the workload or the host shares (design: Limits, A shared
        // activation).
        activation: Activation::draw(),
        max_pending_bytes: DEFAULT_MAX_PENDING_BYTES,
        max_request_bytes: config::max_request_bytes(),
        max_commit_bytes: config::max_commit_bytes(),
        timeout_ms: u64::try_from(left.as_millis()).unwrap_or(u64::MAX),
        access: Access::Owner {
            mode: OwnerMode::Drain,
        },
    };
    let driver = driver();
    let gone = || {
        shared.narrowed(Narrowed::DriverGone);
        Outcome::pending(Undrained::Internal, job.first, Note::DriverGone)
    };
    let Ok(open) = driver.open(spec) else {
        return gone();
    };
    let until = deadline.map_or(Duration::MAX, |d| {
        d.saturating_duration_since(Instant::now())
    });
    if !driver.wait_resolved(open, until) {
        if driver.cancel(open).is_err() {
            return gone();
        }
        return budget(Note::Unresolved);
    }
    let opened = match driver.try_open(open) {
        None => return budget(Note::Unresolved),
        Some(Err(e)) => Opened::Refused {
            why: refusal(&e),
            error: e,
        },
        Some(Ok(bound)) => match hand_back(driver, &bound, deadline) {
            Waited::Released => Opened::Released {
                reset: bound.handle.reset(),
            },
            Waited::Deadline(Some(condition)) => {
                return Outcome::pending(
                    Undrained::Condition(condition),
                    job.first,
                    Note::Condition,
                );
            }
            Waited::Deadline(None) => return budget(Note::Holding),
            Waited::Gone => return gone(),
        },
    };
    let last = match inspection::read_for_drain(&job.resolved, deadline) {
        ReadAgain::Read(state) => match first_read(&state) {
            Ok(read) => LastRead::Read(read.counts),
            Err(error) => LastRead::Unreadable(error),
        },
        ReadAgain::NoGeneration => LastRead::Read(Counts::default()),
        ReadAgain::Cut => LastRead::Cut,
    };
    decide(opened, last, job.first)
}

/// The kind of an open's refusal: a decline's own, else the open's error kind.
fn refusal(e: &OpenError) -> Undrained {
    match e {
        OpenError::Declined(declined) => Undrained::Declined(*declined),
        other => Undrained::Open(other.error_kind()),
    }
}

/// How the wait for a binding given back ended.
#[derive(Debug)]
enum Waited {
    /// It released its root: after its exact, fsynced ACK, or after a reset and its latest ACK.
    Released,
    /// [`GRACE`] before the deadline it still held the root, with this condition.
    Deadline(Option<Condition>),
    /// The driver is gone.
    Gone,
}

/// Gives `bound` back at once, as the driver's cancel of an untaken result does, and waits, woken
/// by the driver's events, until it released its root, or until [`GRACE`] before `deadline`.
fn hand_back(driver: &Driver, bound: &Bound, deadline: Option<Instant>) -> Waited {
    bound.handle.retiring();
    if driver.retire(bound.binding).is_err() {
        return Waited::Gone;
    }
    // A release after the check below moves the events' cursor past this one: the wait returns.
    let mut cursor = 0;
    loop {
        if !bound.handle.holds_root() {
            return Waited::Released;
        }
        let Some(left) = left_before_grace(deadline) else {
            return Waited::Deadline(bound.handle.replication().condition);
        };
        cursor = driver.events(cursor, left).1;
    }
}

fn outcome_token(outcome: &Outcome) -> &'static str {
    match outcome {
        Outcome::Drained(_) => "drained",
        Outcome::Clean(_) => "clean",
        Outcome::Pending { .. } => "pending",
        Outcome::Unreadable { .. } => "unreadable",
    }
}

fn kind_of(outcome: &Outcome) -> Option<&Undrained> {
    match outcome {
        Outcome::Pending { why, .. } => Some(why),
        Outcome::Drained(_) | Outcome::Clean(_) | Outcome::Unreadable { .. } => None,
    }
}

impl Drained {
    /// The result as one line of JSON (design: Hosts, Draining). Within `v` 1 its keys and their
    /// order are fixed, and only the kinds may grow.
    pub fn to_json(&self) -> String {
        let path = |p: &Path| json::str(&p.to_string_lossy());
        let roots: Vec<String> = self.entries.iter().map(|e| entry_json(e, &path)).collect();
        json::object(&[
            ("v", VERSION.to_string()),
            ("origin", json::str(self.origin.as_str())),
            ("uid", self.uid.map_or_else(json::null, |u| u.to_string())),
            ("dir", self.dir.as_deref().map_or_else(json::null, path)),
            ("roots", json::array(&roots)),
            (
                "error",
                self.error
                    .as_ref()
                    .map_or_else(json::null, |e| json::str(&e.to_string())),
            ),
        ])
    }
}

fn entry_json(e: &Entry, path: &dyn Fn(&Path) -> String) -> String {
    let null = json::null;
    let int = |n: u64| n.to_string();
    let (stream, cluster, before) = match &e.first {
        Ok(read) => (
            json::str(&read.stream),
            json::str(&read.cluster),
            int(read.counts.bytes),
        ),
        Err(_) => (null(), null(), null()),
    };
    // A root still being drained at the deadline, or never reached, holds what its first read
    // found.
    let outcome = match (&e.outcome, &e.first) {
        (Some(outcome), _) => outcome.clone(),
        (None, Ok(read)) => Outcome::pending(Undrained::Budget, read.counts, Note::Unfinished),
        (None, Err(error)) => Outcome::Unreadable {
            error: Arc::clone(error),
        },
    };
    let counts = |c: &Counts| (int(c.bytes), int(c.commits), int(c.salvaged));
    let (kind, later, (bytes, commits, salvaged), error) = match &outcome {
        Outcome::Drained(c) | Outcome::Clean(c) => (null(), null(), counts(c), null()),
        Outcome::Pending {
            why,
            counts: c,
            note,
        } => (
            json::str(why.token()),
            json::str(why.later().as_str()),
            counts(c),
            json::str(&pending_error(why, note)),
        ),
        Outcome::Unreadable { error } => (
            null(),
            null(),
            (null(), null(), null()),
            json::str(&error.to_string()),
        ),
    };
    json::object(&[
        ("id", json::str(&e.id)),
        ("root", path(&e.root)),
        ("stream", stream),
        ("cluster", cluster),
        ("uid", e.uid.map_or_else(null, |u| u.to_string())),
        ("outcome", json::str(outcome_token(&outcome))),
        ("kind", kind),
        ("later", later),
        ("before_bytes", before),
        ("unacknowledged_bytes", bytes),
        ("commits", commits),
        ("salvaged", salvaged),
        ("error", error),
    ])
}

#[cfg(test)]
mod tests;
