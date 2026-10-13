//! The crate's error type: why an operation failed, or why an attached database is poisoned.
//!
//! Callers act on four classes, through methods rather than message text: [`Error::is_gone`]
//! (the data lies below the stream's retention: attach retries from a newer snapshot),
//! [`Error::is_recreated`] (the stream is another incarnation: attach rebuilds),
//! [`Error::is_fenced`] (another owner or writer holds the stream: `ursula_status` reports
//! `fenced`) and [`Error::refusal`] (the request was refused before it reached the stream, which
//! applied nothing of it, see [`WriteRefusal`]: the operation fails alone, and `ursula_attach`
//! reports it as SQLITE_AUTH for a 402, SQLITE_IOERR_AUTH otherwise). Every other variant fails
//! the operation, or poisons the database until it is attached again. What callers outside the
//! extension see is [`Error::kind`]: a stable token ([`ErrorKind`]) with what to do about it
//! ([`Action`]), in every SQL function's error and in `ursula_status`.

use std::borrow::Cow;
use std::ffi::CStr;
use std::ffi::c_int;
use std::sync::Arc;
use std::time::Duration;

use crate::descriptors::DbFile;
use crate::frame::FrameError;
use crate::frame::PAGE;
use crate::snapshot::SnapshotError;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    // Local files and the host process.
    /// A C entry point panicked (`guard::contain`). After a file method, the extension's state for
    /// its database may be half changed, so the database is poisoned and needs a new attach.
    #[error("{entry} panicked inside the extension")]
    Panicked { entry: &'static str },
    #[error("{op} {path}: {source}")]
    Io {
        op: &'static str,
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{path:?}: {source}")]
    NulInPath {
        path: String,
        #[source]
        source: std::ffi::NulError,
    },
    #[error(
        "{path} is open by another process (pid {}); close it before attaching",
        .pid.map_or_else(|| "unknown".to_owned(), |pid| pid.to_string())
    )]
    OpenElsewhere {
        path: String,
        pid: Option<libc::pid_t>,
    },
    #[error("{path} is attached by another process ({lock_path}: {source})")]
    AttachedElsewhere {
        path: String,
        lock_path: String,
        #[source]
        source: std::fs::TryLockError,
    },
    #[error("{path} has open connections; close them before attaching")]
    OpenConnections { path: String },
    /// The process holds the db file, its `-wal`, `-shm` or `-journal` open on a descriptor no
    /// connection of this VFS owns (`descriptors::open_in_process`): a SQLite connection outside the
    /// extension (opened before the extension was loaded, or through another VFS, a read-only one
    /// included), whose commits would never reach the stream and which shares the file's
    /// wal-index with the attachment, or another open of the file. Refused up front: nothing
    /// changes.
    #[error(
        "{path} is open in this process outside the extension ({file} on descriptor {fd}): close \
         every SQLite connection to it that the extension did not open (one opened before the \
         extension was loaded, or through another VFS, read-only ones included) and any other \
         open of the file, then attach again (SQLite keeps the descriptors of closed connections \
         open while such a connection holds a lock on the file)"
    )]
    OpenOutsideVfs { path: String, fd: i32, file: DbFile },
    #[error("{path} is being attached by another thread")]
    AttachInProgress { path: String },
    /// The path is not attached in this process: never, or its last attach failed. The message of
    /// a failed one starts with that failure's kind, whose action is what to do about it.
    #[error(
        "{}{path} is not attached{}",
        .last_failure.as_ref().map_or_else(String::new, |e| format!("{}: ", e.kind().as_str())),
        .last_failure.as_ref().map_or_else(String::new, |e| format!(": its last attach failed ({e})"))
    )]
    NotAttached {
        path: String,
        last_failure: Option<Arc<Error>>,
    },
    /// A db file with data and no readable sidecar: a database never replicated (or one whose
    /// sidecar was lost). Its pages may be in no stream, so attach never replays a stream over it.
    #[error(
        "{path} has data but no readable sidecar ({source}): it was never replicated, or its \
         sidecar was lost, so its pages may be in no stream; refusing to attach (to replicate its \
         rows, attach a new file and copy them into it; to attach this path anyway, move {path} \
         and {path}-wal aside first)"
    )]
    NoSidecar {
        path: String,
        #[source]
        source: Box<Error>,
    },
    #[error(
        "{path} is a cache of stream {cached}, not {wanted}; to attach this path to {wanted}, move \
         {path} and {path}-wal aside first"
    )]
    OtherStream {
        path: String,
        cached: String,
        wanted: String,
    },
    /// The files attach to no other URL (`OtherStream`) and a copy of them has no sidecar
    /// (`NoSidecar`), so the data is salvaged by reading a copy as a plain SQLite file.
    #[error(
        "{url} is missing (or hidden from this client's credentials) and {path} holds data: \
         refusing to create an empty stream over it, and keeping the files (to salvage the data, \
         copy {path} and {path}-wal to a new name and export the copy's rows into a newly \
         attached database; to start over, move {path} and {path}-wal aside, or attach a new file \
         under another URL)"
    )]
    StreamMissing { path: String, url: String },
    #[error(
        "{url} is missing (or hidden from this client's credentials), and a read-only attach never \
         creates a stream"
    )]
    NoStream { url: String },
    #[error("unknown attach mode {mode:?}: expected 'owner' or 'read_only'")]
    AttachMode { mode: String },
    #[error("spawn the snapshot thread: {0}")]
    SpawnSnapshotThread(#[source] std::io::Error),

    // The host's SQLite.
    #[error("xFullPathname({path}): {code}")]
    FullPathname { path: String, code: c_int },
    #[error("open {path}: {message}")]
    SqliteOpen { path: String, message: String },
    #[error("{sql:?}: {message}")]
    SqliteQuery { sql: &'static CStr, message: String },
    #[error("{sql:?}: no result row")]
    NoResultRow { sql: &'static CStr },
    #[error("persist WAL: {0}")]
    PersistWal(c_int),
    #[error("{file} handle of a private connection: {code}")]
    FileHandle { file: &'static str, code: c_int },
    #[error("read db file pages: {code} (file shorter than {pages} pages?)")]
    ReadPages { code: c_int, pages: u32 },
    #[error("the host's SQLite has no {0}")]
    MissingRoutine(&'static str),

    // The stream.
    #[error("{op} {url}: {source}")]
    Http {
        op: &'static str,
        url: String,
        #[source]
        source: Box<ureq::Error>,
    },
    #[error("{op} {url}: {status}{}", with_body(.body))]
    Status {
        op: &'static str,
        url: String,
        status: u16,
        body: String,
    },
    /// A redirect the extension does not retry (any 3xx but 307, a 308 included:
    /// `client::redirected`).
    /// It never follows one: `location` is where it pointed.
    #[error(
        "{op} {url}: {status} redirect{}: the extension never follows redirects (point the stream \
         URL at the server or gateway itself)",
        .location.as_ref().map_or_else(String::new, |l| format!(" to {l}"))
    )]
    Redirected {
        op: &'static str,
        url: String,
        status: u16,
        location: Option<String>,
    },
    #[error("append rejected: {status}{}", with_body(.body))]
    AppendRejected { status: u16, body: String },
    #[error("append: {last} (outcome unknown after {attempts} attempts)")]
    AppendUnknown { attempts: u32, last: Attempt },
    #[error("put {url}: {last}")]
    PutUnknown { url: String, last: Attempt },
    #[error(
        "{op} {url}: {status}{}: the endpoint refused this client's credentials (see \
         URSULA_VFS_TOKEN_FILE and ursula_set_token)",
        with_body(.body)
    )]
    Unauthorized {
        op: &'static str,
        url: String,
        status: u16,
        /// The refusing layer's explanation (`client::body_text`), empty when it sent none.
        body: String,
    },
    /// Refused before it reached the stream, or never sent, so that the stream applied nothing of
    /// it (see [`WriteRefusal`]).
    #[error("{op} {url}: {refusal}: {}, and nothing of it reached the stream", .refusal.how())]
    Refused {
        op: &'static str,
        url: String,
        refusal: WriteRefusal,
    },
    #[error("{url} reports no Stream-Incarnation (an older server?); refusing to attach")]
    NoIncarnation { url: String },
    #[error("{op} {url}: no Stream-Next-Offset")]
    NoNextOffset { op: &'static str, url: String },
    #[error("read {url} at {at}: {len} bytes but next offset {next}")]
    OffsetNotAdvanced {
        url: String,
        at: String,
        len: usize,
        next: String,
    },
    #[error("stream {url} ends at {at} inside a frame or before {until:?}")]
    StreamEnded {
        url: String,
        at: String,
        until: Option<String>,
    },
    #[error("claim {url}: the stream ends at {at}, before the claim's end {next}")]
    ClaimCut {
        url: String,
        at: String,
        next: String,
    },
    #[error("claim {url}: {source}")]
    ClaimScan {
        url: String,
        #[source]
        source: ClaimScanError,
    },
    #[error("claim {url}: lost 16 claim races")]
    ClaimRaces { url: String },
    #[error("claim {url}: answered as a commit append (expired producer or Stream-Seq conflict)")]
    ClaimAnswer { url: String },
    #[error("{url}: a frame after {after}: {source}")]
    Frame {
        url: String,
        after: String,
        #[source]
        source: FrameError,
    },
    #[error("encode a frame: {0}")]
    EncodeFrame(#[source] FrameError),
    #[error("snapshot at {offset}: {source}")]
    Snapshot {
        offset: String,
        #[source]
        source: SnapshotError,
    },
    #[error("snapshot at {at} reflects offset {reflects}")]
    SnapshotOffset { at: String, reflects: String },
    #[error("the snapshot at {offset} does not read back")]
    SnapshotNotReadBack { offset: String },
    /// A snapshot publish answered 413: its body is over the request size limit of the server or
    /// of a layer in front of it.
    #[error(
        "publish snapshot {url}: 413{}: a body of {bytes} bytes is over the request size limit of \
         the server or of a layer in front of it (ursulagw: --max-request-body-bytes)",
        with_body(.body)
    )]
    SnapshotTooLarge {
        url: String,
        bytes: usize,
        body: String,
    },
    /// A snapshot publish answered 408: the server or a layer in front of it did not read the
    /// body in time, which a body too large for a slow layer can cause
    /// (`snapshotter::FailureKind`).
    #[error(
        "publish snapshot {url}: 408{}: a body of {bytes} bytes was not read in time by the server \
         or a layer in front of it",
        with_body(.body)
    )]
    SnapshotTimedOut {
        url: String,
        bytes: usize,
        body: String,
    },
    #[error(
        "read {url} at {offset}: beyond the end of the stream that acknowledged it (the server \
         lost acknowledged data?); refusing to attach, and keeping the local files, which may hold \
         the only copy of the lost commits (to salvage them, copy the db file and its -wal to a new \
         name and export the copy's rows into a newly attached database; to rebuild from the \
         stream as it is, move the db file and its -wal aside first)"
    )]
    LostData { url: String, offset: String },
    #[error("gone: {0}")]
    Gone(Gone),
    #[error("fenced: {url} was deleted and recreated (no longer incarnation {incarnation}, 412)")]
    Recreated { url: String, incarnation: String },
    #[error("fenced: {0}")]
    Fenced(Fence),

    // The local files no longer follow the stream: the database is poisoned until re-attached.
    #[error("db file write outside a checkpoint (journal_mode must stay WAL)")]
    DbWriteOutsideCheckpoint,
    #[error("db file truncate outside a checkpoint (journal_mode must stay WAL)")]
    DbTruncateOutsideCheckpoint,
    #[error(
        "WAL write outside a write-locked transaction (locking_mode=EXCLUSIVE is not supported)"
    )]
    WalWriteOutsideTransaction,
    #[error("page size {0} (only {PAGE} is supported)")]
    PageSize(u32),
    #[error("a commit leaves WAL format (journal_mode must stay WAL)")]
    LeavesWal,
    #[error("fsync of the db file before {before} failed ({code})")]
    Fsync { before: &'static str, code: c_int },
    #[error("{what} failed ({code}) after the commit was acknowledged")]
    PostAck { what: &'static str, code: c_int },
    #[error("commit acknowledged but not published locally (mxFrame {mx_frame} < frame {frame})")]
    NotPublished { mx_frame: u32, frame: u32 },
    #[error("append after {offset} acknowledged with next offset {next}, not past it")]
    AckNotPast { offset: String, next: String },
    /// The commit after one refused as never applied (`Db::refuse`), sent at the sequence the
    /// refusal left free, was answered as a duplicate that none of its own attempts explains
    /// (`client::Append::UnexplainedDuplicate`). The stream holds a commit at that sequence that
    /// the local files lack, and the answer does not tell which: the refused commit, applied after
    /// all (a layer in front of the stream broke the rule of design doc §2), or this commit, sent
    /// again by a layer in front of the stream (a gateway that followed a leader redirect sent
    /// after the leader proposed it). `receipt` is that commit's end in the stream.
    #[error(
        "a commit refused as never applied may have been applied after all: the next commit, at \
         epoch {epoch} seq {seq} after {offset}, was answered as a duplicate (receipt {receipt}), \
         so the stream holds either the refused commit (a layer in front of the stream refused a \
         write that the stream applied) or this one (a layer in front of the stream sent it again, \
         such as a gateway that followed a leader redirect), and the local file holds neither: \
         attach the database again (the attach replays whichever the stream holds) and check \
         which one it holds before running either transaction again"
    )]
    RefusedCommitApplied {
        epoch: u64,
        seq: u64,
        offset: String,
        receipt: String,
    },
    #[error("producer expired again right after a re-claim")]
    ProducerExpiredAgain,
    #[error("local WAL write: {0}")]
    LocalWalWrite(c_int),
    #[error(
        "the WAL page at offset {offset} reads back other bytes than this process wrote there (a \
         write-back failed, or something else wrote the file)"
    )]
    LostWrite { offset: i64 },
    #[error("fsync of the {file} failed ({code}): a write-back to it may have been lost")]
    FileSync { file: &'static str, code: c_int },
}

/// What kind of failure an error is, as the SQL functions' messages (`<function>: <kind>: ...`)
/// and `ursula_status` (`error.kind`) name it: a stable token callers branch on instead of the
/// message's text (design doc §1, errors). [`Error::kind`] maps every error, [`Error::poison_kind`]
/// a poison's reason, [`WriteRefusal::error_kind`] a refused write, and each kind has one
/// [`Action`]. `attach_failed` has none of its own: what to do is what the failure it wraps (the
/// kind after it in the message) says.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ErrorKind {
    /// An argument of a SQL function is unusable (an attach mode, a path with a NUL byte).
    InvalidArgument,
    /// The stream URL is unusable as given: it does not parse, names no usable scheme, or answers
    /// with a redirect the extension does not follow.
    InvalidUrl,
    /// The server refused the stream URL's bucket or stream id (a `400` to its `HEAD` or create:
    /// the message carries the server's explanation).
    InvalidName,
    /// The stream does not exist (or is hidden from these credentials).
    NotFound,
    /// The file is in use: connections of the VFS open, another thread or process attaching or
    /// holding it, or a descriptor of this process outside the extension on the db file, its -wal,
    /// -shm or -journal (a SQLite connection outside the extension, read-only ones included).
    InUse,
    /// The db file holds data but has no readable sidecar: it was never replicated, or its sidecar
    /// was lost, so its pages may be in no stream (`Error::NoSidecar`). Attach keeps the files and
    /// never replays a stream over them.
    UnreplicatedFile,
    /// The local files are the cache of another stream (`Error::OtherStream`).
    OtherStream,
    /// A commit's frame is larger than one request may be: the same transaction can never be
    /// sent (`WriteRefusal::TooLarge`).
    TooLarge,
    /// The application did something the VFS does not support (left WAL mode, another page size,
    /// `locking_mode=EXCLUSIVE`).
    Unsupported,
    /// The stream holds or answers something the extension cannot follow (an undecodable frame,
    /// lost acknowledged data, no incarnation, an answer without its offset).
    StreamState,
    /// The file was never attached in this process.
    NotAttached,
    /// No answer from the endpoint: it did not resolve, refused or dropped the connection, or
    /// timed out, or every attempt of a write failed before it went out.
    Unreachable,
    /// The endpoint answered, but put the request off (a 5xx, a `408`, a `429` or a `307`) until
    /// the retry budget ran out.
    Unavailable,
    /// The endpoint refused this client's credentials (a `401`, or a `403` that is no fence) on a
    /// request that applies nothing.
    Unauthorized,
    /// A write was refused before it reached the stream, which applied nothing of it
    /// (`refused_kind` says by what).
    Refused,
    /// An append's outcome is unknown: no answer within the retry budget, or an answer that does
    /// not prove that nothing was applied (a 5xx without a vouched marker, a `307`, a `403` that is
    /// neither the fence nor vouched, a `401` after an attempt that may have reached the stream).
    /// The stream may or may not hold it.
    OutcomeUnknown,
    /// The local files may hold pages this process did not write there (a lost write-back, a failed
    /// fsync): nothing read from them may reach the stream.
    LocalDamage,
    /// A local operation failed (an I/O error on the local files).
    LocalState,
    /// The path's last attach in this process failed: it opens again once an attach succeeds.
    AttachFailed,
    /// A failure inside the extension or the host's SQLite (a panic, a missing routine).
    Internal,
    /// Another owner claimed the stream, it was deleted and recreated, or a writer outside the
    /// protocol appended.
    Fenced,
}

/// What a caller does about an error of a kind ([`ErrorKind::action`]), as `ursula_status` reports
/// it for a poison (`db::Poisoned::action`) or a refused write ([`WriteRefusal::action`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    /// Nothing reached the stream: the same handle works again later (commit, or attach, again
    /// once the cause is gone). A refused commit leaves the file usable.
    Retry,
    /// Close the file's connections and attach it again, which replays what the stream holds (and
    /// rebuilds local files that cannot be trusted).
    Reattach,
    /// Another owner holds the stream: stop writing. Attaching again takes it back from that
    /// owner, which only a deliberate takeover does.
    Stop,
    /// The request cannot succeed as made: fix the URL, the name, the setup, the files or the
    /// transaction, then try again.
    Fix,
}

impl Action {
    /// The stable token.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Action::Retry => "retry",
            Action::Reattach => "reattach",
            Action::Stop => "stop",
            Action::Fix => "fix",
        }
    }
}

impl ErrorKind {
    /// The stable token.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            ErrorKind::InvalidArgument => "invalid_argument",
            ErrorKind::InvalidUrl => "invalid_url",
            ErrorKind::InvalidName => "invalid_name",
            ErrorKind::NotFound => "not_found",
            ErrorKind::InUse => "in_use",
            ErrorKind::UnreplicatedFile => "unreplicated_file",
            ErrorKind::OtherStream => "other_stream",
            ErrorKind::TooLarge => "too_large",
            ErrorKind::Unsupported => "unsupported",
            ErrorKind::StreamState => "stream_state",
            ErrorKind::NotAttached => "not_attached",
            ErrorKind::Unreachable => "unreachable",
            ErrorKind::Unavailable => "unavailable",
            ErrorKind::Unauthorized => "unauthorized",
            ErrorKind::Refused => "refused",
            ErrorKind::OutcomeUnknown => "outcome_unknown",
            ErrorKind::LocalDamage => "local_damage",
            ErrorKind::LocalState => "local_state",
            ErrorKind::AttachFailed => "attach_failed",
            ErrorKind::Internal => "internal",
            ErrorKind::Fenced => "fenced",
        }
    }

    /// What a caller does about an error of this kind. `attach_failed` has none of its own (`None`):
    /// what to do is what the failure it wraps says.
    pub(crate) fn action(self) -> Option<Action> {
        Some(match self {
            ErrorKind::InvalidArgument
            | ErrorKind::InvalidUrl
            | ErrorKind::InvalidName
            | ErrorKind::NotFound
            | ErrorKind::InUse
            | ErrorKind::UnreplicatedFile
            | ErrorKind::OtherStream
            | ErrorKind::TooLarge
            | ErrorKind::Unsupported
            | ErrorKind::StreamState
            | ErrorKind::NotAttached => Action::Fix,
            ErrorKind::Unreachable
            | ErrorKind::Unavailable
            | ErrorKind::Unauthorized
            | ErrorKind::Refused => Action::Retry,
            ErrorKind::OutcomeUnknown
            | ErrorKind::LocalDamage
            | ErrorKind::LocalState
            | ErrorKind::Internal => Action::Reattach,
            ErrorKind::Fenced => Action::Stop,
            ErrorKind::AttachFailed => return None,
        })
    }
}

/// The data asked for lies below the stream's retention (or a snapshot was superseded): a
/// re-attach installs the newer snapshot.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Gone {
    #[error("read {url} at {offset}: below the stream's retention")]
    BelowRetention { url: String, offset: String },
    #[error("snapshot {offset} superseded")]
    SnapshotSuperseded { offset: String },
    #[error("{offset} is below the retention {retained} and no newer snapshot is visible")]
    RetentionPassed { offset: String, retained: String },
}

/// Another owner, or a writer outside this protocol, holds the stream.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Fence {
    #[error("epoch {epoch} superseded by {} (403)", epoch_or_unknown(.current))]
    Superseded { epoch: u64, current: Option<u64> },
    #[error(
        "append at {offset} answered as a duplicate without a receipt: another writer (a foreign \
         one using this Producer-Id?) holds producer {producer} at epoch {epoch} past seq {seq}"
    )]
    DuplicateWithoutReceipt {
        offset: String,
        producer: String,
        epoch: u64,
        seq: u64,
    },
    #[error(
        "append after {offset} refused its Stream-Seq {stream_seq}: another writer appended with \
         a higher one ({body})"
    )]
    SeqConflict {
        offset: String,
        stream_seq: String,
        body: String,
    },
    #[error("producer expired and the stream moved past {offset}")]
    ProducerExpiredMoved { offset: String },
    #[error("another owner wrote or claimed while re-claiming")]
    Reclaim,
    #[error("another owner claimed epoch {epoch} during attach")]
    ClaimedDuringAttach { epoch: u64 },
}

/// The frames read back after a claim do not show where it landed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ClaimScanError {
    #[error(transparent)]
    Frame(#[from] FrameError),
    #[error("the answered offset is not a frame boundary")]
    NotAtBoundary,
}

/// The last answer to a request retried while its outcome was unknown.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Attempt {
    #[error("{status}{}", with_body(.body))]
    Status { status: u16, body: String },
    /// A 5xx by which the server, or a layer in front of it, put the request off: it asked for a
    /// later attempt (`Retry-After`, as `retry_after`) or marked that it applied nothing
    /// (`x-ursula-not-applied`, vouched for or not), the way the server's ingress admission
    /// refuses a write under pressure. Only `client::put_idempotent` tells it apart: such a
    /// refusal is not about the body's size, which is refused with a 413.
    #[error("{status}{}", with_body(.body))]
    Deferred {
        status: u16,
        body: String,
        retry_after: Option<Duration>,
    },
    #[error("{0}")]
    Transport(#[source] Box<ureq::Error>),
}

/// Why a write was refused before it reached the stream, so that the stream applied nothing of it
/// (design doc §2): the write fails alone and never poisons a database (`Db::refuse`). A body is
/// the refusing layer's explanation: its response body (cut short: `client::body_text`), or the
/// token of a vouched marker when the body is empty (`client::Proof::refusal`).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum WriteRefusal {
    /// 402 Payment Required: a layer in front of the stream (an authorizer, a quota or billing
    /// service) refuses writes until something is paid or settled.
    #[error("402 Payment Required{}", with_body(.body))]
    PaymentRequired { body: String },
    /// 401 until the retry budget ran out: an authorizer refused the token, read again before
    /// every attempt.
    #[error(
        "401{}: the endpoint refused this client's credentials until the retry budget ran out \
         (see URSULA_VFS_TOKEN_FILE and ursula_set_token)",
        with_body(.body)
    )]
    Unauthorized { body: String },
    /// 403 without `Producer-Epoch`, marked `x-ursula-not-applied` with a gateway's vouch: an
    /// authorizer denied the write before passing it on (the server's fence names the epoch that
    /// owns the stream, and any other 403 may have replaced the stream's own answer:
    /// `client::append`).
    #[error("403{}: an authorizer denied it", with_body(.body))]
    Forbidden { body: String },
    /// 429 until the retry budget ran out, marked or not.
    #[error("429{}: rate limited until the retry budget ran out", with_body(.body))]
    RateLimited { body: String },
    /// A refusal marked `x-ursula-not-applied` (the server refusing it at its ingress admission or
    /// before it proposed it, a gateway or a proxy refusing it before passing it on) and vouched
    /// for by a gateway (`x-ursula-not-applied-checked`, see `client::applied_nothing`): a final
    /// one, or the last one when the retry budget ran out. A 401, 402, 403 or 429 keeps its own
    /// kind. `reason` is the marker's token (`RaftLogPressure`, say), empty when it has none.
    #[error("{status} (not applied{})", with_token(.reason))]
    NotApplied { status: u16, reason: String },
    /// The commit's frame, `bytes` long, is larger than one request may be (`over`). The same
    /// transaction can never be sent: split it.
    #[error("{}", too_large(*.bytes, .over))]
    TooLarge { bytes: usize, over: FrameLimit },
    /// Every attempt failed before any byte of it went out, or was refused, until the retry budget
    /// ran out, and the last one failed so (`net::NotSent`): its host did not resolve, or no
    /// connection to it was made, the TLS handshake included. No layer refused it: nothing reached
    /// one. `reason` is the last attempt's failure.
    #[error("{reason}")]
    Unreachable { reason: String },
}

/// The limit a commit frame too large for one request is over ([`WriteRefusal::TooLarge`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FrameLimit {
    /// The extension's frame limit (`URSULA_VFS_MAX_FRAME_BYTES`), or what a frame can hold at
    /// all: the frame was never sent ([`WriteRefusal::frame_over_limit`]).
    Extension { limit: usize },
    /// A lower request limit in front of the stream, which refused the frame with a 413 that
    /// proves it applied nothing (marked `x-ursula-not-applied` and vouched for, see
    /// `client::applied_nothing`). `token` is the marker's.
    InFront { token: String },
}

impl WriteRefusal {
    /// A commit frame of `bytes` that the extension never sends: over `limit`, its frame limit
    /// (`URSULA_VFS_MAX_FRAME_BYTES`), or over what a frame can hold at all. Its reason, which
    /// `ursula_status` reports, gives both numbers.
    pub(crate) fn frame_over_limit(bytes: usize, limit: usize) -> WriteRefusal {
        WriteRefusal::TooLarge {
            bytes,
            over: FrameLimit::Extension { limit },
        }
    }

    /// Whether the layer that refused this write refuses other writes too, a snapshot publish among
    /// them: `Db::writes_refused` records it as `layer_refused` then, which holds snapshots back. A
    /// refused token, a denial, rate limiting, a payment or write pressure do. A frame too large
    /// for one request says nothing about any other write.
    pub(crate) fn refuses_other_writes(&self) -> bool {
        !matches!(
            self,
            WriteRefusal::TooLarge { .. } | WriteRefusal::Unreachable { .. }
        )
    }

    /// How the write came to apply nothing, for an error's message: refused by a layer in front of
    /// the stream, or its last attempt never sent (earlier ones may have been refused).
    pub(crate) fn how(&self) -> &'static str {
        match self {
            WriteRefusal::Unreachable { .. } => "its last attempt failed before it went out",
            _ => "a layer in front of the stream refused it",
        }
    }

    /// The kind of failure a refused write is ([`ErrorKind`]): `too_large` for a frame no request
    /// can carry (the same transaction never goes through), `unreachable` when its last attempt
    /// never went out, and `refused` for a layer's refusal. Used for a refused attach
    /// ([`Error::kind`]) and a refused commit (`ursula_status`) alike.
    pub(crate) fn error_kind(&self) -> ErrorKind {
        match self {
            WriteRefusal::TooLarge { .. } => ErrorKind::TooLarge,
            WriteRefusal::Unreachable { .. } => ErrorKind::Unreachable,
            WriteRefusal::PaymentRequired { .. }
            | WriteRefusal::Unauthorized { .. }
            | WriteRefusal::Forbidden { .. }
            | WriteRefusal::RateLimited { .. }
            | WriteRefusal::NotApplied { .. } => ErrorKind::Refused,
        }
    }

    /// What a caller does about a refused write: its kind's action.
    pub(crate) fn action(&self) -> Action {
        self.error_kind().action().unwrap_or(Action::Retry)
    }

    /// What refused the write, as `ursula_status` reports it (`refused_kind`).
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            WriteRefusal::PaymentRequired { .. } => "payment_required",
            WriteRefusal::Unauthorized { .. } => "unauthorized",
            WriteRefusal::Forbidden { .. } => "forbidden",
            WriteRefusal::RateLimited { .. } => "rate_limited",
            WriteRefusal::NotApplied { .. } => "not_applied",
            WriteRefusal::TooLarge { .. } => "too_large",
            WriteRefusal::Unreachable { .. } => "unreachable",
        }
    }

    /// Why, as `ursula_status` reports it (`refused_reason`): the layer's explanation (its body, or
    /// the marker's token when the body is empty), the marker's token for `NotApplied` and for a
    /// 413 in front, trimmed, `None` when the layer sent neither, and for a frame over the
    /// extension's limit its size and the limit.
    pub(crate) fn reason(&self) -> Option<Cow<'_, str>> {
        let reason = match self {
            WriteRefusal::PaymentRequired { body }
            | WriteRefusal::Unauthorized { body }
            | WriteRefusal::Forbidden { body }
            | WriteRefusal::RateLimited { body } => body,
            WriteRefusal::NotApplied { reason, .. } | WriteRefusal::Unreachable { reason } => {
                reason
            }
            WriteRefusal::TooLarge {
                over: FrameLimit::InFront { token },
                ..
            } => token,
            WriteRefusal::TooLarge {
                bytes,
                over: FrameLimit::Extension { limit },
            } => {
                return Some(Cow::Owned(format!(
                    "{bytes}-byte frame over the {limit}-byte frame limit"
                )));
            }
        };
        Some(reason.trim())
            .filter(|reason| !reason.is_empty())
            .map(Cow::Borrowed)
    }
}

impl Error {
    /// How long the server, or a layer in front of it, asked to wait before another attempt: the
    /// `Retry-After` of the answer that put off the last attempt of a `PUT` (`Attempt::Deferred`).
    pub(crate) fn retry_after(&self) -> Option<Duration> {
        match self {
            Error::PutUnknown {
                last: Attempt::Deferred { retry_after, .. },
                ..
            } => *retry_after,
            _ => None,
        }
    }

    /// The data lies below the stream's retention, or a snapshot was superseded: retry from the
    /// stream's latest snapshot.
    pub(crate) fn is_gone(&self) -> bool {
        matches!(self, Error::Gone(_))
    }

    /// The stream at the path is no longer the incarnation the request was sent to: it was deleted
    /// and recreated, and nothing of the request reached it.
    pub(crate) fn is_recreated(&self) -> bool {
        matches!(self, Error::Recreated { .. })
    }

    /// Another owner, a writer outside this protocol, or another incarnation of the stream holds
    /// it: this owner cannot commit again without a new attach.
    pub(crate) fn is_fenced(&self) -> bool {
        matches!(self, Error::Fenced(_) | Error::Recreated { .. })
    }

    /// Why the request was refused before it reached the stream, which applied nothing of it: the
    /// operation fails alone, and never poisons a database (see `Db::refuse`). `None` for any
    /// other error.
    pub(crate) fn refusal(&self) -> Option<&WriteRefusal> {
        match self {
            Error::Refused { refusal, .. } => Some(refusal),
            _ => None,
        }
    }

    /// The local files may hold other pages than this process wrote (a write-back was lost, or
    /// something else wrote them): nothing read from them may reach the stream, not even in a
    /// snapshot.
    pub(crate) fn is_local_damage(&self) -> bool {
        matches!(
            self,
            Error::Fsync { .. } | Error::LostWrite { .. } | Error::FileSync { .. }
        )
    }

    /// The kind of this error as the reason a database is poisoned (`Db::set_poisoned`), the one
    /// `ursula_status` reports (`error.kind`) unless a fence was seen since (then `fenced`). A
    /// poison is never retried on the same handle, so a reason whose kind would be (a re-claim's
    /// read that the endpoint put off or never answered) is an unknown outcome here: the commit
    /// failed, and only an attach tells what the stream holds.
    pub(crate) fn poison_kind(&self) -> ErrorKind {
        match self.kind() {
            kind if kind.action() == Some(Action::Retry) => ErrorKind::OutcomeUnknown,
            kind => kind,
        }
    }

    /// What kind of failure this is ([`ErrorKind`]).
    pub(crate) fn kind(&self) -> ErrorKind {
        if self.is_fenced() {
            return ErrorKind::Fenced;
        }
        match self {
            Error::AttachMode { .. } | Error::NulInPath { .. } => ErrorKind::InvalidArgument,
            Error::Redirected { .. } => ErrorKind::InvalidUrl,
            Error::Http { source, .. } => transport_kind(source),
            Error::Status { op, status, .. } => status_kind(op, *status),
            // An append's (a claim's) credential refusal is built only when it does not prove that
            // nothing was applied (`client::append`): its outcome is unknown.
            Error::Unauthorized { op: "append", .. } => ErrorKind::OutcomeUnknown,
            Error::Unauthorized { .. } => ErrorKind::Unauthorized,
            Error::Refused { refusal, .. } => refusal.error_kind(),
            Error::PutUnknown { last, .. } => match last {
                Attempt::Transport(source) => transport_kind(source),
                Attempt::Status { .. } | Attempt::Deferred { .. } => ErrorKind::Unavailable,
            },
            Error::Gone(_) | Error::ClaimRaces { .. } => ErrorKind::Unavailable,
            Error::AppendUnknown { .. }
            | Error::AppendRejected { .. }
            | Error::AckNotPast { .. }
            | Error::RefusedCommitApplied { .. }
            | Error::ProducerExpiredAgain => ErrorKind::OutcomeUnknown,
            Error::NoStream { .. } | Error::StreamMissing { .. } => ErrorKind::NotFound,
            Error::OpenElsewhere { .. }
            | Error::AttachedElsewhere { .. }
            | Error::OpenConnections { .. }
            | Error::OpenOutsideVfs { .. }
            | Error::AttachInProgress { .. } => ErrorKind::InUse,
            Error::NotAttached {
                last_failure: None, ..
            } => ErrorKind::NotAttached,
            Error::NotAttached { .. } => ErrorKind::AttachFailed,
            Error::NoSidecar { .. } => ErrorKind::UnreplicatedFile,
            Error::OtherStream { .. } => ErrorKind::OtherStream,
            Error::Io { .. }
            | Error::FullPathname { .. }
            | Error::PostAck { .. }
            | Error::NotPublished { .. }
            | Error::LocalWalWrite(_) => ErrorKind::LocalState,
            Error::Fsync { .. } | Error::LostWrite { .. } | Error::FileSync { .. } => {
                ErrorKind::LocalDamage
            }
            Error::DbWriteOutsideCheckpoint
            | Error::DbTruncateOutsideCheckpoint
            | Error::WalWriteOutsideTransaction
            | Error::PageSize(_)
            | Error::LeavesWal => ErrorKind::Unsupported,
            Error::NoIncarnation { .. }
            | Error::NoNextOffset { .. }
            | Error::OffsetNotAdvanced { .. }
            | Error::StreamEnded { .. }
            | Error::ClaimCut { .. }
            | Error::ClaimScan { .. }
            | Error::ClaimAnswer { .. }
            | Error::Frame { .. }
            | Error::Snapshot { .. }
            | Error::SnapshotOffset { .. }
            | Error::SnapshotNotReadBack { .. }
            | Error::SnapshotTooLarge { .. }
            | Error::SnapshotTimedOut { .. }
            | Error::LostData { .. } => ErrorKind::StreamState,
            // Taken above (`is_fenced`).
            Error::Recreated { .. } | Error::Fenced(_) => ErrorKind::Fenced,
            Error::Panicked { .. }
            | Error::SpawnSnapshotThread(_)
            | Error::SqliteOpen { .. }
            | Error::SqliteQuery { .. }
            | Error::NoResultRow { .. }
            | Error::PersistWal(_)
            | Error::FileHandle { .. }
            | Error::ReadPages { .. }
            | Error::MissingRoutine(_)
            | Error::EncodeFrame(_) => ErrorKind::Internal,
        }
    }
}

/// The kind of a request that got no answer: a URL that cannot be sent at all is the caller's to
/// fix, anything else is the endpoint's (or the network's).
fn transport_kind(e: &ureq::Error) -> ErrorKind {
    match e {
        ureq::Error::BadUri(_)
        | ureq::Error::Http(_)
        | ureq::Error::RequireHttpsOnly(_)
        | ureq::Error::InvalidProxyUrl => ErrorKind::InvalidUrl,
        _ => ErrorKind::Unreachable,
    }
}

/// The kind of a final answer `status` to request `op` that its error names no other way. A `400`
/// to the stream's `HEAD` or create refuses its URL's bucket or stream id (any later request
/// carries ids the server took already).
fn status_kind(op: &str, status: u16) -> ErrorKind {
    match status {
        400 if matches!(op, "head" | "create") => ErrorKind::InvalidName,
        401 | 403 => ErrorKind::Unauthorized,
        404 => ErrorKind::NotFound,
        307 | 408 | 429 | 500..=599 => ErrorKind::Unavailable,
        _ => ErrorKind::StreamState,
    }
}

/// An epoch a server answer named, or `unknown`.
fn epoch_or_unknown(epoch: &Option<u64>) -> String {
    epoch.map_or_else(|| "unknown".to_owned(), |epoch| epoch.to_string())
}

/// A response body appended to a status, when there is one.
fn with_body(body: &str) -> String {
    let body = body.trim();
    if body.is_empty() {
        String::new()
    } else {
        format!(" {body}")
    }
}

/// [`WriteRefusal::TooLarge`]'s message: the frame's size and the limit it is over, with the
/// marker's token for a 413 from a layer in front.
fn too_large(bytes: usize, over: &FrameLimit) -> String {
    match over {
        FrameLimit::Extension { limit } => format!(
            "{bytes}-byte commit frame over the {limit}-byte frame limit \
             (URSULA_VFS_MAX_FRAME_BYTES): split the transaction"
        ),
        FrameLimit::InFront { token } => format!(
            "{bytes}-byte commit frame over a request body limit in front of the stream \
             (413{}): split the transaction",
            with_token(token)
        ),
    }
}

/// `: token` for a marker's token, nothing for an empty one.
fn with_token(token: &str) -> String {
    let token = token.trim();
    if token.is_empty() {
        String::new()
    } else {
        format!(": {token}")
    }
}

#[cfg(test)]
mod tests {
    use super::Action;
    use super::Attempt;
    use super::Error;
    use super::ErrorKind;
    use super::Fence;
    use super::FrameLimit;
    use super::Gone;
    use super::WriteRefusal;
    use crate::descriptors::DbFile;

    // Callers act on the class, never on the text: attach retries `Gone`, rebuilds on a recreated
    // stream, and `ursula_status` reports `fenced` for the fence classes only. The texts below are
    // what operators and the e2e suites look for.
    #[test]
    fn classes_and_texts() {
        let recreated = Error::Recreated {
            url: "http://h/b/s".into(),
            incarnation: "i1".into(),
        };
        assert!(recreated.is_recreated() && recreated.is_fenced() && !recreated.is_gone());
        assert!(recreated.to_string().contains("deleted and recreated"));
        let too_large = Error::SnapshotTooLarge {
            url: "http://h/b/s/snapshot/7".into(),
            bytes: 41_943_040,
            body: "snapshot body is too large".into(),
        };
        assert_eq!(
            too_large.to_string(),
            "publish snapshot http://h/b/s/snapshot/7: 413 snapshot body is too large: a body of \
             41943040 bytes is over the request size limit of the server or of a layer in front of \
             it (ursulagw: --max-request-body-bytes)"
        );
        let timed_out = Error::SnapshotTimedOut {
            url: "http://h/b/s/snapshot/7".into(),
            bytes: 41_943_040,
            body: String::new(),
        };
        assert_eq!(
            timed_out.to_string(),
            "publish snapshot http://h/b/s/snapshot/7: 408: a body of 41943040 bytes was not read in \
             time by the server or a layer in front of it"
        );
        let superseded = Error::Fenced(Fence::Superseded {
            epoch: 2,
            current: Some(3),
        });
        assert!(superseded.is_fenced() && !superseded.is_recreated());
        assert!(superseded.to_string().ends_with("superseded by 3 (403)"));
        let unknown = Error::Fenced(Fence::Superseded {
            epoch: 2,
            current: None,
        });
        assert!(unknown.to_string().ends_with("superseded by unknown (403)"));
        let gone = Error::Gone(Gone::SnapshotSuperseded { offset: "7".into() });
        assert!(gone.is_gone() && !gone.is_fenced() && !gone.is_recreated());
        let local = Error::LocalWalWrite(10);
        assert!(!local.is_fenced() && !local.is_gone() && !local.is_recreated());
        // A lost write-back (found on a read, or reported by an fsync) keeps every page of the
        // local files out of a snapshot; a fence does not.
        let lost_page = Error::LostWrite { offset: 56 };
        let lost_sync = Error::FileSync {
            file: "WAL",
            code: 10,
        };
        assert!(lost_page.is_local_damage() && lost_sync.is_local_damage());
        assert!(!lost_page.is_fenced() && !superseded.is_local_damage());
        assert!(!local.is_local_damage());
        let missing = Error::StreamMissing {
            path: "/data/app.db".into(),
            url: "http://h/b/s".into(),
        }
        .to_string();
        assert!(missing.starts_with("http://h/b/s is missing") && missing.contains("holds data"));
        assert!(missing.contains("attach a new file under another URL"));
        let lost = Error::LostData {
            url: "http://h/b/s".into(),
            offset: "9".into(),
        };
        assert!(!lost.is_gone() && lost.to_string().contains("lost acknowledged data"));
        // A refusal in front of the stream is its own class: never a fence, never gone, never
        // damage. The 402's text is the one 0.7.1 introduced.
        let payment = WriteRefusal::PaymentRequired {
            body: "{\"detail\":\"quota exhausted\"}".into(),
        };
        let refused = Error::Refused {
            op: "claim",
            url: "http://h/b/s".into(),
            refusal: payment.clone(),
        };
        assert_eq!(refused.refusal(), Some(&payment));
        assert!(!refused.is_fenced() && !refused.is_gone() && !refused.is_local_damage());
        assert_eq!(
            refused.to_string(),
            "claim http://h/b/s: 402 Payment Required {\"detail\":\"quota exhausted\"}: a layer in front \
             of the stream refused it, and nothing of it reached the stream"
        );
        assert_eq!(superseded.refusal(), None);
        assert_eq!(local.refusal(), None);
        let unauthorized = Error::Unauthorized {
            op: "head",
            url: "http://h/b/s".into(),
            status: 401,
            body: String::new(),
        };
        assert_eq!(unauthorized.refusal(), None);
        let pressure = Error::Refused {
            op: "claim",
            url: "http://h/b/s".into(),
            refusal: WriteRefusal::NotApplied {
                status: 503,
                reason: "RaftLogPressure".into(),
            },
        };
        assert_eq!(
            pressure.to_string(),
            "claim http://h/b/s: 503 (not applied: RaftLogPressure): a layer in front of the \
             stream refused it, and nothing of it reached the stream"
        );
        // The commit after a refused one, answered as a duplicate, poisons: neither a fence nor
        // damage nor a refusal. The text names both commits the stream may hold.
        let applied = Error::RefusedCommitApplied {
            epoch: 2,
            seq: 5,
            offset: "9".into(),
            receipt: "12".into(),
        };
        assert!(!applied.is_fenced() && !applied.is_local_damage() && applied.refusal().is_none());
        assert_eq!(
            applied.to_string(),
            "a commit refused as never applied may have been applied after all: the next commit, \
             at epoch 2 seq 5 after 9, was answered as a duplicate (receipt 12), so the stream \
             holds either the refused commit (a layer in front of the stream refused a write that \
             the stream applied) or this one (a layer in front of the stream sent it again, such as \
             a gateway that followed a leader redirect), and the local file holds neither: attach \
             the database again (the attach replays whichever the stream holds) and check which \
             one it holds before running either transaction again"
        );
        // A redirect the extension does not retry fails the request: never a fence, never gone.
        let redirected = Error::Redirected {
            op: "head",
            url: "http://h/b/s".into(),
            status: 301,
            location: Some("https://h/b/s".into()),
        };
        assert!(!redirected.is_fenced() && !redirected.is_gone());
        assert!(redirected.refusal().is_none() && !redirected.is_recreated());
        assert_eq!(
            redirected.to_string(),
            "head http://h/b/s: 301 redirect to https://h/b/s: the extension never follows \
             redirects (point the stream URL at the server or gateway itself)"
        );
    }

    // Every error has a stable kind, and every kind an action. A refusal is retried, an unknown
    // outcome attached again, a fence stops; a URL or name the server refuses, a file in use, a
    // file never replicated or a transaction too large needs a fix. The SQL functions show the
    // kind after their name, and a path whose last attach failed shows that failure's kind next.
    #[test]
    fn errors_have_stable_kinds() {
        let url = || "http://h/b/s".to_owned();
        let status = |op, status| Error::Status {
            op,
            url: url(),
            status,
            body: String::new(),
        };
        let refused = |refusal| Error::Refused {
            op: "claim",
            url: url(),
            refusal,
        };
        let denied = |op| Error::Unauthorized {
            op,
            url: url(),
            status: 403,
            body: String::new(),
        };
        let cases = [
            (status("head", 400), "invalid_name", Some("fix")),
            (status("create", 400), "invalid_name", Some("fix")),
            (status("read", 400), "stream_state", Some("fix")),
            (status("head", 404), "not_found", Some("fix")),
            (status("head", 503), "unavailable", Some("retry")),
            (
                Error::Http {
                    op: "head",
                    url: url(),
                    source: Box::new(ureq::Error::ConnectionFailed),
                },
                "unreachable",
                Some("retry"),
            ),
            (
                Error::Http {
                    op: "head",
                    url: url(),
                    source: Box::new(ureq::Error::BadUri("no host".into())),
                },
                "invalid_url",
                Some("fix"),
            ),
            (denied("head"), "unauthorized", Some("retry")),
            // An append's (a claim's) denial does not prove that nothing was applied.
            (denied("append"), "outcome_unknown", Some("reattach")),
            (
                refused(WriteRefusal::RateLimited {
                    body: String::new(),
                }),
                "refused",
                Some("retry"),
            ),
            (
                refused(WriteRefusal::Unreachable {
                    reason: "timeout: resolve".into(),
                }),
                "unreachable",
                Some("retry"),
            ),
            (
                refused(WriteRefusal::frame_over_limit(40 << 20, 32 << 20)),
                "too_large",
                Some("fix"),
            ),
            (
                Error::AppendUnknown {
                    attempts: 35,
                    last: Attempt::Status {
                        status: 503,
                        body: String::new(),
                    },
                },
                "outcome_unknown",
                Some("reattach"),
            ),
            (
                Error::Fenced(Fence::Superseded {
                    epoch: 1,
                    current: Some(2),
                }),
                "fenced",
                Some("stop"),
            ),
            (
                Error::Fenced(Fence::ClaimedDuringAttach { epoch: 7 }),
                "fenced",
                Some("stop"),
            ),
            (
                Error::Recreated {
                    url: url(),
                    incarnation: "i1".into(),
                },
                "fenced",
                Some("stop"),
            ),
            (
                Error::OpenOutsideVfs {
                    path: "/data/app.db".into(),
                    fd: 7,
                    file: DbFile::Shm,
                },
                "in_use",
                Some("fix"),
            ),
            (
                Error::NoSidecar {
                    path: "/data/app.db".into(),
                    source: Box::new(Error::LeavesWal),
                },
                "unreplicated_file",
                Some("fix"),
            ),
            (
                Error::OtherStream {
                    path: "/data/app.db".into(),
                    cached: "/b/a".into(),
                    wanted: "/b/s".into(),
                },
                "other_stream",
                Some("fix"),
            ),
            (
                Error::LostData {
                    url: url(),
                    offset: "9".into(),
                },
                "stream_state",
                Some("fix"),
            ),
            (
                Error::LostWrite { offset: 56 },
                "local_damage",
                Some("reattach"),
            ),
            (
                Error::PostAck {
                    what: "local WAL write",
                    code: 10,
                },
                "local_state",
                Some("reattach"),
            ),
            (
                Error::NotAttached {
                    path: "/data/app.db".into(),
                    last_failure: None,
                },
                "not_attached",
                Some("fix"),
            ),
            (Error::LeavesWal, "unsupported", Some("fix")),
            (
                Error::Panicked { entry: "x_read" },
                "internal",
                Some("reattach"),
            ),
        ];
        for (e, kind, action) in cases {
            assert_eq!(
                (e.kind().as_str(), e.kind().action().map(Action::as_str)),
                (kind, action),
                "{e}"
            );
        }
        // `attach_failed` has no action of its own: the message names the failure's kind next.
        let failed = Error::NotAttached {
            path: "/data/app.db".into(),
            last_failure: Some(std::sync::Arc::new(status("head", 400))),
        };
        assert_eq!(failed.kind().as_str(), "attach_failed");
        assert_eq!(failed.kind().action(), None);
        assert_eq!(
            failed.to_string(),
            "invalid_name: /data/app.db is not attached: its last attach failed (head \
             http://h/b/s: 400)"
        );
        // A poison is never retried on the same handle: a reason of a retry kind is an unknown
        // outcome there.
        assert_eq!(status("read", 503).poison_kind(), ErrorKind::OutcomeUnknown);
        assert_eq!(Error::LeavesWal.poison_kind(), ErrorKind::Unsupported);
        // Nothing tells the caller to delete files that may hold the only copy of data: each such
        // refusal names the salvage path, or moving the files aside.
        let salvage = [
            Error::LostData {
                url: url(),
                offset: "9".into(),
            },
            Error::StreamMissing {
                path: "/data/app.db".into(),
                url: url(),
            },
        ];
        for e in salvage {
            let m = e.to_string();
            assert!(m.contains("to a new name and export"), "{m}");
            assert!(m.contains("aside"), "{m}");
            assert!(!m.contains("delete"), "{m}");
        }
        let never_replicated = Error::NoSidecar {
            path: "/data/app.db".into(),
            source: Box::new(Error::LeavesWal),
        }
        .to_string();
        assert!(never_replicated.contains("aside") && !never_replicated.contains("delete"));
    }

    // `ursula_status` reports a refusal's kind (stable names) and its reason: the marker's token or
    // the layer's explanation, `None` when it sent none.
    #[test]
    fn refusal_kinds_and_reasons() {
        let body = |b: &str| b.to_owned();
        let cases = [
            (
                WriteRefusal::PaymentRequired {
                    body: body("quota"),
                },
                "payment_required",
                Some("quota"),
            ),
            (
                WriteRefusal::Unauthorized { body: body("") },
                "unauthorized",
                None,
            ),
            (
                WriteRefusal::Forbidden {
                    body: body(" denied \n"),
                },
                "forbidden",
                Some("denied"),
            ),
            (
                WriteRefusal::RateLimited { body: body("slow") },
                "rate_limited",
                Some("slow"),
            ),
            (
                WriteRefusal::NotApplied {
                    status: 503,
                    reason: body("WalDiskPressure"),
                },
                "not_applied",
                Some("WalDiskPressure"),
            ),
            (
                WriteRefusal::frame_over_limit(40 << 20, 32 << 20),
                "too_large",
                Some("41943040-byte frame over the 33554432-byte frame limit"),
            ),
            (
                WriteRefusal::TooLarge {
                    bytes: 9 << 20,
                    over: FrameLimit::InFront {
                        token: body("RequestBodyTooLarge"),
                    },
                },
                "too_large",
                Some("RequestBodyTooLarge"),
            ),
            (
                WriteRefusal::TooLarge {
                    bytes: 9 << 20,
                    over: FrameLimit::InFront { token: body(" ") },
                },
                "too_large",
                None,
            ),
        ];
        for (refusal, kind, reason) in cases {
            assert_eq!(
                (refusal.kind(), refusal.reason().as_deref()),
                (kind, reason)
            );
        }
        // A frame too large says what it was over, and how to get past it.
        assert_eq!(
            WriteRefusal::frame_over_limit(40 << 20, 32 << 20).to_string(),
            "41943040-byte commit frame over the 33554432-byte frame limit \
             (URSULA_VFS_MAX_FRAME_BYTES): split the transaction"
        );
        let in_front = WriteRefusal::TooLarge {
            bytes: 9 << 20,
            over: FrameLimit::InFront {
                token: "RequestBodyTooLarge".into(),
            },
        };
        assert_eq!(
            in_front.to_string(),
            "9437184-byte commit frame over a request body limit in front of the stream (413: \
             RequestBodyTooLarge): split the transaction"
        );
    }
}
