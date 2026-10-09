//! The crate's error type: why an operation failed, or why an attached database is poisoned.
//!
//! Callers act on three classes, through methods rather than message text: [`Error::is_gone`]
//! (the data lies below the stream's retention: attach retries from a newer snapshot),
//! [`Error::is_recreated`] (the stream is another incarnation: attach rebuilds) and
//! [`Error::is_fenced`] (another owner or writer holds the stream: `ursula_status` reports
//! `fenced`). Every other variant fails the operation, or poisons the database until it is
//! attached again.

use std::ffi::CStr;
use std::ffi::c_int;
use std::sync::Arc;

use crate::frame::FrameError;
use crate::frame::PAGE;
use crate::snapshot::SnapshotError;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    // Local files and the host process.
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
    #[error("{path} is being attached by another thread")]
    AttachInProgress { path: String },
    #[error(
        "{path} is not attached{}",
        .last_failure.as_ref().map_or_else(String::new, |e| format!(": its last attach failed ({e})"))
    )]
    NotAttached {
        path: String,
        last_failure: Option<Arc<Error>>,
    },
    #[error("{path} has content but no readable sidecar ({source}); refusing to attach")]
    NoSidecar {
        path: String,
        #[source]
        source: Box<Error>,
    },
    #[error("{path} is a cache of stream {cached}, not {wanted}; delete it to attach it there")]
    OtherStream {
        path: String,
        cached: String,
        wanted: String,
    },
    #[error(
        "{url} is missing and {path} holds data: refusing to create an empty stream over it \
         (delete {path} to start over, or attach it under another URL)"
    )]
    StreamMissing { path: String, url: String },
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
    #[error("db file handle: {0}")]
    DbFileHandle(c_int),
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
    #[error("append rejected: {status}{}", with_body(.body))]
    AppendRejected { status: u16, body: String },
    #[error("append: {last} (outcome unknown after {attempts} attempts)")]
    AppendUnknown { attempts: u32, last: Attempt },
    #[error("put {url}: {last}")]
    PutUnknown { url: String, last: Attempt },
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
    #[error(
        "read {url} at {offset}: beyond the end of the stream that acknowledged it (the server \
         lost acknowledged data?); the local files are kept (delete them to rebuild)"
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
    #[error("producer expired again right after a re-claim")]
    ProducerExpiredAgain,
    #[error("local WAL write: {0}")]
    LocalWalWrite(c_int),
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
    #[error("epoch {epoch} superseded by {current:?} (403)")]
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
    #[error("another owner claimed epoch {epoch} during attach; attach again")]
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
    #[error("{0}")]
    Transport(#[source] Box<ureq::Error>),
}

impl Error {
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

#[cfg(test)]
mod tests {
    use super::Error;
    use super::Fence;
    use super::Gone;

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
        let superseded = Error::Fenced(Fence::Superseded {
            epoch: 2,
            current: Some(3),
        });
        assert!(superseded.is_fenced() && !superseded.is_recreated());
        assert!(
            superseded
                .to_string()
                .ends_with("superseded by Some(3) (403)")
        );
        let gone = Error::Gone(Gone::SnapshotSuperseded { offset: "7".into() });
        assert!(gone.is_gone() && !gone.is_fenced() && !gone.is_recreated());
        let local = Error::LocalWalWrite(10);
        assert!(!local.is_fenced() && !local.is_gone() && !local.is_recreated());
        let lost = Error::LostData {
            url: "http://h/b/s".into(),
            offset: "9".into(),
        };
        assert!(!lost.is_gone() && lost.to_string().contains("lost acknowledged data"));
    }
}
