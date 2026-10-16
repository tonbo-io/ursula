//! `ursula_import(url, file, options_json, timeout_ms)`: a plain SQLite file becomes the first
//! snapshot of a new stream, page for page.
//!
//! The import never changes the source file (I1: it is opened read-only) and publishes nothing on
//! a stream that is not new (I2). In order:
//!
//! 1. the file is checked and copied into a temp directory, before anything is sent: SQLite's
//!    magic, 4096-byte pages, no `-wal` and no hot journal next to it (before and after the copy),
//!    and a length of exactly the pages its header counts. Page 1 of the copy is marked WAL (bytes
//!    18 and 19), as a bound database's always is;
//! 2. a HEAD: the stream is missing, or an empty stream of the VFS's content type with no
//!    snapshot. Anything else refuses (`server_too_old`, `stream_format`, `stream_state`). An
//!    answer without `Ursula-Protocol: 2` is `server_too_old`, never a missing stream: a gateway's
//!    concealing `404` creates nothing;
//! 3. the create, and the claim of epoch 1 under the create's token with the reserved activation
//!    `IMPORT_ACTIVATION`. The claim is resent unchanged, keyed by the producer, until a definite
//!    answer (I5). Its receipt must start at offset 0: no byte came before it. Its end is `W`,
//!    above 0 (I3), since builds skip a snapshot at offset 0, and the first open then claims above
//!    epoch 1;
//! 4. the `USS3` body of the copy at `W`, epoch 1, a fresh lineage and position 0;
//! 5. a HEAD naming the tail `W` and no snapshot, then the snapshotter's publish (one `PUT`, or
//!    parts with a cold store, otherwise refused naming the limit), then a HEAD again. The import
//!    is done only on a `204` naming `(W, digest)` and a HEAD naming that snapshot with the tail
//!    still at `W` (I4). Bytes after `W` fail with `stream_state`.
//!
//! A snapshot at `W` must equal a replay of the stream up to `W`, and the import's does not: the
//! stream up to `W` is the claim alone. So no read of the stream builds from the claim. Any read
//! that meets a claim of `IMPORT_ACTIVATION` (a build or catch-up from offset 0, a reconcile)
//! refuses `stream_state` (`codec::stream::FrameStream`), and an open's `activation` cannot be
//! the reserved one. Once the snapshot is published, every read starts at `W` and never meets the
//! claim. An open while the import runs is refused, and so is every open of a stream whose import
//! failed after its claim, until the stream is deleted or recreated. A second import into it is
//! refused too.
//!
//! The import blocks for at most `timeout_ms`.

use std::fs::File;
use std::io::Read;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

use ursula_headers::ClusterId;
use ursula_headers::Epoch;
use ursula_headers::Incarnation;
use ursula_headers::IncarnationPrecondition;
use ursula_headers::Offset;
use ursula_headers::ProducerId;
use ursula_headers::ProducerRequestHeaders;
use ursula_headers::ProducerWrite;
use ursula_headers::Seq;
use ursula_headers::SnapshotDigest;
use ursula_headers::SnapshotRef;
use ursula_headers::StreamHead;

use crate::codec::PAGE;
use crate::codec::digest::producer_digest;
use crate::codec::frame;
use crate::codec::frame::CONTENT_TYPE;
use crate::codec::frame::IMPORT_ACTIVATION;
use crate::codec::marker::SQLITE_MAGIC;
use crate::codec::snapshot::SnapshotFields;
use crate::generation::PathError;
use crate::generation::canonical_path;
use crate::ids::CommitId;
use crate::ids::GenId;
use crate::ids::Lineage;
use crate::ids::Position;
use crate::ids::random16;
use crate::json::Value;
use crate::json::parse_object;
use crate::log;
use crate::log::Level;
use crate::open::table::Refused;
use crate::open::table::check_identity;
use crate::options::OptionsError;
use crate::replicator::answer::ClaimDecision;
use crate::replicator::answer::Common;
use crate::replicator::answer::decide_claim;
use crate::snapshotter::Outcome;
use crate::snapshotter::Publish;
use crate::snapshotter::Remote;
use crate::snapshotter::Snapper;
use crate::snapshotter::SnapshotFailure;
use crate::snapshotter::Until;
use crate::snapshotter::publish;
use crate::snapshotter::transient;
use crate::snapshotter::write_snapshot;
use crate::transport::Transport;
use crate::wire::Answer;
use crate::wire::Exchange;
use crate::wire::Method;
use crate::wire::PRODUCER_ID;
use crate::wire::Request;
use crate::wire::Target;
use crate::wire::Typed;

/// The epoch the import claims: the first, so the first open claims above it.
const IMPORT_EPOCH: u64 = 1;

/// What a SQLite header holds where: the page size (big-endian u16, 1 for 65536), the file format
/// write and read versions (2 for WAL), the change counter, the page count, and the version-valid
/// counter that makes the page count trustworthy when it equals the change counter.
const PAGE_SIZE_AT: usize = 16;
const WRITE_VERSION_AT: u64 = 18;
const CHANGE_COUNTER_AT: usize = 24;
const PAGE_COUNT_AT: usize = 28;
const VERSION_VALID_AT: usize = 92;
const HEADER_LEN: usize = 100;

/// The format versions of a WAL database.
const WAL_VERSIONS: [u8; 2] = [2, 2];

/// Why the source file cannot be imported. Nothing was sent.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SourceProblem {
    #[error("{} is not a SQLite database", .0.display())]
    NotSqlite(PathBuf),
    #[error("{} has {page_size}-byte pages; the VFS takes 4096-byte pages (VACUUM INTO after PRAGMA page_size = 4096)", .path.display())]
    PageSize { path: PathBuf, page_size: u32 },
    #[error("{} is {len} bytes, not the {pages} pages of 4096 bytes its header counts", .path.display())]
    Length { path: PathBuf, len: u64, pages: u64 },
    #[error("{} is {len} bytes, not whole 4096-byte pages", .path.display())]
    PartialPage { path: PathBuf, len: u64 },
    /// A WAL beside the file may hold commits the file lacks.
    #[error("{} exists: checkpoint and close every connection first, or VACUUM INTO a file", .0.display())]
    Wal(PathBuf),
    /// A rollback journal beside the file: an interrupted transaction.
    #[error("{} is a hot journal: open the database with SQLite once to roll it back", .0.display())]
    HotJournal(PathBuf),
    #[error("{} changed while it was copied", .0.display())]
    Changed(PathBuf),
}

/// Why the stream is not one an import publishes on (I2), or stopped being one.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum NotNew {
    #[error("the stream is closed")]
    Closed,
    #[error("the stream holds bytes up to offset {tail}")]
    HoldsBytes { tail: Offset },
    #[error("the stream holds a snapshot at offset {offset}")]
    HoldsSnapshot { offset: Offset },
    #[error("bytes up to offset {start} came before the import's claim")]
    ClaimNotFirst { start: Offset },
    #[error("another claimant holds the stream (epoch {epoch:?})")]
    ClaimLost { epoch: Option<Epoch> },
    #[error("the stream went missing or was created again during the import")]
    Replaced,
    #[error("the stream refused the claim as out of sequence")]
    ClaimRefused,
    #[error("the stream's tail is at {found}, not at the imported snapshot's offset {imported}")]
    TailMoved { imported: Offset, found: Offset },
    #[error("the stream's latest snapshot is {found:?}, not the imported one at {imported}")]
    SnapshotMoved {
        imported: Offset,
        found: Option<SnapshotRef>,
    },
}

/// Why an import failed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ImportError {
    #[error("{name}: {why}")]
    InvalidArgument { name: &'static str, why: String },
    #[error(transparent)]
    Options(#[from] OptionsError),
    #[error("invalid_url: {0}")]
    InvalidUrl(#[from] PathError),
    #[error("{0}")]
    Source(SourceProblem),
    #[error("local_setup: {op} {}: {source}", .path.display())]
    Io {
        op: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("server_too_old: protocol {protocol:?}, this extension needs 2")]
    ServerTooOld { protocol: Option<u32> },
    #[error("stream_format: the stream's content type is {found:?}")]
    ContentType { found: Option<String> },
    #[error("stream_format: a stream at the path exists with another configuration")]
    Configuration,
    #[error("stream_state: {0}: import into a new stream")]
    NotNew(NotNew),
    #[error("wrong_cluster: the stream answered as cluster {found}, then as {other}")]
    WrongCluster { found: ClusterId, other: ClusterId },
    #[error("the stream answered {status} to the {what}")]
    Rejected { what: &'static str, status: u16 },
    #[error("unavailable: the {what} got no definite answer: {cause}")]
    Unavailable { what: &'static str, cause: String },
    #[error("unavailable: the import ran out of its {timeout_ms} ms")]
    TimedOut { timeout_ms: u64 },
    #[error("the snapshot: {0}")]
    Publish(#[source] SnapshotFailure),
    #[error("stream_state: {0:?}")]
    Refused(Refused),
}

impl From<Refused> for ImportError {
    fn from(refused: Refused) -> Self {
        match refused {
            Refused::ServerTooOld { protocol } => Self::ServerTooOld { protocol },
            Refused::ContentType { found } => Self::ContentType { found },
            Refused::Closed => Self::NotNew(NotNew::Closed),
            // The identity is checked against no cluster of ours, and a missing stream is not
            // refused: neither comes back.
            refused @ (Refused::WrongCluster { .. } | Refused::NotFound) => Self::Refused(refused),
        }
    }
}

/// The epoch the import claims: the first, so the first open claims above it.
fn import_epoch() -> Epoch {
    Epoch::try_from(IMPORT_EPOCH).expect("1 is an epoch")
}

/// The producer id of every VFS write.
fn producer_id() -> ProducerId {
    ProducerId::new(PRODUCER_ID).expect("the VFS's producer id is valid")
}

/// A token for the requests sent before the stream's is known, which carry none.
fn placeholder_token() -> Incarnation {
    Incarnation::try_from(1).expect("1 is a token")
}

fn io(op: &'static str, path: &Path) -> impl FnOnce(std::io::Error) -> ImportError {
    let path = path.to_owned();
    move |source| ImportError::Io { op, path, source }
}

/// What a finished import published.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Imported {
    /// `W`: the snapshot's offset, the end of the import's claim.
    pub(crate) offset: Offset,
    pub(crate) digest: SnapshotDigest,
    pub(crate) pages: u32,
    /// The `USS3` body's length.
    pub(crate) bytes: u64,
}

/// `options_json` of an import: `token` alone.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ImportOptions {
    pub(crate) token: Option<String>,
}

impl TryFrom<&str> for ImportOptions {
    type Error = OptionsError;

    fn try_from(text: &str) -> Result<Self, OptionsError> {
        let mut options = ImportOptions::default();
        let text = text.trim();
        if text.is_empty() || text == "null" {
            return Ok(options);
        }
        for (key, value) in parse_object(text)? {
            match (key.as_str(), value) {
                (_, Value::Null) => {}
                ("token", Value::Text(token)) => options.token = Some(token),
                ("token", _) => {
                    return Err(OptionsError::Type {
                        name: "token",
                        expected: "a string",
                    });
                }
                (other, _) => return Err(OptionsError::Unknown(other.to_owned())),
            }
        }
        Ok(options)
    }
}

/// `ursula_import`: imports `file` into a new stream at `url` over HTTP.
pub(crate) fn import(
    url: &str,
    file: &str,
    options: &str,
    timeout_ms: u64,
) -> Result<Imported, ImportError> {
    if timeout_ms == 0 {
        return Err(ImportError::InvalidArgument {
            name: "timeout_ms",
            why: "an import blocks: give it a positive timeout".to_owned(),
        });
    }
    canonical_path(url)?;
    let options = ImportOptions::try_from(options)?;
    let now = Instant::now();
    let deadline = now
        .checked_add(Duration::from_millis(timeout_ms))
        .unwrap_or(now);
    let staged = Staged::copy(Path::new(file))?;
    // The token stays the process's only once the import succeeded, and a failed import puts the
    // previous one back unless the application set another meanwhile (`ursula_set_token`).
    let lent = options.token.as_deref().map(crate::auth::lend_token);
    let transport = crate::transport::http::HttpTransport::new(url);
    let imported = run(&transport, &staged, deadline, timeout_ms);
    if let (Err(_failed), Some(lent)) = (&imported, lent) {
        crate::auth::take_back(lent);
    }
    let imported = imported?;
    log::emit(Level::Info, "imported", &[
        ("stream", &url),
        ("offset", &imported.offset),
        ("pages", &imported.pages),
        ("bytes", &imported.bytes),
    ]);
    Ok(imported)
}

/// The import's temp directory, removed when dropped.
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Result<Self, ImportError> {
        let id = GenId::random().map_err(io("read", Path::new(crate::ids::URANDOM)))?;
        let path = std::env::temp_dir().join(format!("ursula-import-{}", id.hex()));
        std::fs::create_dir(&path).map_err(io("create", &path))?;
        Ok(Self(path))
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_dir_all(&self.0) {
            log::emit(Level::Warn, "import_temp_left", &[
                ("path", &self.0.display()),
                ("error", &e),
            ]);
        }
    }
}

/// The checked copy of the source file, page 1 marked WAL.
pub(crate) struct Staged {
    dir: TempDir,
    image: PathBuf,
    pages: u32,
}

/// `<file><suffix>`.
fn beside(file: &Path, suffix: &str) -> PathBuf {
    let mut name = file.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

/// Neither a WAL nor a hot journal lies beside `file`.
fn check_beside(file: &Path) -> Result<(), ImportError> {
    let wal = beside(file, "-wal");
    if wal.exists() {
        return Err(ImportError::Source(SourceProblem::Wal(wal)));
    }
    let journal = beside(file, "-journal");
    match std::fs::metadata(&journal) {
        Ok(meta) if meta.len() > 0 => Err(ImportError::Source(SourceProblem::HotJournal(journal))),
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(io("stat", &journal)(e)),
    }
}

/// The pages of a database of `len` bytes whose first bytes are `header`, or why it is not one
/// the VFS imports.
pub(crate) fn check_header(path: &Path, header: &[u8], len: u64) -> Result<u32, SourceProblem> {
    let field = |at: usize, n: usize| header.get(at..at.saturating_add(n));
    if header.get(..SQLITE_MAGIC.len()) != Some(&SQLITE_MAGIC[..]) || header.len() < HEADER_LEN {
        return Err(SourceProblem::NotSqlite(path.to_owned()));
    }
    let be16 = |at| {
        field(at, 2)
            .and_then(|b| b.try_into().ok())
            .map(u16::from_be_bytes)
    };
    let be32 = |at| {
        field(at, 4)
            .and_then(|b| b.try_into().ok())
            .map(u32::from_be_bytes)
    };
    let page_size = match be16(PAGE_SIZE_AT) {
        Some(1) => 65_536,
        Some(n) => u32::from(n),
        None => return Err(SourceProblem::NotSqlite(path.to_owned())),
    };
    if page_size != PAGE as u32 {
        return Err(SourceProblem::PageSize {
            path: path.to_owned(),
            page_size,
        });
    }
    let page = PAGE as u64;
    if !len.is_multiple_of(page) {
        return Err(SourceProblem::PartialPage {
            path: path.to_owned(),
            len,
        });
    }
    let whole = len.checked_div(page).unwrap_or(0);
    let counted = be32(PAGE_COUNT_AT).filter(|n| {
        *n > 0
            && be32(CHANGE_COUNTER_AT).is_some()
            && be32(VERSION_VALID_AT) == be32(CHANGE_COUNTER_AT)
    });
    if let Some(counted) = counted
        && u64::from(counted) != whole
    {
        return Err(SourceProblem::Length {
            path: path.to_owned(),
            len,
            pages: u64::from(counted),
        });
    }
    u32::try_from(whole).map_err(|_too_many| SourceProblem::Length {
        path: path.to_owned(),
        len,
        pages: whole,
    })
}

impl Staged {
    /// Checks `file` and copies it (read-only) into a temp directory, page 1 marked WAL.
    pub(crate) fn copy(file: &Path) -> Result<Self, ImportError> {
        // The WAL and journal SQLite uses lie beside the file a symbolic link names.
        let resolved = std::fs::canonicalize(file).map_err(io("resolve", file))?;
        let file = resolved.as_path();
        check_beside(file)?;
        let dir = TempDir::new()?;
        let image = dir.0.join("image");
        let mut source = File::open(file).map_err(io("open", file))?;
        let before = source.metadata().map_err(io("stat", file))?;
        let mut copy = File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&image)
            .map_err(io("create", &image))?;
        std::io::copy(&mut source, &mut copy).map_err(io("copy", file))?;
        let after = source.metadata().map_err(io("stat", file))?;
        if before.len() != after.len() || before.modified().ok() != after.modified().ok() {
            return Err(ImportError::Source(SourceProblem::Changed(file.to_owned())));
        }
        drop(source);
        check_beside(file)?;
        let len = copy.metadata().map_err(io("stat", &image))?.len();
        let mut header = Vec::with_capacity(HEADER_LEN);
        File::open(&image)
            .and_then(|f| f.take(HEADER_LEN as u64).read_to_end(&mut header))
            .map_err(io("read", &image))?;
        let pages = check_header(file, &header, len).map_err(ImportError::Source)?;
        copy.write_all_at(&WAL_VERSIONS, WRITE_VERSION_AT)
            .map_err(io("write", &image))?;
        Ok(Self { dir, image, pages })
    }
}

/// Stops the import's [`Snapper`] at the deadline, so its publish sends nothing more.
struct Watchdog {
    snapper: Arc<Snapper>,
    expired: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Watchdog {
    fn start(deadline: Instant) -> Self {
        let snapper = Arc::new(Snapper::default());
        let expired = Arc::new(AtomicBool::new(false));
        let (s, e) = (Arc::clone(&snapper), Arc::clone(&expired));
        let thread = std::thread::Builder::new()
            .name("ursula-vfs-import".to_owned())
            .spawn(move || {
                // True at the deadline, false once the import stopped it.
                if s.sleep_until(deadline) {
                    e.store(true, Ordering::Release);
                    s.stop();
                }
            })
            .ok();
        Self {
            snapper,
            expired,
            thread,
        }
    }

    fn expired(&self) -> bool {
        self.expired.load(Ordering::Acquire)
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        self.snapper.stop();
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
        {
            log::emit(Level::Error, "import_watchdog_panicked", &[]);
        }
    }
}

/// Steps 2 to 5 of the module docs, through `transport`.
pub(crate) fn run(
    transport: &dyn Transport,
    staged: &Staged,
    deadline: Instant,
    timeout_ms: u64,
) -> Result<Imported, ImportError> {
    let watchdog = Watchdog::start(deadline);
    let outcome = steps(transport, staged, deadline, &watchdog.snapper);
    match outcome {
        Err(ImportError::Publish(SnapshotFailure::Reset) | ImportError::Unavailable { .. })
            if watchdog.expired() || Instant::now() >= deadline =>
        {
            Err(ImportError::TimedOut { timeout_ms })
        }
        other => other,
    }
}

/// A request sent until a definite answer or the deadline.
fn send(
    remote: &Remote<'_>,
    what: &'static str,
    request: &Request,
    deadline: Instant,
) -> Result<Answer, ImportError> {
    remote
        .send_until(what, request, Until::Deadline(deadline), transient)
        .map_err(|e| match e {
            SnapshotFailure::Outage { what, cause, .. } => ImportError::Unavailable {
                what,
                cause: cause.to_string(),
            },
            other => ImportError::Publish(other),
        })
}

/// The stream a HEAD names, checked: protocol 2 and the cluster `ours` once known, then missing
/// (`None`, a `404`), or a stream of the VFS's content type, open and empty, with no snapshot.
fn check_new(
    answer: &Answer,
    ours: Option<&ClusterId>,
) -> Result<(ClusterId, Option<StreamHead>), ImportError> {
    let (identity, stream) = match &answer.typed {
        Ok(Typed::Head { identity, stream }) if matches!(answer.status, 200 | 404) => {
            (identity.as_ref(), stream.as_ref())
        }
        _ => return Err(rejected("HEAD", answer)),
    };
    let identity = check_identity(identity, None)?;
    if let Some(ours) = ours
        && *ours != identity.cluster_id
    {
        return Err(ImportError::WrongCluster {
            found: ours.clone(),
            other: identity.cluster_id.clone(),
        });
    }
    let cluster_id = identity.cluster_id.clone();
    let Some(stream) = stream else {
        return Ok((cluster_id, None));
    };
    if answer.content_type.as_deref() != Some(CONTENT_TYPE) {
        return Err(ImportError::ContentType {
            found: answer.content_type.clone(),
        });
    }
    if stream.closed() {
        return Err(ImportError::NotNew(NotNew::Closed));
    }
    if let Some(snapshot) = stream.snapshot() {
        return Err(ImportError::NotNew(NotNew::HoldsSnapshot {
            offset: snapshot.offset,
        }));
    }
    if stream.next_offset() > Offset::ZERO {
        return Err(ImportError::NotNew(NotNew::HoldsBytes {
            tail: stream.next_offset(),
        }));
    }
    Ok((cluster_id, Some(*stream)))
}

/// A definite answer the import does not take.
fn rejected(what: &'static str, answer: &Answer) -> ImportError {
    match &answer.typed {
        Ok(Typed::IncarnationMismatch(_)) => ImportError::NotNew(NotNew::Replaced),
        _ => ImportError::Rejected {
            what,
            status: answer.status,
        },
    }
}

/// The create: the token of the stream, ours or one that was created meanwhile (the claim's
/// receipt proves it empty).
fn create(remote: &Remote<'_>, deadline: Instant) -> Result<Incarnation, ImportError> {
    let request = Request {
        content_type: Some(CONTENT_TYPE.to_owned()),
        ..Request::new(Method::Put, Target::Stream)
    };
    let answer = send(remote, "create", &request, deadline)?;
    match &answer.typed {
        Ok(Typed::Created { incarnation }) => Ok(*incarnation),
        _ if answer.status == 409 => Err(ImportError::Configuration),
        _ => Err(rejected("create", &answer)),
    }
}

/// The claim of epoch 1 under `incarnation`, resent unchanged until a definite answer: `W`, the
/// end of its receipt, which must start at offset 0.
fn claim(
    remote: &Remote<'_>,
    incarnation: Incarnation,
    deadline: Instant,
) -> Result<Offset, ImportError> {
    let epoch = import_epoch();
    let body: Arc<[u8]> = Arc::from(frame::encode_claim(epoch, IMPORT_ACTIVATION));
    let id = producer_id();
    let request = Request {
        keys: ProducerRequestHeaders {
            producer: Some(ProducerWrite {
                id,
                epoch,
                seq: Seq::ZERO,
                digest: Some(producer_digest(&body)),
            }),
            precondition: Some(IncarnationPrecondition { incarnation }),
        },
        content_type: Some(CONTENT_TYPE.to_owned()),
        body,
        ..Request::new(Method::Post, Target::Stream)
    };
    let answer = send(remote, "claim", &request, deadline)?;
    let status = answer.status;
    match decide_claim(&Exchange::Answered(Box::new(answer))) {
        ClaimDecision::Won { receipt, .. } if receipt.start() == Offset::ZERO => {
            let w = receipt.next();
            if w > Offset::ZERO {
                Ok(w)
            } else {
                Err(ImportError::NotNew(NotNew::ClaimRefused))
            }
        }
        ClaimDecision::Won { receipt, .. } => Err(ImportError::NotNew(NotNew::ClaimNotFirst {
            start: receipt.start(),
        })),
        ClaimDecision::Lost { epoch } => Err(ImportError::NotNew(NotNew::ClaimLost { epoch })),
        ClaimDecision::Common(Common::Head(_)) => Err(ImportError::NotNew(NotNew::Replaced)),
        ClaimDecision::Common(Common::Reset(_)) => Err(ImportError::NotNew(NotNew::ClaimRefused)),
        ClaimDecision::Common(_) => Err(ImportError::Rejected {
            what: "claim",
            status,
        }),
    }
}

/// The checks of a HEAD after the claim: the tail at `w`, and the latest snapshot `snapshot`.
fn check_after(head: &StreamHead, w: Offset, snapshot: Option<SnapshotRef>) -> Result<(), NotNew> {
    if head.next_offset() != w {
        return Err(NotNew::TailMoved {
            imported: w,
            found: head.next_offset(),
        });
    }
    if head.snapshot() != snapshot {
        return Err(NotNew::SnapshotMoved {
            imported: w,
            found: head.snapshot(),
        });
    }
    Ok(())
}

fn steps(
    transport: &dyn Transport,
    staged: &Staged,
    deadline: Instant,
    snapper: &Snapper,
) -> Result<Imported, ImportError> {
    let probe = Remote {
        transport,
        snapper,
        cluster_id: "",
        // Unused: the HEAD, the create and the claim carry their own keys.
        incarnation: placeholder_token(),
    };
    let head = send(
        &probe,
        "HEAD",
        &Request::new(Method::Head, Target::Stream),
        deadline,
    )?;
    let (cluster_id, _) = check_new(&head, None)?;
    let incarnation = create(&probe, deadline)?;
    let w = claim(&probe, incarnation, deadline)?;
    let lineage = Lineage(random16().map_err(io("read", Path::new(crate::ids::URANDOM)))?);
    let fields = SnapshotFields {
        offset: w,
        epoch: import_epoch(),
        last: CommitId {
            lineage,
            position: Position(0),
        },
    };
    let mut image = File::open(&staged.image).map_err(io("open", &staged.image))?;
    let body = staged.dir.0.join("body");
    let (len, digest) = write_snapshot(&fields, &mut image, &body).map_err(ImportError::Publish)?;
    drop(image);
    let remote = Remote {
        transport,
        snapper,
        cluster_id: cluster_id.as_str(),
        incarnation,
    };
    let fail = |e: SnapshotFailure| match e {
        SnapshotFailure::TokenChanged => ImportError::NotNew(NotNew::Replaced),
        e => ImportError::Publish(e),
    };
    let before = remote.head().map_err(fail)?;
    check_after(&before, w, None).map_err(ImportError::NotNew)?;
    let published = publish(&Publish {
        remote: Remote { ..remote },
        request_limit: crate::config::max_request_bytes().unwrap_or(u64::MAX),
        offset: w,
        digest,
        body: &body,
        len,
    })
    .map_err(fail)?;
    match published {
        Outcome::Published { offset } if offset == w => {}
        Outcome::Published { offset } | Outcome::Superseded { offset } => {
            return Err(ImportError::NotNew(NotNew::HoldsSnapshot { offset }));
        }
        Outcome::Busy | Outcome::Skipped => {
            return Err(ImportError::Publish(SnapshotFailure::Reset));
        }
    }
    let after = remote.head().map_err(fail)?;
    let ours = SnapshotRef { offset: w, digest };
    check_after(&after, w, Some(ours)).map_err(ImportError::NotNew)?;
    Ok(Imported {
        offset: w,
        digest,
        pages: staged.pages,
        bytes: len,
    })
}

#[cfg(test)]
mod tests;
