//! The snapshot thread of a bound database (design: Snapshots): snapshots published keyed by offset
//! and digest, and retention one snapshot behind.
//!
//! A snapshot is due when the frame bytes acknowledged since the latest one exceed the db file's
//! size or 8 MiB, whichever is larger (`URSULA_VFS_SNAPSHOT_DUE_BYTES` replaces the threshold, for
//! tests). The driver wakes the thread ([`Snapper`]) when it is due. An attempt:
//!
//! 1. opens the extension's private connection through the "ursula" VFS (so its checkpoints take
//!    F1) and checkpoints passively outside the window;
//! 2. opens the window once no logged commit is undecided and no other connection checkpoints
//!    (commit points wait while it is open, and at most a second for a wanted one);
//! 3. takes `k`, the last logged commit, and asks the driver for a batch ending exactly at `k`;
//! 4. checkpoints every frame, begins a read transaction that sees exactly `k` and closes the
//!    window;
//! 5. copies the pages into a temp file 256 pages at a time, fsyncs the db file and appends
//!    BASE(`k`);
//! 6. waits for `k`'s acknowledgement, whose receipt names `W`, fsyncs ACK(`k`, `W`) (so the
//!    snapshot never runs ahead of the durable acknowledged position the `410` rule compares
//!    with), and writes the `USS3` body to a temp file while its digest is computed;
//! 7. publishes it: a body of at most the request limit `L` in one `PUT {stream}/snapshot/{W}`
//!    with `Stream-Snapshot-Digest`; a larger one, when the HEAD's `Ursula-Max-Snapshot-Bytes`
//!    holds it in at most 4096 parts of `L`, in parts ([`publish`]): each part read from the body
//!    file and staged with `POST {stream}/snapshot/{W}/parts`, then one commit listing their ids.
//!    Only a `204` naming `W` and the digest confirms it, or a `409` naming them as the latest
//!    (an earlier attempt's publish). A `409` with a later offset, or a `410`
//!    below a later snapshot's retention, means another snapshot superseded it; one at `W` with
//!    another digest stops snapshots (`stream_state`); a body no request or commit can carry is
//!    not published (`snapshot.error`);
//! 8. moves retention to the snapshot the stream held before, one behind, monotonic, and only
//!    after that `204`.
//!
//! A commit is keyed by `(W, digest)`, and the server answers its repeat from the key even after
//! the parts are gone, so a commit whose answer is lost or retryable (no answer, a timeout, a
//! `502`, a `503` because the node copies it or other commits) is repeated unchanged, for up to
//! 30 minutes. Part ids are minted by the server and never reused: a part whose `201` is lost is
//! staged again under a new id, and a commit lists only the ids its own staging got, in order. A
//! `400` stages every part again once. A `429` to a part naming the stream's limit of staged parts
//! (`Ursula-Max-Staged-Parts` or `-Bytes`) is never repeated: it fails the attempt. Another `429`
//! is a rate limit, repeated as any. Nothing of a staging survives the attempt: one that fails
//! before its commit is decided discards the stream's staged parts (`DELETE
//! {stream}/snapshot/parts`, once, best effort), the next attempt starts over, and the server
//! sweeps what is left after a day.
//!
//! A failed attempt waits before the next ([`SnapshotFailing`]): from 100 ms, doubling, up to five
//! minutes, or an hour when the body is too large. Requests carry the binding's token as it is
//! when they go out, so snapshots go on under the new token after a re-key (design: History). A
//! HEAD or a publish that finds another token, and a HEAD of another cluster, are failures like any
//! other: the binding re-keys, resets or blocks meanwhile, and the next attempt finds out which.
//! Each attempt sends its HEAD before the checkpoint, so a binding left idle across a history
//! change copies and compresses nothing, and a token found changed makes the replicator probe.

use std::collections::hash_map::RandomState;
use std::fs::File;
use std::hash::BuildHasher;
use std::hash::Hasher;
use std::io::BufWriter;
use std::io::Seek;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::OnceLock;
use std::sync::PoisonError;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

use ursula_headers::ClusterId;
use ursula_headers::Epoch;
use ursula_headers::ErrorCode;
use ursula_headers::Incarnation;
use ursula_headers::IncarnationPrecondition;
use ursula_headers::MAX_SNAPSHOT_PARTS;
use ursula_headers::Offset;
use ursula_headers::PartsCommit;
use ursula_headers::PartsManifest;
use ursula_headers::ProducerRequestHeaders;
use ursula_headers::SizeLimit;
use ursula_headers::SnapshotDigest;
use ursula_headers::SnapshotPublishAnswer;
use ursula_headers::SnapshotRef;
use ursula_headers::StreamHead;
use ursula_headers::WriteOutcome;

use crate::codec::PAGE;
use crate::codec::digest::SnapshotDigester;
use crate::codec::frame::CONTENT_TYPE;
use crate::codec::segment::RecordBody;
use crate::codec::snapshot::SnapshotError;
use crate::codec::snapshot::SnapshotFields;
use crate::codec::snapshot::write_body;
use crate::config::SnapshotHold;
use crate::db::Binding;
use crate::db::LogRefused;
use crate::db::Window;
use crate::driver::runtime::DriverGone;
use crate::driver::runtime::driver;
use crate::host::Checkpoint;
use crate::host::HostError;
use crate::host::Private;
use crate::ids::CommitId;
use crate::ids::Position;
use crate::log;
use crate::log::Level;
use crate::replicator::BoundaryReceipt;
use crate::replicator::ResetKind;
use crate::replicator::backoff::BACKOFF_CAP_MS;
use crate::replicator::backoff::RETRY_AFTER_CAP_MS;
use crate::replicator::backoff::backoff_ms;
use crate::transport::Attempt;
use crate::transport::Transport;
use crate::wire::Answer;
use crate::wire::Exchange;
use crate::wire::Method;
use crate::wire::NoAnswer;
use crate::wire::Request;
use crate::wire::Target;
use crate::wire::Typed;

/// The smallest due threshold: a snapshot is due past the db file's size or this, whichever is
/// larger.
const DUE_MIN_BYTES: u64 = 8 << 20;

/// The frame bytes acknowledged since the latest snapshot past which one is due, for a db file
/// of `db_bytes`.
pub(crate) fn due_bytes(db_bytes: u64) -> u64 {
    crate::config::snapshot_due_bytes().unwrap_or_else(|| db_bytes.max(DUE_MIN_BYTES))
}

/// What the snapshot thread waits on: a request, a reschedule, the stop, and the receipts of the
/// boundaries it asked the driver for.
#[derive(Debug, Default)]
struct WakeState {
    requested: bool,
    rescheduled: bool,
    stopped: bool,
    receipts: Vec<BoundaryReceipt>,
}

/// Wakes a bound database's snapshot thread, and hands it the receipts of its boundaries.
#[derive(Debug, Default)]
pub(crate) struct Snapper {
    state: Mutex<WakeState>,
    cv: Condvar,
}

impl Snapper {
    fn lock(&self) -> MutexGuard<'_, WakeState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A snapshot is due.
    pub(crate) fn request(&self) {
        self.lock().requested = true;
        self.cv.notify_all();
    }

    /// The wait after a failure moved: a sleep ends early to look at it again.
    pub(crate) fn reschedule(&self) {
        self.lock().rescheduled = true;
        self.cv.notify_all();
    }

    pub(crate) fn stopped(&self) -> bool {
        self.lock().stopped
    }

    /// The binding retired or reset: the thread ends after its attempt.
    pub(crate) fn stop(&self) {
        self.lock().stopped = true;
        self.cv.notify_all();
    }

    /// The driver acknowledged (or passed) a boundary.
    pub(crate) fn receipt(&self, receipt: BoundaryReceipt) {
        self.lock().receipts.push(receipt);
        self.cv.notify_all();
    }

    /// Waits for a request (true) or the stop (false).
    pub(crate) fn wait(&self) -> bool {
        let mut s = self.lock();
        loop {
            if s.stopped {
                return false;
            }
            if std::mem::take(&mut s.requested) {
                return true;
            }
            s = self.cv.wait(s).unwrap_or_else(PoisonError::into_inner);
        }
    }

    /// Sleeps until `deadline` or a reschedule (true), or the stop (false). Requests do not end
    /// it: they stay for the next [`Snapper::wait`].
    pub(crate) fn sleep_until(&self, deadline: Instant) -> bool {
        let mut s = self.lock();
        loop {
            if s.stopped {
                return false;
            }
            if std::mem::take(&mut s.rescheduled) {
                return true;
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return true;
            }
            s = self
                .cv
                .wait_timeout(s, left)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }

    /// Forgets the receipts of earlier attempts.
    fn clear_receipts(&self) {
        self.lock().receipts.clear();
    }

    /// Waits for the receipt of boundary `position`; `None` once stopped.
    pub(crate) fn wait_receipt(&self, position: Position) -> Option<BoundaryReceipt> {
        let mut s = self.lock();
        loop {
            if s.stopped {
                return None;
            }
            if let Some(at) = s.receipts.iter().position(|r| r.position == position) {
                return Some(s.receipts.swap_remove(at));
            }
            s = self.cv.wait(s).unwrap_or_else(PoisonError::into_inner);
        }
    }
}

/// What status reports of a binding's snapshots (design: Status).
#[derive(Debug, Clone, Default)]
pub(crate) struct SnapshotStatus {
    /// The latest snapshot this binding published, and when.
    pub(crate) published: Option<(Offset, Instant)>,
    /// The run of failed attempts since the last that published.
    pub(crate) failing: Option<SnapshotFailing>,
    /// Snapshots stopped for good: why.
    pub(crate) stopped: Option<String>,
}

impl SnapshotStatus {
    /// What `snapshot.error` reports.
    pub(crate) fn error(&self) -> Option<&str> {
        self.stopped
            .as_deref()
            .or(self.failing.as_ref().map(|f| f.error.as_str()))
    }
}

/// A bound database's snapshots, shared by its snapshot thread, the driver and status.
#[derive(Debug)]
pub(crate) struct Snapshots {
    pub(crate) snapper: Snapper,
    /// The threshold the thread last computed ([`due_bytes`]).
    due_bytes: AtomicU64,
    status: Mutex<SnapshotStatus>,
}

impl Default for Snapshots {
    fn default() -> Self {
        Self {
            snapper: Snapper::default(),
            due_bytes: AtomicU64::new(due_bytes(0)),
            status: Mutex::new(SnapshotStatus::default()),
        }
    }
}

impl Snapshots {
    pub(crate) fn due_bytes(&self) -> u64 {
        self.due_bytes.load(Ordering::Acquire)
    }

    fn set_due_bytes(&self, bytes: u64) {
        self.due_bytes.store(bytes, Ordering::Release);
    }

    fn lock(&self) -> MutexGuard<'_, SnapshotStatus> {
        self.status.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn status(&self) -> SnapshotStatus {
        self.lock().clone()
    }

    /// `log_bytes` frame bytes are acknowledged since the latest snapshot: due past the
    /// threshold, the thread wakes.
    pub(crate) fn acknowledged(&self, log_bytes: u64) {
        let rescheduled = self
            .lock()
            .failing
            .as_mut()
            .is_some_and(|f| f.commit_acknowledged(Instant::now()));
        if rescheduled {
            self.snapper.reschedule();
        }
        if log_bytes > self.due_bytes() {
            self.snapper.request();
        }
    }
}

/// The snapshot threads of the process, joined as they end.
fn threads() -> MutexGuard<'static, Vec<JoinHandle<()>>> {
    static T: OnceLock<Mutex<Vec<JoinHandle<()>>>> = OnceLock::new();
    T.get_or_init(Default::default)
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// Starts the snapshot thread of `b`, which ends once the binding retires or resets.
pub(crate) fn spawn(b: &Arc<Binding>) {
    let binding = Arc::clone(b);
    let spawned = std::thread::Builder::new()
        .name("ursula-vfs-snapshot".to_owned())
        .spawn(move || {
            crate::guard::contain("snapshot_thread", || (), || run(&binding));
        });
    match spawned {
        Ok(handle) => {
            let mut threads = threads();
            let (done, running): (Vec<_>, Vec<_>) =
                threads.drain(..).partition(JoinHandle::is_finished);
            *threads = running;
            threads.push(handle);
            drop(threads);
            for handle in done {
                if handle.join().is_err() {
                    log::emit(Level::Error, "snapshot_thread_panicked", &[]);
                }
            }
        }
        Err(e) => log::emit(Level::Error, "snapshot_thread_failed", &[
            ("file", &b.path),
            ("error", &e),
        ]),
    }
}

/// The db file's size in pages, and whether a snapshot is due (recording the threshold).
fn due(b: &Binding) -> (u32, bool) {
    let db_bytes = std::fs::metadata(b.handle.generation.db()).map_or(0, |m| m.len());
    let threshold = due_bytes(db_bytes);
    let snapshots = &b.handle.snapshots;
    snapshots.set_due_bytes(threshold);
    let pages = u32::try_from(db_bytes.checked_div(PAGE as u64).unwrap_or(0)).unwrap_or(u32::MAX);
    (pages, b.handle.replication().log_bytes > threshold)
}

/// The snapshot thread's loop.
fn run(b: &Binding) {
    let snapshots = &b.handle.snapshots;
    let snapper = &snapshots.snapper;
    let mut busy = Duration::from_millis(10);
    while snapper.wait() {
        // After a failure, the next attempt waits, unless the database shrank.
        loop {
            let (pages, _) = due(b);
            let hold = snapshots
                .status()
                .failing
                .filter(|f| f.holds_back(pages, Instant::now()))
                .map(|f| f.until);
            match hold {
                Some(until) if !snapper.sleep_until(until) => return,
                Some(_) => {}
                None => break,
            }
        }
        if b.handle.reset().is_some() || snapper.stopped() {
            return;
        }
        let (pages, is_due) = due(b);
        if !is_due {
            continue;
        }
        let attempt = attempt(b);
        let now = Instant::now();
        match attempt {
            Ok(Outcome::Busy) => {
                let until = now.checked_add(busy).unwrap_or(now);
                if !snapper.sleep_until(until) {
                    return;
                }
                busy = busy.saturating_mul(2).min(Duration::from_secs(1));
                snapper.request();
            }
            Ok(Outcome::Skipped) => {}
            Ok(Outcome::Published { offset } | Outcome::Superseded { offset }) => {
                busy = Duration::from_millis(10);
                let mut status = snapshots.lock();
                status.failing = None;
                status.published = Some((offset, now));
                drop(status);
                log::emit(Level::Info, "snapshot_published", &[
                    ("file", &b.path),
                    ("offset", &offset),
                ]);
                if due(b).1 {
                    snapper.request();
                }
            }
            Err(e) if e.stops() => {
                log::emit(Level::Error, "snapshots_stopped", &[
                    ("file", &b.path),
                    ("reason", &e),
                ]);
                snapshots.lock().stopped = Some(e.to_string());
                return;
            }
            Err(e) => {
                let kind = e.kind();
                let failing = {
                    let mut status = snapshots.lock();
                    let failing = SnapshotFailing::failed(
                        status.failing.as_ref(),
                        kind,
                        e.to_string(),
                        pages,
                        e.retry_after(),
                        now,
                    );
                    status.failing = Some(failing.clone());
                    failing
                };
                log::emit(Level::Warn, "snapshot_failed", &[
                    ("file", &b.path),
                    ("stream", &b.url),
                    ("failures", &failing.count),
                    (
                        "retry_ms",
                        &failing.until.saturating_duration_since(now).as_millis(),
                    ),
                    ("kind", &kind.name()),
                    ("reason", &e),
                ]);
                // Taken once the wait ends, if still due then.
                snapper.request();
            }
        }
    }
}

/// What an attempt came to, short of a failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    Published {
        offset: Offset,
    },
    /// A later snapshot was published first: it covers this one.
    Superseded {
        offset: Offset,
    },
    /// Another connection's checkpoint, or a reader, kept frames out of the db file: soon again.
    Busy,
    /// Stopped, or no longer bound: nothing to do.
    Skipped,
}

/// Why an attempt failed.
/// What the last attempt of a request that never got a definite answer met.
#[derive(Debug, thiserror::Error)]
pub(crate) enum OutageCause {
    #[error("no answer: {0:?}")]
    NoAnswer(NoAnswer),
    #[error("status {0}")]
    Status(u16),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SnapshotFailure {
    #[error("the private connection: {0}")]
    Host(#[from] HostError),
    #[error("{op} {}: {source}", .path.display())]
    Io {
        op: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("the snapshot body: {0}")]
    Body(#[from] SnapshotError),
    #[error("request_too_large: a snapshot body of {bytes} bytes is not published: {bound}")]
    TooLarge { bytes: u64, bound: SizeBound },
    #[error("stream_state: the stream holds another snapshot at offset {offset}")]
    Conflict { offset: Offset },
    #[error("the stream answered {status} to the {what}")]
    Status { what: &'static str, status: u16 },
    #[error("the stream answered the {what} with malformed headers")]
    Malformed { what: &'static str },
    /// The stream answered under another token: the binding re-keys or resets meanwhile, and the
    /// next attempt runs under the token it has then.
    #[error("the stream answered under another token")]
    TokenChanged,
    /// A HEAD answered as another deployment: the binding blocks `wrong_cluster` until a routing
    /// fix (design: Identity).
    #[error("wrong_cluster: the stream answers as cluster {found}, not {expected}")]
    WrongCluster { expected: String, found: ClusterId },
    #[error("unavailable: the {what} got no definite answer: {cause}")]
    Outage {
        what: &'static str,
        cause: OutageCause,
        retry_after: Option<Duration>,
    },
    /// A part met the stream's limit of staged parts (`429`): the parts were discarded, and the
    /// next attempt stages again.
    #[error("unavailable: the stream holds as many staged parts as it takes (status {status})")]
    StagedPartsFull { status: u16 },
    #[error("the binding reset")]
    Reset,
    #[error(transparent)]
    Driver(#[from] DriverGone),
}

/// The bound a snapshot body that cannot be published passes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum SizeBound {
    /// The request limit `L` of a single request, which a `413` named.
    #[error("above the request limit of {0} bytes")]
    Request(u64),
    /// Above `L`, and the stream takes no parts: no cold store (no `Ursula-Max-Snapshot-Bytes`,
    /// or a `501`).
    #[error("above the request limit of {request} bytes, and the stream takes no parts")]
    NoParts { request: u64 },
    /// The snapshot limit of a commit in parts, which the HEAD or a `413` named.
    #[error("above the snapshot limit of {0} bytes")]
    Snapshot(u64),
    /// More than `MAX_SNAPSHOT_PARTS` parts of `L` bytes.
    #[error("more than {MAX_SNAPSHOT_PARTS} parts of the request limit of {part_bytes} bytes")]
    Parts { part_bytes: u64 },
    /// A `413` that named no limit.
    #[error("above a limit the stream did not name")]
    Unnamed,
}

impl SnapshotFailure {
    fn kind(&self) -> FailureKind {
        match self {
            Self::TooLarge { .. } => FailureKind::Size,
            Self::Outage { .. } | Self::StagedPartsFull { .. } => FailureKind::Outage,
            _ => FailureKind::Other,
        }
    }

    /// Whether snapshots stop for this binding: it reset, or the stream holds another snapshot
    /// at the offset this one would take.
    pub(crate) fn stops(&self) -> bool {
        matches!(self, Self::Reset | Self::Conflict { .. } | Self::Driver(_))
    }

    fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::Outage { retry_after, .. } => *retry_after,
            _ => None,
        }
    }
}

fn io(op: &'static str, path: &Path) -> impl FnOnce(std::io::Error) -> SnapshotFailure {
    let path = path.to_owned();
    move |source| SnapshotFailure::Io { op, path, source }
}

/// The window, closed when dropped, which wakes the commits waiting on it.
struct WindowGuard<'a>(&'a Binding);

impl Drop for WindowGuard<'_> {
    fn drop(&mut self) {
        self.0.lock().window = Window::Closed;
        self.0.notify_decided();
    }
}

/// How often the snapshot thread looks at a window it wants again: decisions and checkpoint
/// ends wake it, this only bounds a missed wake-up.
const WINDOW_POLL: Duration = Duration::from_millis(100);

/// Opens the window once no logged commit is undecided and no other connection checkpoints, and
/// returns `k`, the last logged commit. `None` when the binding stopped meanwhile.
fn open_window(b: &Binding) -> Option<(WindowGuard<'_>, Position)> {
    let mut c = b.lock();
    c.window = Window::Wanted;
    // Wanted, a commit point waits for it at most a second: wake those waiting.
    b.notify_decided();
    loop {
        if b.reset_kind().is_some() || b.handle.snapshots.snapper.stopped() {
            c.window = Window::Closed;
            drop(c);
            b.notify_decided();
            return None;
        }
        if c.undecided.is_empty() && c.checkpoint.is_none() {
            break;
        }
        c = b.wait_decided(c, WINDOW_POLL);
    }
    c.window = Window::Open;
    let k = b.handle.last_commit();
    drop(c);
    Some((WindowGuard(b), k))
}

/// The temp files of an attempt, in the generation's directory, removed when dropped.
struct Temp(PathBuf);

impl Drop for Temp {
    fn drop(&mut self) {
        match std::fs::remove_file(&self.0) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => log::emit(Level::Warn, "snapshot_temp_left", &[
                ("path", &self.0.display()),
                ("error", &e),
            ]),
        }
    }
}

/// One snapshot attempt (see the module docs).
fn attempt(b: &Binding) -> Result<Outcome, SnapshotFailure> {
    let snapper = &b.handle.snapshots.snapper;
    let transport = driver().transport(&b.url);
    let remote = Remote {
        transport: transport.as_ref(),
        snapper,
        cluster_id: &b.handle.cluster_id,
        incarnation: b.handle.incarnation(),
    };
    // Before the checkpoint, the copy and the compression: an idle binding learns of a history
    // change or a misroute only when it sends, so the HEAD finds out first, and a probe makes the
    // replicator re-key, reset or block meanwhile.
    if let Err(e) = remote.head() {
        if matches!(e, SnapshotFailure::TokenChanged) {
            driver().sync(b.id, None, false)?;
        }
        return Err(e);
    }
    let conn = match Private::open(&b.path) {
        Ok(conn) => conn,
        // No connection of the host holds the binding: it is retiring, nothing to snapshot.
        Err(HostError::SqliteOpen { .. }) if b.handle.connections() == 0 => {
            return Ok(Outcome::Skipped);
        }
        Err(e) => return Err(e.into()),
    };
    // Outside the window, so the checkpoint inside it (which commits wait for) only covers the
    // frames committed in between.
    conn.checkpoint()?;
    let Some((window, k)) = open_window(b) else {
        return Ok(Outcome::Skipped);
    };
    if k == Position(0) {
        return Ok(Outcome::Skipped);
    }
    snapper.clear_receipts();
    driver().boundary(b.id, k)?;
    // A checkpoint another connection started after the window opened makes this one busy (it
    // gets no busy handler): retried briefly, it only covers frames up to `k` too.
    let mut tries: u32 = 0;
    loop {
        match conn.checkpoint()? {
            Checkpoint::Done => break,
            Checkpoint::Busy if tries < 50 => {
                tries = tries.saturating_add(1);
                std::thread::sleep(Duration::from_millis(2));
            }
            Checkpoint::Busy | Checkpoint::Pinned => return Ok(Outcome::Busy),
        }
    }
    // The read transaction sees exactly `k`. While it lasts no checkpoint writes newer frames into
    // the db file (a reader at mark 0 blocks backfill) and no closing connection checkpoints (that
    // needs an EXCLUSIVE lock), so the pages copied are those of `k`.
    conn.exec(c"BEGIN")?;
    conn.query(c"SELECT count(*) FROM sqlite_schema")?;
    drop(window);
    let pages = conn
        .query(c"PRAGMA page_count")?
        .first()
        .and_then(|n| u32::try_from(*n).ok())
        .unwrap_or(0);
    let dir = &b.handle.generation.dir;
    let image_path = Temp(dir.join("snapshot.image"));
    let mut image = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&image_path.0)
        .map_err(io("create", &image_path.0))?;
    {
        let mut out = BufWriter::new(&mut image);
        conn.copy_pages(pages, &mut out)?;
        out.flush().map_err(io("write", &image_path.0))?;
    }
    conn.exec(c"COMMIT")?;
    // F2 for BASE(k): the db file holds every commit up to `k` durably (design: Fsyncs).
    if let Err(e) = conn.sync_db() {
        b.reset(ResetKind::LocalDamage, &format_args!("snapshot: {e}"));
        return Err(SnapshotFailure::Reset);
    }
    // While the private connection is open the binding cannot retire: the log is still its own.
    base(b, k)?;
    let hold = SnapshotHold::begin();
    drop(conn);
    drop(hold);
    let Some(receipt) = snapper.wait_receipt(k) else {
        return Ok(Outcome::Skipped);
    };
    let Some(offset) = receipt.offset else {
        // Acknowledged without a receipt ending at `k`: nothing to key a snapshot by this time.
        log::emit(Level::Info, "snapshot_skipped", &[
            ("file", &b.path),
            ("position", &k.0),
            ("reason", &"acknowledged without a receipt ending there"),
        ]);
        return Ok(Outcome::Skipped);
    };
    // The snapshot never runs ahead of the durable acknowledged position: the 410 rule trusts a
    // snapshot of our lineage only at or below it (design: Reconcile).
    durable_ack(b, k, offset, receipt.log_bytes)?;
    image.rewind().map_err(io("rewind", &image_path.0))?;
    let fields = SnapshotFields {
        offset,
        epoch: receipt.epoch.unwrap_or(Epoch::ZERO),
        last: CommitId {
            lineage: b.handle.generation.lineage,
            position: k,
        },
    };
    let body_path = Temp(dir.join("snapshot.body"));
    let (len, digest) = write_snapshot(&fields, &mut image, &body_path.0)?;
    drop(image);
    drop(image_path);
    // The token as it is now: the binding may have re-keyed meanwhile.
    let published = publish(&Publish {
        remote: Remote {
            incarnation: b.handle.incarnation(),
            ..remote
        },
        request_limit: b.handle.replication().request_limit,
        offset,
        digest,
        body: &body_path.0,
        len,
    })?;
    // Its frame bytes up to `k` no longer count toward the next snapshot.
    driver().snapshotted(b.id, receipt.log_bytes)?;
    Ok(published)
}

/// Appends ACK(`k`, `offset`) and fsyncs the log, before the snapshot of `k` is published. A
/// failure resets the binding `local_damage`. The reset is checked under the log's lock
/// ([`Binding::log_unless_reset`]): an open that copies the generation after a reset finds every
/// ACK a published snapshot relies on in the copy.
fn durable_ack(
    b: &Binding,
    k: Position,
    offset: Offset,
    log_bytes: u64,
) -> Result<(), SnapshotFailure> {
    let commits = b.lock();
    let written = match b.log_unless_reset() {
        Err(LogRefused::Reset(_)) => return Err(SnapshotFailure::Reset),
        // Never lower the log's ACK: one at or past `k` only needs its fsync.
        Ok(mut log) if log.acked() >= k => log.sync(),
        Ok(mut log) => log
            .append(&RecordBody::Ack {
                acked: k,
                acked_offset: offset,
                log_bytes,
            })
            .and_then(|_position| log.sync()),
        Err(LogRefused::Poisoned(_)) => {
            drop(commits);
            b.reset(
                ResetKind::Internal,
                &"snapshot: the log's lock was poisoned",
            );
            return Err(SnapshotFailure::Reset);
        }
    };
    if let Err(e) = written {
        drop(commits);
        b.reset(
            ResetKind::LocalDamage,
            &format_args!("snapshot: the ACK record: {e}"),
        );
        return Err(SnapshotFailure::Reset);
    }
    Ok(())
}

/// Appends BASE(`k`) once the db file holds every commit up to `k` durably. A failure resets the
/// binding `local_damage`.
fn base(b: &Binding, k: Position) -> Result<(), SnapshotFailure> {
    let mut c = b.lock();
    if b.reset_kind().is_some() {
        return Err(SnapshotFailure::Reset);
    }
    if c.based >= k {
        return Ok(());
    }
    let appended = match b.log_unless_reset() {
        Ok(mut log) => log.append(&RecordBody::Base { position: k }).map(drop),
        Err(LogRefused::Reset(_)) => return Err(SnapshotFailure::Reset),
        Err(LogRefused::Poisoned(_)) => {
            drop(c);
            b.reset(
                ResetKind::Internal,
                &"snapshot: the log's lock was poisoned",
            );
            return Err(SnapshotFailure::Reset);
        }
    };
    if let Err(e) = appended {
        drop(c);
        b.reset(
            ResetKind::LocalDamage,
            &format_args!("snapshot: the BASE record: {e}"),
        );
        return Err(SnapshotFailure::Reset);
    }
    c.based = k;
    Ok(())
}

/// A writer that computes the snapshot digest and counts what goes through it.
struct Digesting<W> {
    out: W,
    digester: SnapshotDigester,
    len: u64,
}

impl<W: Write> Write for Digesting<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.out.write(buf)?;
        let written = buf.get(..n).unwrap_or_default();
        self.digester.update(written);
        self.len = self
            .len
            .saturating_add(u64::try_from(n).unwrap_or(u64::MAX));
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.out.flush()
    }
}

/// Writes the `USS3` body of the image read from `image` to `path`: its length and
/// `Stream-Snapshot-Digest`.
pub(crate) fn write_snapshot(
    fields: &SnapshotFields,
    image: &mut File,
    path: &Path,
) -> Result<(u64, SnapshotDigest), SnapshotFailure> {
    let file = File::create(path).map_err(io("create", path))?;
    let mut out = Digesting {
        out: BufWriter::new(file),
        digester: SnapshotDigester::new(CONTENT_TYPE),
        len: 0,
    };
    write_body(fields, image, &mut out)?;
    out.flush().map_err(io("write", path))?;
    Ok((out.len, out.digester.finalize()))
}

/// The stream as the snapshot thread reaches it: under the binding's cluster and token.
pub(crate) struct Remote<'a> {
    pub(crate) transport: &'a dyn Transport,
    /// Ends the retries once stopped, and sleeps between them.
    pub(crate) snapper: &'a Snapper,
    /// The deployment the binding's generation was built on.
    pub(crate) cluster_id: &'a str,
    pub(crate) incarnation: Incarnation,
}

/// A publish of a snapshot body written to a file.
pub(crate) struct Publish<'a> {
    pub(crate) remote: Remote<'a>,
    /// The replicator's request limit `L` (the latest HEAD's, a typed `413`'s, the configured).
    pub(crate) request_limit: u64,
    pub(crate) offset: Offset,
    pub(crate) digest: SnapshotDigest,
    pub(crate) body: &'a Path,
    pub(crate) len: u64,
}

/// Attempts per request before a publish fails as an outage. A parts commit is repeated for
/// [`COMMIT_DEADLINE`] instead.
const PUBLISH_ATTEMPTS: u32 = 6;

/// How long a parts commit is repeated after answers that decide nothing: copying up to 1 GiB
/// on a node whose copy slots other commits hold can take long, and giving up means staging every
/// part again on the next attempt.
const COMMIT_DEADLINE: Duration = Duration::from_secs(30 * 60);

/// The stagings a commit repeats after a `400`, before it fails.
const COMMIT_RESTAGES: u32 = 1;

/// Whether an answer decides nothing, so the request goes out again. A write's answer goes by
/// what `ursula_headers::classify` read from it ([`Answer::outcome`]):
///
/// - `Unknown` at a `408`, a `429` or a `5xx` is sent again, but at a `501`, which no repeat
///   changes. `classify` reads every `429` as `Refused` today. The `429` here keeps an untyped
///   rate limit transient whatever it reads, since a snapshot write repeats safely by its offset
///   and digest. An unknown `1xx`, `2xx` or `3xx` is no answer of Ursula's, and the snapshotter
///   decides on it: a success never counts as published without the typed snapshot.
/// - `Refused` for load or routing is sent again: a `408` cut before any of the body was
///   forwarded, a `429`, and a `5xx` whose code proves nothing applied (`NO_LEADER` and the
///   pressure codes), but `NO_COLD_STORE`, which no repeat changes. Every other refusal decides.
/// - `Applied`, `Repeat` and `Redirected` decide.
///
/// A HEAD's answer, which is no write's, goes by the same statuses alone.
pub(crate) fn transient(answer: &Answer) -> bool {
    match &answer.outcome {
        Some(WriteOutcome::Unknown) => matches!(answer.status, 408 | 429 | 500 | 502..=599),
        Some(WriteOutcome::Refused(code)) => match answer.status {
            408 | 429 => true,
            500..=599 => code != &ErrorCode::NoColdStore,
            _ => false,
        },
        Some(WriteOutcome::Applied | WriteOutcome::Repeat | WriteOutcome::Redirected) => false,
        None => matches!(answer.status, 408 | 429 | 500 | 502..=599),
    }
}

/// [`transient`] for a part: a `429` naming the stream's limit of staged parts decides, since no
/// repeat of the part lifts it. Any other `429` (a rate limit) is sent again.
pub(crate) fn transient_part(answer: &Answer) -> bool {
    !matches!(answer.typed, Ok(Typed::StagedPartsFull)) && transient(answer)
}

impl Remote<'_> {
    fn keys(&self) -> ProducerRequestHeaders {
        ProducerRequestHeaders {
            producer: None,
            precondition: Some(IncarnationPrecondition {
                incarnation: self.incarnation,
            }),
        }
    }

    /// The stream's HEAD, when it answers as the binding's cluster under the binding's token.
    pub(crate) fn head(&self) -> Result<StreamHead, SnapshotFailure> {
        let head = self.send("HEAD", &Request::new(Method::Head, Target::Stream))?;
        match &head.typed {
            Ok(Typed::Head {
                identity: Some(identity),
                ..
            }) if identity.cluster_id.as_str() != self.cluster_id => {
                Err(SnapshotFailure::WrongCluster {
                    expected: self.cluster_id.to_owned(),
                    found: identity.cluster_id.clone(),
                })
            }
            Ok(Typed::Head {
                stream: Some(head), ..
            }) if head.incarnation() == self.incarnation => Ok(*head),
            Ok(Typed::Head {
                stream: Some(_), ..
            }) => Err(SnapshotFailure::TokenChanged),
            _ => Err(SnapshotFailure::Status {
                what: "HEAD",
                status: head.status,
            }),
        }
    }

    /// Sends `request` until a definite answer: no answer and [`transient`] answers are sent
    /// again after backoff, at most [`PUBLISH_ATTEMPTS`] times.
    fn send(&self, what: &'static str, request: &Request) -> Result<Answer, SnapshotFailure> {
        self.send_until(what, request, Until::Attempts(PUBLISH_ATTEMPTS), transient)
    }

    /// Sends `request` until an answer that `retry` does not send again, or until `until` gives
    /// up: the same request every time, after backoff (at least the `Retry-After` asked for, up to
    /// a minute).
    pub(crate) fn send_until(
        &self,
        what: &'static str,
        request: &Request,
        until: Until,
        retry: fn(&Answer) -> bool,
    ) -> Result<Answer, SnapshotFailure> {
        let mut attempt = Attempt::default();
        let mut failures: u32 = 0;
        loop {
            let (cause, retry_after) = match self.transport.send(request, attempt) {
                Exchange::Answered(answer) if !retry(&answer) => return Ok(*answer),
                Exchange::Answered(answer) => {
                    attempt = Attempt::default();
                    (OutageCause::Status(answer.status), answer.retry_after)
                }
                Exchange::NoAnswer(no) => {
                    attempt = Attempt {
                        fresh_connection: true,
                        refresh_token: false,
                    };
                    (OutageCause::NoAnswer(no), None)
                }
            };
            failures = failures.saturating_add(1);
            let floor = retry_after.map_or(0, |after| {
                u64::try_from(after.as_millis())
                    .unwrap_or(u64::MAX)
                    .min(RETRY_AFTER_CAP_MS)
            });
            let wait = floor.max(backoff_ms(failures).min(BACKOFF_CAP_MS));
            let now = Instant::now();
            let next = now.checked_add(Duration::from_millis(wait)).unwrap_or(now);
            let gives_up = match until {
                Until::Attempts(attempts) => failures >= attempts,
                Until::Deadline(deadline) => next >= deadline,
            };
            if gives_up {
                return Err(SnapshotFailure::Outage {
                    what,
                    cause,
                    retry_after,
                });
            }
            if !self.snapper.sleep_until(next) {
                return Err(SnapshotFailure::Reset);
            }
        }
    }
}

/// When a request that gets no definite answer gives up.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Until {
    /// After this many attempts.
    Attempts(u32),
    /// Before an attempt that would go out after this.
    Deadline(Instant),
}

/// How a body goes up: in one request, or in parts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Upload {
    /// One `PUT` of the whole body, at most the request limit.
    Single,
    /// `count` parts of `part_bytes` (the last one shorter), then a commit.
    Parts { part_bytes: u64, count: u32 },
}

impl Upload {
    /// How a body of `len` bytes goes up under the request limit `limit` and the HEAD's snapshot
    /// limit `max_snapshot` (`None`: the stream takes no parts), or why it cannot.
    fn plan(len: u64, limit: u64, max_snapshot: Option<u64>) -> Result<Self, SizeBound> {
        if len <= limit {
            return Ok(Self::Single);
        }
        let Some(max_snapshot) = max_snapshot else {
            return Err(SizeBound::NoParts { request: limit });
        };
        if len > max_snapshot {
            return Err(SizeBound::Snapshot(max_snapshot));
        }
        let count = len
            .div_ceil(limit.max(1))
            .try_into()
            .ok()
            .filter(|count| *count <= MAX_SNAPSHOT_PARTS);
        match count {
            Some(count) => Ok(Self::Parts {
                part_bytes: limit,
                count,
            }),
            None => Err(SizeBound::Parts { part_bytes: limit }),
        }
    }
}

/// Publishes the body (step 7 of the module docs) and moves retention one snapshot behind.
pub(crate) fn publish(p: &Publish<'_>) -> Result<Outcome, SnapshotFailure> {
    let head = p.remote.head()?;
    let limit = p.request_limit.min(head.max_request_bytes());
    let upload = Upload::plan(p.len, limit, head.max_snapshot_bytes()).map_err(|bound| {
        SnapshotFailure::TooLarge {
            bytes: p.len,
            bound,
        }
    })?;
    let (what, answer) = match upload {
        Upload::Single => {
            let body = std::fs::read(p.body).map_err(io("read", p.body))?;
            let request = Request {
                keys: p.remote.keys(),
                content_type: Some(CONTENT_TYPE.to_owned()),
                snapshot_digest: Some(p.digest),
                body: Arc::from(body),
                ..Request::new(Method::Put, Target::Snapshot { offset: p.offset })
            };
            (
                "snapshot publish",
                p.remote.send("snapshot publish", &request)?,
            )
        }
        Upload::Parts { part_bytes, count } => {
            ("snapshot commit", publish_parts(p, part_bytes, count)?)
        }
    };
    match decided(p, what, upload, limit, &answer)? {
        Outcome::Published { .. } => {}
        other => return Ok(other),
    }
    // Retention trails one snapshot behind: a host that read the previous snapshot (or whose
    // generation is past it) still finds the frames after it. The snapshot is published whatever
    // the move does, and a move that fails is tried again after the next snapshot.
    let previous = head
        .snapshot()
        .map(|s| s.offset)
        .filter(|previous| *previous < p.offset && *previous > head.retained_offset());
    if let Some(previous) = previous {
        let request = Request {
            keys: p.remote.keys(),
            ..Request::new(Method::Put, Target::Retention { offset: previous })
        };
        match p.remote.send("retention move", &request) {
            // Moved, or already past it (monotonic).
            Ok(answer)
                if matches!(answer.typed, Ok(Typed::Retained { .. })) || answer.status == 409 => {}
            Ok(answer) => log::emit(Level::Warn, "retention_failed", &[
                ("offset", &previous),
                ("status", &answer.status),
            ]),
            Err(e) => log::emit(Level::Warn, "retention_failed", &[
                ("offset", &previous),
                ("reason", &e),
            ]),
        }
    }
    Ok(Outcome::Published { offset: p.offset })
}

/// What a publish's or a commit's definite answer decides (S4): published only by a `204` naming
/// our offset and digest.
fn decided(
    p: &Publish<'_>,
    what: &'static str,
    upload: Upload,
    limit: u64,
    answer: &Answer,
) -> Result<Outcome, SnapshotFailure> {
    let ours = SnapshotRef {
        offset: p.offset,
        digest: p.digest,
    };
    let too_large = |bound| SnapshotFailure::TooLarge {
        bytes: p.len,
        bound,
    };
    match &answer.typed {
        // A `409` naming our offset and digest as the latest is ours, published by an earlier
        // attempt: checked before any other conflict.
        Ok(Typed::Snapshot(
            SnapshotPublishAnswer::Published { snapshot, .. }
            | SnapshotPublishAnswer::Conflict { latest: snapshot },
        )) if *snapshot == ours => Ok(Outcome::Published { offset: p.offset }),
        Ok(Typed::Snapshot(conflict @ SnapshotPublishAnswer::Conflict { latest }))
            if conflict.supersedes(p.offset) =>
        {
            Ok(Outcome::Superseded {
                offset: latest.offset,
            })
        }
        Ok(Typed::Snapshot(SnapshotPublishAnswer::Conflict { latest }))
            if latest.offset == p.offset =>
        {
            Err(SnapshotFailure::Conflict { offset: p.offset })
        }
        // Below retention: retention only moves to a published snapshot, so a later one
        // superseded this one; the HEAD names it.
        Ok(Typed::Untyped) if answer.status == 410 => match p.remote.head()?.snapshot() {
            Some(latest) if latest.offset > p.offset => Ok(Outcome::Superseded {
                offset: latest.offset,
            }),
            Some(_) | None => Err(SnapshotFailure::Status { what, status: 410 }),
        },
        Ok(Typed::TooLarge(bound)) => Err(too_large(size_bound(*bound))),
        // A commit's `501`: the stream takes no parts. A single request at or under the limit
        // was not refused for its size.
        Ok(Typed::Untyped) if answer.status == 501 && upload != Upload::Single => {
            Err(too_large(SizeBound::NoParts { request: limit }))
        }
        Ok(Typed::IncarnationMismatch(_)) => Err(SnapshotFailure::TokenChanged),
        Err(_malformed) => Err(SnapshotFailure::Malformed { what }),
        Ok(_) => Err(SnapshotFailure::Status {
            what,
            status: answer.status,
        }),
    }
}

/// The bound a `413` named.
fn size_bound(limit: Option<SizeLimit>) -> SizeBound {
    match limit {
        Some(SizeLimit::Request(bytes)) => SizeBound::Request(bytes),
        Some(SizeLimit::Snapshot(bytes)) => SizeBound::Snapshot(bytes),
        None => SizeBound::Unnamed,
    }
}

/// Stages the body in `count` parts of `part_bytes` and commits them: the commit's definite
/// answer. A `400` stages every part again, under new ids, [`COMMIT_RESTAGES`] times.
fn publish_parts(p: &Publish<'_>, part_bytes: u64, count: u32) -> Result<Answer, SnapshotFailure> {
    let file = File::open(p.body).map_err(io("open", p.body))?;
    let mut restages: u32 = 0;
    loop {
        let manifest = stage_parts(p, &file, part_bytes, count).inspect_err(|_failed| {
            discard_parts(p);
        })?;
        log::emit(Level::Info, "snapshot_parts_staged", &[
            ("offset", &p.offset),
            ("parts", &count),
            ("bytes", &p.len),
        ]);
        let answer = commit_parts(p, &manifest).inspect_err(|_undecided| {
            discard_parts(p);
        })?;
        if answer.status != 400 || restages >= COMMIT_RESTAGES {
            return Ok(answer);
        }
        restages = restages.saturating_add(1);
        log::emit(Level::Warn, "snapshot_parts_restaged", &[
            ("offset", &p.offset),
            ("parts", &count),
        ]);
    }
}

/// Stages every part, read from the body file one at a time (S6): each under the id its own
/// `201` names, so a part whose answer was lost is staged again and gets a new id. The manifest
/// lists this staging's ids alone, in order.
fn stage_parts(
    p: &Publish<'_>,
    file: &File,
    part_bytes: u64,
    count: u32,
) -> Result<PartsManifest, SnapshotFailure> {
    let what = "snapshot part";
    let mut ids = Vec::with_capacity(usize::try_from(count).unwrap_or(0));
    for index in 0..count {
        // The binding retired or reset, or an import ran out of time: no part more goes out.
        if p.remote.snapper.stopped() {
            return Err(SnapshotFailure::Reset);
        }
        let at = u64::from(index).saturating_mul(part_bytes);
        let len = part_bytes.min(p.len.saturating_sub(at));
        let mut bytes = vec![0; usize::try_from(len).unwrap_or(usize::MAX)];
        file.read_exact_at(&mut bytes, at)
            .map_err(io("read", p.body))?;
        let request = Request {
            keys: p.remote.keys(),
            body: Arc::from(bytes),
            ..Request::new(Method::Post, Target::SnapshotParts { offset: p.offset })
        };
        let answer = p.remote.send_until(
            what,
            &request,
            Until::Attempts(PUBLISH_ATTEMPTS),
            transient_part,
        )?;
        match answer.typed {
            Ok(Typed::PartStaged(staged)) => ids.push(staged.part),
            Ok(Typed::IncarnationMismatch(_)) => return Err(SnapshotFailure::TokenChanged),
            Ok(Typed::TooLarge(bound)) => {
                return Err(SnapshotFailure::TooLarge {
                    bytes: p.len,
                    bound: size_bound(bound),
                });
            }
            // The stream's limit of staged parts: the caller discards them, and the next attempt
            // stages again.
            Ok(Typed::StagedPartsFull) => {
                return Err(SnapshotFailure::StagedPartsFull {
                    status: answer.status,
                });
            }
            Ok(Typed::Untyped) if answer.status == 501 => {
                return Err(SnapshotFailure::TooLarge {
                    bytes: p.len,
                    bound: SizeBound::NoParts {
                        request: part_bytes,
                    },
                });
            }
            Err(_malformed) => return Err(SnapshotFailure::Malformed { what }),
            Ok(_) => {
                return Err(SnapshotFailure::Status {
                    what,
                    status: answer.status,
                });
            }
        }
    }
    // Ids the server repeated break its own contract.
    PartsManifest::new(ids).map_err(|_repeated| SnapshotFailure::Malformed { what })
}

/// Sends the commit of `manifest` until a definite answer (S4): the identical request, keyed by
/// `(W, digest)`, after every answer that decides nothing, for up to [`COMMIT_DEADLINE`]. Its
/// answer may wait for the server to copy its parts ([`Request::answer_wait`]).
fn commit_parts(p: &Publish<'_>, manifest: &PartsManifest) -> Result<Answer, SnapshotFailure> {
    let request = Request {
        keys: p.remote.keys(),
        content_type: Some(CONTENT_TYPE.to_owned()),
        parts: Some(Box::new(PartsCommit::for_manifest(manifest, p.digest))),
        body: Arc::from(manifest.render().into_bytes()),
        ..Request::new(Method::Put, Target::Snapshot { offset: p.offset })
    };
    let now = Instant::now();
    let deadline = now.checked_add(COMMIT_DEADLINE).unwrap_or(now);
    p.remote.send_until(
        "snapshot commit",
        &request,
        Until::Deadline(deadline),
        transient,
    )
}

/// Discards the stream's staged parts (`DELETE {stream}/snapshot/parts`, under the token) after a
/// staging or a commit that failed before its answer decided it: one attempt, best effort. The
/// server sweeps what this leaves after a day, so nothing depends on it.
fn discard_parts(p: &Publish<'_>) {
    let request = Request {
        keys: p.remote.keys(),
        ..Request::new(Method::Delete, Target::StagedParts)
    };
    match p
        .remote
        .send_until("parts discard", &request, Until::Attempts(1), transient)
    {
        Ok(answer) if (200..300).contains(&answer.status) => {}
        Ok(answer) => log::emit(Level::Info, "snapshot_parts_left", &[
            ("offset", &p.offset),
            ("status", &answer.status),
        ]),
        Err(e) => log::emit(Level::Info, "snapshot_parts_left", &[
            ("offset", &p.offset),
            ("reason", &e),
        ]),
    }
}

/// The wait after a first failed attempt.
const FIRST_RETRY: Duration = Duration::from_millis(100);

/// The longest wait between failed attempts: an hour while the body is too large for a layer in
/// front of the stream, which fails the same way until the database shrinks, and five minutes
/// after any other failure, so that a snapshot follows soon after its cause goes away.
const SIZE_RETRY_MAX: Duration = Duration::from_secs(3600);
const RETRY_MAX: Duration = Duration::from_secs(300);

/// What a failed snapshot attempt failed on, for the wait before the next one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FailureKind {
    /// The body is over a size limit in front of the stream.
    Size,
    /// No answer, or a 5xx: the server or a layer in front of it is unavailable.
    Outage,
    /// Anything else.
    Other,
}

impl FailureKind {
    fn retry_max(self) -> Duration {
        match self {
            FailureKind::Size => SIZE_RETRY_MAX,
            FailureKind::Outage | FailureKind::Other => RETRY_MAX,
        }
    }

    /// What the `snapshot_failed` log calls it.
    pub(crate) fn name(self) -> &'static str {
        match self {
            FailureKind::Size => "size",
            FailureKind::Outage => "outage",
            FailureKind::Other => "other",
        }
    }
}

/// Snapshot attempts that failed in a row, since the last one that published.
///
/// Each failure doubles the wait before the next attempt, from 100 ms up to the kind's limit, give
/// or take a fifth, so the owners a cluster-wide outage failed together do not try again together.
/// An attempt does not wait once the database is below three quarters of the image that last
/// failed. After an outage, the first acknowledged commit brings the next attempt forward, once a
/// run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SnapshotFailing {
    /// How many failed in a row.
    pub(crate) count: u32,
    /// Why the last one failed.
    pub(crate) error: String,
    pub(crate) kind: FailureKind,
    /// The size, in pages, of the image the last failure took.
    pub(crate) pages: u32,
    /// The wait after the last failure, before the jitter.
    pub(crate) wait: Duration,
    /// The next attempt starts no sooner, unless the database shrank (`holds_back`).
    pub(crate) until: Instant,
    /// A commit brought the next attempt forward in this run already.
    pub(crate) brought_forward: bool,
}

impl SnapshotFailing {
    /// The run after one more failure at `now`, after `last` (`None`: the first of a run). It
    /// waits at least `retry_after`, what the stream asked for.
    pub(crate) fn failed(
        last: Option<&SnapshotFailing>,
        kind: FailureKind,
        error: String,
        pages: u32,
        retry_after: Option<Duration>,
        now: Instant,
    ) -> SnapshotFailing {
        let first = FIRST_RETRY.min(kind.retry_max());
        let (count, wait, brought_forward) = match last {
            Some(last) => (
                last.count.saturating_add(1),
                last.wait.saturating_mul(2).clamp(first, kind.retry_max()),
                last.brought_forward,
            ),
            None => (1, first, false),
        };
        let until = [Some(jittered(wait)), retry_after]
            .into_iter()
            .flatten()
            .max()
            .and_then(|after| now.checked_add(after))
            .unwrap_or(now);
        SnapshotFailing {
            count,
            error,
            kind,
            pages,
            wait,
            until,
            brought_forward,
        }
    }

    /// Whether an attempt at `now`, with the database at `pages`, waits: until `until`, unless it
    /// is below three quarters of the image that last failed.
    pub(crate) fn holds_back(&self, pages: u32, now: Instant) -> bool {
        let shrank = u64::from(pages).saturating_mul(4) < u64::from(self.pages).saturating_mul(3);
        now < self.until && !shrank
    }

    /// A commit was acknowledged at `now`: after an outage the next attempt moves to a random
    /// point within `BROUGHT_FORWARD_SPREAD`, once a run. Whether it moved.
    pub(crate) fn commit_acknowledged(&mut self, now: Instant) -> bool {
        if self.kind != FailureKind::Outage || self.brought_forward || now >= self.until {
            return false;
        }
        let spread = self
            .until
            .saturating_duration_since(now)
            .min(BROUGHT_FORWARD_SPREAD);
        self.until = now
            .checked_add(spread.mul_f64(random_fraction()))
            .unwrap_or(now);
        self.brought_forward = true;
        true
    }
}

/// How far after the commit that brings it forward an attempt may start.
const BROUGHT_FORWARD_SPREAD: Duration = Duration::from_secs(30);

/// A random number from 0 to 1, from the standard library's randomly keyed hasher.
fn random_fraction() -> f64 {
    RandomState::new().build_hasher().finish() as f64 / u64::MAX as f64
}

/// `wait` give or take a fifth.
fn jittered(wait: Duration) -> Duration {
    wait.mul_f64(0.8 + 0.4 * random_fraction())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::time::Instant;

    use ursula_headers::Offset;

    use super::BROUGHT_FORWARD_SPREAD;
    use super::BoundaryReceipt;
    use super::FailureKind;
    use super::Position;
    use super::RETRY_MAX;
    use super::SIZE_RETRY_MAX;
    use super::Snapper;
    use super::SnapshotFailing;
    use super::jittered;

    fn run(kind: FailureKind, n: usize, pages: u32, now: Instant) -> SnapshotFailing {
        let mut last: Option<SnapshotFailing> = None;
        for _ in 0..n {
            last = Some(SnapshotFailing::failed(
                last.as_ref(),
                kind,
                "e".into(),
                pages,
                None,
                now,
            ));
        }
        last.unwrap()
    }

    // Each failure doubles the wait, up to an hour for a body too large and five minutes
    // otherwise, and never less than the stream asked for.
    #[test]
    fn failures_double_the_wait_up_to_their_kinds_limit() {
        let now = Instant::now();
        assert_eq!(
            run(FailureKind::Other, 1, 0, now).wait,
            Duration::from_millis(100)
        );
        assert_eq!(
            run(FailureKind::Other, 3, 0, now).wait,
            Duration::from_millis(400)
        );
        assert_eq!(run(FailureKind::Other, 30, 0, now).wait, RETRY_MAX);
        assert_eq!(run(FailureKind::Size, 30, 0, now).wait, SIZE_RETRY_MAX);
        let asked = SnapshotFailing::failed(
            None,
            FailureKind::Outage,
            "e".into(),
            0,
            Some(Duration::from_secs(9)),
            now,
        );
        assert!(asked.until >= now + Duration::from_secs(9));
        assert_eq!(
            [FailureKind::Size, FailureKind::Outage, FailureKind::Other].map(FailureKind::name),
            ["size", "outage", "other"]
        );
    }

    #[test]
    fn a_failure_holds_the_next_attempt_back_until_its_wait_ends_or_the_database_shrinks() {
        let now = Instant::now();
        let failing = run(FailureKind::Size, 1, 400, now);
        for pages in [400, 401, 4000, 300] {
            assert!(failing.holds_back(pages, now), "{pages}");
        }
        assert!(!failing.holds_back(299, now));
        assert!(!failing.holds_back(400, failing.until));
    }

    // After an outage, the first commit acknowledged brings the next attempt forward, once a run,
    // and not after a failure about the body's size or anything else.
    #[test]
    fn a_commit_after_an_outage_brings_the_next_attempt_forward_once() {
        let now = Instant::now();
        let mut failing = run(FailureKind::Outage, 12, 0, now);
        assert!(failing.until.duration_since(now) > BROUGHT_FORWARD_SPREAD);
        assert!(failing.commit_acknowledged(now));
        assert!(failing.until.duration_since(now) <= BROUGHT_FORWARD_SPREAD);
        assert!(!failing.commit_acknowledged(now));
        let next = SnapshotFailing::failed(
            Some(&failing),
            FailureKind::Outage,
            "e".into(),
            0,
            None,
            now,
        );
        assert!(next.brought_forward);
        assert!(!run(FailureKind::Size, 1, 0, now).commit_acknowledged(now));
        assert!(!run(FailureKind::Other, 1, 0, now).commit_acknowledged(now));
    }

    #[test]
    fn waits_are_spread_by_a_fifth() {
        let wait = Duration::from_secs(10);
        let spread: Vec<Duration> = (0..200).map(|_| jittered(wait)).collect();
        assert!(
            spread
                .iter()
                .all(|d| *d >= wait.mul_f64(0.8) && *d <= wait.mul_f64(1.2))
        );
        assert!(spread.iter().any(|d| *d < wait.mul_f64(0.95)));
        assert!(spread.iter().any(|d| *d > wait.mul_f64(1.05)));
    }

    // A sleep ends at its deadline, a reschedule or the stop, never at a request, which stays for
    // the next wait.
    #[test]
    fn a_sleep_ends_at_its_deadline_a_reschedule_or_the_stop() {
        let snapper = Snapper::default();
        let later = Instant::now() + Duration::from_secs(3600);
        snapper.request();
        let started = Instant::now();
        assert!(snapper.sleep_until(started + Duration::from_millis(20)));
        assert!(started.elapsed() >= Duration::from_millis(20));
        assert!(snapper.wait());
        snapper.reschedule();
        assert!(snapper.sleep_until(later));
        snapper.stop();
        assert!(!snapper.sleep_until(later));
        assert!(!snapper.wait());
        assert!(snapper.stopped());
    }

    // A boundary's receipt waits for the thread, and a stop ends the wait.
    #[test]
    fn receipts_reach_the_waiting_attempt() {
        let snapper = std::sync::Arc::new(Snapper::default());
        let receipt = BoundaryReceipt {
            position: Position(7),
            offset: Some(Offset::from_u64(99)),
            epoch: None,
            log_bytes: 5,
        };
        let sender = std::sync::Arc::clone(&snapper);
        let other = BoundaryReceipt {
            position: Position(3),
            ..receipt
        };
        let thread = std::thread::spawn(move || {
            sender.receipt(other);
            sender.receipt(receipt);
        });
        assert_eq!(snapper.wait_receipt(Position(7)), Some(receipt));
        thread.join().unwrap();
        snapper.clear_receipts();
        snapper.stop();
        assert_eq!(snapper.wait_receipt(Position(3)), None);
    }
}
