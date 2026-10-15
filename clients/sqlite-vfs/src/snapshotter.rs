//! The per-database snapshot thread: snapshots, their read-back, and retention.

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::hash::Hasher;
use std::sync::Condvar;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use crate::client::RESENT_UP_TO;
use crate::client::answers;
use crate::client::get_snapshot;
use crate::client::head;
use crate::client::header_offset;
use crate::client::put_idempotent;
use crate::client::recreated;
use crate::config::snapshot_retry;
use crate::db::Db;
use crate::db::lock;
use crate::error::Attempt;
use crate::error::Error;
use crate::host::Checkpoint;
use crate::host::Private;
use crate::log;
use crate::log::Level;
use crate::snapshot;
use crate::stats::SnapshotStat;
use crate::transport::http::before_connection;
use crate::transport::http::body_text;

/// Wakes an attached database's snapshot thread.
#[derive(Default)]
pub(crate) struct Snapper {
    /// (requested, stopped)
    pub(crate) state: Mutex<(bool, bool)>,
    pub(crate) cv: Condvar,
    /// With the database's mutex: a commit waits for the snapshot window to close, the snapshot
    /// for an acknowledged commit to be published.
    pub(crate) window_cv: Condvar,
}

/// Closes the snapshot window when dropped.
struct Window<'a>(&'a Mutex<Db>);

impl Drop for Window<'_> {
    fn drop(&mut self) {
        let mut d = lock(self.0);
        d.window = false;
        d.snapper.window_cv.notify_all();
    }
}

impl Snapper {
    pub(crate) fn request(&self) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).0 = true;
        self.cv.notify_one();
    }

    pub(crate) fn stopped(&self) -> bool {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).1
    }

    pub(crate) fn stop(&self) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).1 = true;
        self.cv.notify_one();
    }

    /// Undoes `stop`, for a new thread on the same attachment: an attach refused before it changed
    /// anything leaves the previous binding as it was (`attach::Unbound::Refused`).
    pub(crate) fn resume(&self) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).1 = false;
    }

    /// Waits for a request (true) or the stop (false).
    pub(crate) fn wait(&self) -> bool {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if s.1 {
                return false;
            }
            if std::mem::take(&mut s.0) {
                return true;
            }
            s = self.cv.wait(s).unwrap_or_else(|e| e.into_inner());
        }
    }

    /// Waits for a request or `deadline` (true), or the stop (false), whichever comes first.
    pub(crate) fn wait_until(&self, deadline: Instant) -> bool {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if s.1 {
                return false;
            }
            if std::mem::take(&mut s.0) {
                return true;
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return true;
            }
            s = self
                .cv
                .wait_timeout(s, left)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
    }

    /// Sleeps for `d` unless stopped first (false).
    pub(crate) fn pause(&self, d: Duration) -> bool {
        let s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let (s, _) = self
            .cv
            .wait_timeout_while(s, d, |s| !s.1)
            .unwrap_or_else(|e| e.into_inner());
        !s.1
    }
}

/// What one snapshot attempt did (`snapshot_once`).
#[derive(Clone, Copy, Debug)]
enum Outcome {
    /// Published at the window's offset and read back.
    Published,
    /// A snapshot at or past the window's offset exists already (another owner's, or this file's
    /// before a re-attach).
    Covered,
    /// Nothing to take: not due, the database poisoned, or the thread stopped.
    Skipped,
    /// The stream is another incarnation now: the owner is fenced, and nothing of it landed there.
    Fenced,
    /// A write-back to the local files may have been lost: the owner is poisoned, and nothing was
    /// published.
    Damaged,
    /// Not possible right now (a reader pins WAL frames the checkpoint needs, or another
    /// checkpoint kept it busy): try again shortly.
    Busy,
    /// A layer in front of the stream refused the publish or the retention move with 402 Payment
    /// Required (`Db::writes_refused`): no snapshot is taken until a commit is acknowledged again
    /// (`Db::snapshot_wanted`), which requests one if it is still due (nothing adds to the log
    /// meanwhile).
    Refused,
}

/// The longest wait between failed snapshot attempts (`SnapshotFailing`): an hour while the body
/// is too large for a layer in front of the stream, which fails the same way until the database
/// shrinks, and five minutes after any other failure, so that a snapshot follows soon after its
/// cause goes away.
const SIZE_RETRY_MAX: Duration = Duration::from_secs(3600);
const RETRY_MAX: Duration = Duration::from_secs(300);

/// What a failed snapshot attempt failed on, for the wait before the next one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FailureKind {
    /// The body is over a size limit in front of the stream: answered 413, or a large body failed
    /// while the stream answers (see [`FailureKind::may_be_size`]). A gateway stops reading a body
    /// at its limit and closes the connection: the client usually sees a failed write, not the
    /// 413.
    Size,
    /// No answer, or a 5xx: the server or a layer in front of it is unavailable.
    Outage,
    /// Anything else: a snapshot that did not read back as published, a local error.
    Other,
}

impl FailureKind {
    /// Whether a failure may be about the body's size, which a probe of the stream then decides
    /// (`snapshot_loop`). Only the body's publish can fail for its size, so the stream has not
    /// accepted it (`Taken::published`). The body is over `RESENT_UP_TO` (a smaller one was sent
    /// again until the retry budget ran out) and larger than any this owner published or
    /// installed (`largest_published`). And the failure is one a size limit in front of the stream
    /// causes: no answer after a connection carried the request (a layer that stops reading at
    /// its limit closes the connection), a 5xx that neither asked for a later attempt nor said it
    /// applied nothing (`Attempt::Deferred`: the server's ingress admission refuses a large body
    /// first, and a refusal for the size is a 413), or a 408. A connection that never opened (a
    /// refused connection, a host that did not resolve, a failed TLS handshake, a resolve or
    /// connect timeout) says nothing about the body.
    fn may_be_size(e: &Error, taken: &Taken, largest_published: usize) -> bool {
        let large = taken
            .body
            .is_some_and(|bytes| bytes > RESENT_UP_TO && bytes > largest_published);
        !taken.published
            && large
            && match e {
                Error::PutUnknown {
                    last: Attempt::Transport(e),
                    ..
                } => !before_connection(e),
                Error::PutUnknown {
                    last: Attempt::Status { status, .. },
                    ..
                } => *status >= 500,
                Error::SnapshotTimedOut { .. } => true,
                _ => false,
            }
    }

    /// What `e` failed on. `answered`: for a failure that may be about the size
    /// ([`FailureKind::may_be_size`]), whether the stream answered a `HEAD` right after it with a
    /// status below 500 (`client::answers`). Then the layers in front of it pass requests and the
    /// server is up, and the body is what failed. Otherwise it is an outage.
    fn of(e: &Error, answered: Option<bool>) -> FailureKind {
        match (e, answered) {
            (Error::SnapshotTooLarge { .. }, _) | (_, Some(true)) => FailureKind::Size,
            (_, Some(false)) | (Error::PutUnknown { .. } | Error::Http { .. }, _) => {
                FailureKind::Outage
            }
            (Error::Status { status, .. }, _) if *status >= 500 => FailureKind::Outage,
            _ => FailureKind::Other,
        }
    }

    fn retry_max(self) -> Duration {
        match self {
            FailureKind::Size => SIZE_RETRY_MAX,
            FailureKind::Outage | FailureKind::Other => RETRY_MAX,
        }
    }

    /// What the `snapshot_failed` log calls it (`kind`).
    fn name(self) -> &'static str {
        match self {
            FailureKind::Size => "size",
            FailureKind::Outage => "outage",
            FailureKind::Other => "other",
        }
    }
}

/// What a snapshot attempt took, as far as it got (`snapshot_once`): the image's size in pages,
/// the body's size in bytes, and whether the stream accepted the body's publish (a 2xx). What
/// fails after the publish (the read-back, the `HEAD` of a covered snapshot) is not about the
/// body's size ([`FailureKind::may_be_size`]).
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Taken {
    pub(crate) pages: Option<u32>,
    pub(crate) body: Option<usize>,
    pub(crate) published: bool,
}

/// Snapshot attempts that failed in a row, since the last one that published a snapshot at its
/// offset or found one there (`Db::snapshot_failing`). It lives in memory: a new attach starts
/// without one.
///
/// Each failure doubles the wait before the next attempt, from `snapshot_retry()` (100 ms) up to
/// [`FailureKind::retry_max`] (an hour for a body too large, five minutes otherwise), give or take a
/// fifth (`jittered`), so the owners a cluster-wide outage failed together do not try again
/// together. An attempt reads, compresses and sends the whole database, and a body over a size
/// limit fails the same way each time until the database shrinks. So an attempt does not wait once
/// the database is below three quarters of the image that last failed (after a `VACUUM`, say).
/// After an outage, the first commit acknowledged brings the next attempt forward, once a run, so
/// that a snapshot that keeps failing while commits succeed still backs off.
pub(crate) struct SnapshotFailing {
    /// How many failed in a row.
    pub(crate) count: u32,
    /// Why the last one failed.
    pub(crate) error: String,
    /// What it failed on.
    pub(crate) kind: FailureKind,
    /// The size, in pages, of the image the last failure took (of the database, when it failed
    /// before taking one).
    pub(crate) pages: u32,
    /// The wait after the last failure, before `jittered`.
    pub(crate) wait: Duration,
    /// The next attempt starts no sooner, unless the database shrank (`holds_back`).
    pub(crate) until: Instant,
    /// A commit brought the next attempt forward in this run already (`commit_acknowledged`).
    pub(crate) brought_forward: bool,
}

impl SnapshotFailing {
    /// Whether an attempt at `now` waits, with the database at `pages`: until `until`, unless it is
    /// below three quarters of the image that last failed.
    pub(crate) fn holds_back(&self, pages: u32, now: Instant) -> bool {
        let shrank = u64::from(pages).saturating_mul(4) < u64::from(self.pages).saturating_mul(3);
        now < self.until && !shrank
    }

    /// A commit was acknowledged at `now`: after an outage the stream takes writes again, so the
    /// next attempt need not wait out the rest. It moves to a random point within
    /// `BROUGHT_FORWARD_SPREAD` from now (or before the wait would end), so that the owners an
    /// outage failed together do not all try again at once. Once a run: whether it brought the
    /// attempt forward.
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

/// How far after the commit that brings it forward an attempt may start (`commit_acknowledged`).
const BROUGHT_FORWARD_SPREAD: Duration = Duration::from_secs(30);

/// A random number from 0 to 1, from the standard library's randomly keyed hasher: no dependency
/// for one.
fn random_fraction() -> f64 {
    RandomState::new().build_hasher().finish() as f64 / u64::MAX as f64
}

/// `wait` give or take a fifth.
fn jittered(wait: Duration) -> Duration {
    wait.mul_f64(0.8 + 0.4 * random_fraction())
}

/// The snapshot health `ursula_status` reports, and the wait before the next attempt, after an
/// attempt that ended at `now` having taken `taken`: a snapshot at its offset (one published, or
/// one already there) ends a run of failures, an error extends it (`SnapshotFailing`), and any
/// other outcome leaves it as it was (nothing was published, nor did anything fail).
fn record_attempt(
    d: &mut Db,
    attempt: &Result<Outcome, Error>,
    taken: Taken,
    answered: Option<bool>,
    now: Instant,
) {
    match attempt {
        Ok(Outcome::Published | Outcome::Covered) => d.snapshot_failing = None,
        Ok(
            Outcome::Skipped
            | Outcome::Fenced
            | Outcome::Damaged
            | Outcome::Busy
            | Outcome::Refused,
        ) => {}
        Err(e) => {
            let kind = FailureKind::of(e, answered);
            let first = snapshot_retry().clamp(Duration::from_millis(1), kind.retry_max());
            let (count, wait, brought_forward) = match &d.snapshot_failing {
                Some(last) => (
                    last.count.saturating_add(1),
                    last.wait.saturating_mul(2).clamp(first, kind.retry_max()),
                    last.brought_forward,
                ),
                None => (1, first, false),
            };
            // Never sooner than the server, or a layer in front of it, asked for.
            let until = [Some(jittered(wait)), e.retry_after()]
                .into_iter()
                .flatten()
                .max()
                .and_then(|after| now.checked_add(after))
                .unwrap_or(now);
            d.snapshot_failing = Some(SnapshotFailing {
                count,
                error: e.to_string(),
                kind,
                pages: taken.pages.unwrap_or(d.pages),
                wait,
                until,
                brought_forward,
            });
        }
    }
}

pub(crate) fn snapshot_loop(db: &Mutex<Db>, snapper: &Snapper) {
    // A pinned or busy checkpoint clears up quickly.
    let mut busy = Duration::from_millis(10);
    while snapper.wait() {
        // After a failure the next attempt waits (`SnapshotFailing`): a request meanwhile (a commit
        // while a snapshot is due) only checks whether the database shrank.
        loop {
            let until = {
                let d = lock(db);
                match &d.snapshot_failing {
                    Some(failing) if failing.holds_back(d.pages, Instant::now()) => failing.until,
                    _ => break,
                }
            };
            if !snapper.wait_until(until) {
                return;
            }
        }
        let mut taken = Taken::default();
        let attempt = snapshot_once(db, snapper, &mut taken);
        // After a failure that may be about the body's size, whether the stream answers a request
        // without a body (`client::answers`): asked outside the lock. Not once stopped: a
        // re-attach waits for this thread, and a stop cut the attempt short.
        let answered = match &attempt {
            Err(e) if !snapper.stopped() => {
                let (largest, url) = {
                    let d = lock(db);
                    (d.snapshot_published_bytes, d.url.clone())
                };
                FailureKind::may_be_size(e, &taken, largest).then(|| answers(&url))
            }
            Err(_) | Ok(_) => None,
        };
        let (path, url, failing, due) = {
            let mut d = lock(db);
            let now = Instant::now();
            record_attempt(&mut d, &attempt, taken, answered, now);
            let failing = d
                .snapshot_failing
                .as_ref()
                .map(|f| (f.count, f.until.saturating_duration_since(now), f.kind));
            (d.path.clone(), d.url.clone(), failing, d.snapshot_due())
        };
        match attempt {
            Ok(Outcome::Busy) => {
                if !snapper.pause(busy) {
                    return;
                }
                busy = busy.saturating_mul(2).min(Duration::from_secs(1));
                if lock(db).snapshot_due() {
                    snapper.request();
                }
            }
            Ok(
                Outcome::Published
                | Outcome::Covered
                | Outcome::Skipped
                | Outcome::Fenced
                | Outcome::Damaged
                | Outcome::Refused,
            ) => busy = Duration::from_millis(10),
            Err(e) => {
                // A failed attempt always leaves a run of failures (`record_attempt`).
                let (failures, wait, kind) = failing
                    .map_or((0, Duration::ZERO, ""), |(count, wait, kind)| {
                        (count, wait, kind.name())
                    });
                log::emit(Level::Warn, "snapshot_failed", &[
                    ("file", &path),
                    ("stream", &url),
                    ("failures", &failures),
                    ("retry_ms", &wait.as_millis()),
                    ("kind", &kind),
                    ("reason", &e),
                ]);
                // Taken once the wait ends (above), if still due then.
                if due {
                    snapper.request();
                }
            }
        }
    }
}

/// One snapshot attempt (see the crate docs), and what it did.
///
/// The image is exactly the stream's state at `offset`. The window opens once every acknowledged
/// commit is published locally (`!committed`); until then the next commit waits for it at its
/// commit point (`window_wanted`, bounded by `WINDOW_WAIT`), so a writer committing back to back
/// cannot starve it: a due snapshot opens its window within one transaction. It keeps any further commit from being
/// acknowledged (it waits at its commit point) until the read transaction has started, so the
/// checkpoint moves every WAL frame up to `offset` into the db file and the read transaction sees
/// the db file alone at `offset`. While the read transaction lasts no checkpoint can write newer
/// frames into the db file (a reader at mark 0 blocks backfill, one at a later mark caps it) and no
/// closing connection can checkpoint (that needs an EXCLUSIVE lock), so the pages copied are those
/// of `offset`. Commits wait only for the checkpoint and the start of the read transaction.
fn snapshot_once(db: &Mutex<Db>, snapper: &Snapper, taken: &mut Taken) -> Result<Outcome, Error> {
    let stopped = || snapper.stopped();
    let started = Instant::now();
    let path = lock(db).path.clone();
    let conn = Private::open(&path, false)?;
    // Closing as the last connection keeps the WAL (as the attached connections do).
    conn.persist_wal()?;
    // Backfill outside the window, so the checkpoint inside it (which commits wait for) only
    // covers the frames committed in between.
    conn.checkpoint(c"PRAGMA wal_checkpoint(PASSIVE)")?;
    let (url, incarnation, offset, epoch, pages, log) = {
        let mut d = lock(db);
        loop {
            // Not while writes are refused before they reach the stream (a refused commit, or
            // `Outcome::Refused`): see `Db::snapshot_wanted`.
            if !d.snapshot_wanted() || stopped() {
                d.window_wanted = false;
                snapper.window_cv.notify_all();
                return Ok(Outcome::Skipped);
            }
            // Not while a commit is acknowledged but unpublished, nor while another connection
            // (typically the writer's auto-checkpoint, right after its commit) holds the
            // checkpoint lock, which would make the checkpoint below busy.
            if !d.committed && d.checkpoint_started.is_none() {
                break;
            }
            // The next commit waits for the window (see `window_wanted`).
            d.window_wanted = true;
            d = snapper
                .window_cv
                .wait_timeout(d, Duration::from_millis(100))
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        d.window_wanted = false;
        d.window = true;
        (
            d.url.clone(),
            d.incarnation.clone(),
            d.offset.clone(),
            d.epoch,
            d.pages,
            d.log,
        )
    };
    taken.pages = Some(pages);
    let window = Window(db);
    // A checkpoint started by another connection after the window opened makes this one busy
    // (it gets no busy handler): retry briefly, it only covers frames up to `offset` too.
    let mut tries: u32 = 0;
    loop {
        match conn.checkpoint(c"PRAGMA wal_checkpoint(PASSIVE)")? {
            Checkpoint::Done => break,
            Checkpoint::Busy if tries < 50 => {
                tries = tries.saturating_add(1);
                std::thread::sleep(Duration::from_millis(2));
            }
            _ => return Ok(Outcome::Busy),
        }
    }
    conn.query(c"BEGIN")?;
    conn.query(c"SELECT count(*) FROM sqlite_schema")?;
    drop(window);
    let copy_started = Instant::now();
    let image = conn.read_pages(pages)?;
    conn.query(c"COMMIT")?; // ends the read transaction
    let copy = copy_started.elapsed();
    // The checkpoints above read WAL pages, and `read_pages` db pages, outside the VFS's check of
    // pages read back (`overlay_read`): a page whose write-back failed reads back older bytes, and
    // once published, the next snapshot's retention move makes the damage permanent. The commit
    // path never fsyncs the WAL, and the db file only before the WAL restarts, so such an error is
    // usually unreported yet: fsyncs after the reads report it (on Linux), unless the owner found
    // it first (a lost page read back, a failed fsync of the db file) and is poisoned (`damaged`,
    // kept even when a fence poisoned it before).
    let synced = conn.sync_files();
    drop(conn);
    match synced {
        Err(e) if e.is_local_damage() => {
            lock(db).set_poisoned(e, Some(&format!("snapshot at {offset} not published")));
            return Ok(Outcome::Damaged);
        }
        Err(e) => return Err(e),
        Ok(()) => {}
    }
    if lock(db).damaged() {
        return Ok(Outcome::Damaged);
    }
    let body = snapshot::encode(&offset, epoch, &image).map_err(|source| Error::Snapshot {
        offset: offset.clone(),
        source,
    })?;
    lock(db).snapshot_bytes = Some(body.len());
    taken.body = Some(body.len());
    // This state is a prefix of the incarnation it was attached to, not of a stream recreated at
    // the same path since: the publish and the retention move carry the incarnation, so the
    // server refuses them there (412), and this owner's commits stop now rather than at its next
    // append (which the recreated stream refuses the same way).
    let fence = |what: String, e: Error| {
        // Not `poison()`: the overlay belongs to a write transaction that may be in flight; it
        // clears it itself when it ends, and `x_write` refuses its commit (`poisoned`).
        lock(db).set_poisoned(e, Some(&what));
    };
    // A 402 in front of the stream: nothing was published or moved (`Outcome::Refused`).
    let refused = |e: Error, during: &str| match e {
        Error::Refused { refusal, .. } => {
            lock(db).writes_refused(refusal, during);
            Ok(Outcome::Refused)
        }
        e => Err(e),
    };
    let published = match put_idempotent(
        &format!("{url}/snapshot/{offset}"),
        &incarnation,
        &body,
        &stopped,
    ) {
        Ok(answer) => answer,
        Err(e) => return refused(e, "snapshot publish"),
    };
    match published {
        (200..=299, _) => taken.published = true,
        (412, _) => {
            fence(
                format!("snapshot at {offset} not published"),
                recreated(&url, &incarnation),
            );
            return Ok(Outcome::Fenced);
        }
        (409 | 410, _) => {
            // A snapshot at or past `offset` exists (another owner's, or this file's before a
            // re-attach): it covers the log counted up to the window, unless the stream is
            // another incarnation by now.
            let head = head(&url, &stopped)?;
            if head.incarnation.as_deref() != Some(incarnation.as_str()) {
                fence(
                    format!("snapshot at {offset} not published"),
                    recreated(&url, &incarnation),
                );
                return Ok(Outcome::Fenced);
            }
            let newer = head.snapshot;
            let mut d = lock(db);
            if let Some(newer) = newer.filter(|n| *n > d.snapshot) {
                d.snapshot = newer;
                d.snapshot_published_at = Some(Instant::now());
            }
            d.log = d.log.saturating_sub(log);
            return Ok(Outcome::Covered);
        }
        (413, mut r) => {
            return Err(Error::SnapshotTooLarge {
                url: format!("{url}/snapshot/{offset}"),
                bytes: body.len(),
                body: body_text(&mut r),
            });
        }
        (408, mut r) => {
            return Err(Error::SnapshotTimedOut {
                url: format!("{url}/snapshot/{offset}"),
                bytes: body.len(),
                body: body_text(&mut r),
            });
        }
        (status, mut r) => {
            return Err(Error::Status {
                op: "publish snapshot",
                url: format!("{url}/snapshot/{offset}"),
                status,
                body: r.body_mut().read_to_string().unwrap_or_default(),
            });
        }
    }
    // Nothing relies on the snapshot before it reads back intact (a follower may not show it yet).
    let mut verified = false;
    for i in 1..=20 {
        if stopped() {
            return Ok(Outcome::Skipped);
        }
        match get_snapshot(&url, &incarnation, &offset, &stopped) {
            Ok(read) if read.as_deref() == Some(&body[..]) => {
                verified = true;
                break;
            }
            Ok(_) => {}
            Err(e) if e.is_recreated() => {
                fence(format!("snapshot at {offset} not read back"), e);
                return Ok(Outcome::Fenced);
            }
            Err(e) => return Err(e),
        }
        std::thread::sleep(Duration::from_millis(25_u64.saturating_mul(i)));
    }
    if !verified {
        return Err(Error::SnapshotNotReadBack { offset });
    }
    let (previous, retained) = {
        let mut d = lock(db);
        let previous = d.snapshot.clone();
        if offset > d.snapshot {
            d.snapshot.clone_from(&offset);
        }
        d.log = d.log.saturating_sub(log);
        d.snapshot_published_at = Some(Instant::now());
        d.snapshot_published_bytes = d.snapshot_published_bytes.max(body.len());
        d.stats.snapshot(SnapshotStat {
            offset: offset.clone(),
            bytes: body.len(),
            raw: image.len(),
            copy,
            total: started.elapsed(),
        });
        (previous, d.retained.clone())
    };
    // Retention trails one snapshot behind: a host that read the previous snapshot (or whose file
    // is past it) still finds the frames after it, and the newer snapshot has read back. The
    // snapshot is published whatever the move does: a move that fails is logged
    // (`retention_failed`) and tried again after the next snapshot, and does not extend a run of
    // failed snapshots.
    if previous > retained && previous < offset {
        let target = format!("{url}/retention/{previous}");
        match put_idempotent(&target, &incarnation, &[], &stopped) {
            Ok((412, _)) => {
                fence(
                    format!("retention not moved to {previous}"),
                    recreated(&url, &incarnation),
                );
                return Ok(Outcome::Fenced);
            }
            Ok((200..=299, r)) => {
                let effective = header_offset(&r, "stream-retained-offset").unwrap_or(previous);
                let mut d = lock(db);
                if effective > d.retained {
                    d.retained = effective;
                }
            }
            // Already past it (another owner, or this file before a re-attach).
            Ok((409 | 410, _)) => {}
            Ok((status, mut r)) => retention_failed(db, &Error::Status {
                op: "move retention",
                url: target,
                status,
                body: body_text(&mut r),
            }),
            // A 402: nothing moved, and no snapshot is taken until a commit is acknowledged.
            Err(Error::Refused { refusal, .. }) => {
                lock(db).writes_refused(refusal, "retention move");
            }
            Err(e) => retention_failed(db, &e),
        }
    }
    Ok(Outcome::Published)
}

/// A retention move after a published snapshot failed with `e`: logged, and tried again after the
/// next snapshot (retention never passes it meanwhile, so nothing is lost, the log stays longer).
fn retention_failed(db: &Mutex<Db>, e: &Error) {
    let (path, url) = {
        let d = lock(db);
        (d.path.clone(), d.url.clone())
    };
    log::emit(Level::Warn, "retention_failed", &[
        ("file", &path),
        ("stream", &url),
        ("reason", e),
    ]);
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;
    use std::time::Duration;
    use std::time::Instant;

    use super::BROUGHT_FORWARD_SPREAD;
    use super::FailureKind;
    use super::Outcome;
    use super::RETRY_MAX;
    use super::SIZE_RETRY_MAX;
    use super::Snapper;
    use super::Taken;
    use super::jittered;
    use super::record_attempt;
    use crate::db::Db;
    use crate::error::Attempt;
    use crate::error::Error;

    fn failed() -> Result<Outcome, Error> {
        Err(Error::SnapshotNotReadBack { offset: "7".into() })
    }

    fn too_large() -> Result<Outcome, Error> {
        Err(Error::SnapshotTooLarge {
            url: "http://h/b/s/snapshot/7".into(),
            bytes: 40 << 20,
            body: String::new(),
        })
    }

    fn unanswered() -> Error {
        Error::PutUnknown {
            url: "http://h/b/s/snapshot/7".into(),
            last: Attempt::Transport(Box::new(ureq::Error::Io(ErrorKind::BrokenPipe.into()))),
        }
    }

    /// The image of `pages` and a body of `body` bytes.
    /// What an attempt that built a body of `body` bytes took, the stream not having accepted it.
    fn took(pages: u32, body: usize) -> Taken {
        Taken {
            pages: Some(pages),
            body: Some(body),
            published: false,
        }
    }

    // Only a snapshot at the offset (published, or one already there) ends a run of failures: an
    // attempt that published nothing (skipped, fenced, damaged local files, busy, refused) leaves
    // the run, its last error and its wait as they were.
    #[test]
    fn only_a_snapshot_at_the_offset_ends_a_run_of_failures() {
        let mut d = Db::for_tests();
        let now = Instant::now();
        record_attempt(&mut d, &failed(), Taken::default(), None, now);
        record_attempt(&mut d, &failed(), Taken::default(), None, now);
        let until = d.snapshot_failing.as_ref().map(|f| (f.count, f.until));
        assert_eq!(until.map(|(count, _)| count), Some(2));
        for outcome in [
            Outcome::Skipped,
            Outcome::Fenced,
            Outcome::Damaged,
            Outcome::Busy,
            Outcome::Refused,
        ] {
            record_attempt(&mut d, &Ok(outcome), Taken::default(), None, now);
            let failing = d.snapshot_failing.as_ref().map(|f| (f.count, f.until));
            assert_eq!(failing, until, "{outcome:?}");
        }
        record_attempt(&mut d, &Ok(Outcome::Covered), Taken::default(), None, now);
        assert!(d.snapshot_failing.is_none());
        record_attempt(&mut d, &failed(), Taken::default(), None, now);
        record_attempt(&mut d, &Ok(Outcome::Published), Taken::default(), None, now);
        assert!(d.snapshot_failing.is_none());
    }

    /// A body's publish that never got a connection: `e` before anything was sent.
    fn unconnected(e: ureq::Error) -> Error {
        Error::PutUnknown {
            url: "http://h/b/s/snapshot/7".into(),
            last: Attempt::Transport(Box::new(e)),
        }
    }

    // A failure may be about the body's size only for a publish the stream did not accept, of a
    // body over 1 MiB and larger than any this owner published or installed, when it got no answer
    // after a connection carried it, a 5xx that neither asked for a later attempt nor said it
    // applied nothing, or a 408. A connection that never opened (refused, a host that did not
    // resolve, a resolve or connect timeout) says nothing about the body, and neither does a small
    // body, which was sent again until the budget ran out, nor anything after the publish.
    #[test]
    fn a_failure_may_be_about_the_size_only_for_a_large_publish_that_went_out() {
        let large = took(400, 2 << 20);
        let unknown = |last| Error::PutUnknown {
            url: "u".into(),
            last,
        };
        let status = |status| {
            unknown(Attempt::Status {
                status,
                body: String::new(),
            })
        };
        let deferred = |retry_after| {
            unknown(Attempt::Deferred {
                status: 503,
                body: String::new(),
                retry_after,
            })
        };
        let timed_out = Error::SnapshotTimedOut {
            url: "u".into(),
            bytes: 2 << 20,
            body: String::new(),
        };
        assert!(FailureKind::may_be_size(&unanswered(), &large, 1000));
        // So may a reset that macOS reports as `EINVAL` (`client::reset_on_macos`), on every
        // system: like a broken pipe, it came on an open connection, which may have carried the
        // body.
        let einval = ureq::Error::Io(std::io::Error::from_raw_os_error(libc::EINVAL));
        assert!(FailureKind::may_be_size(
            &unknown(Attempt::Transport(Box::new(einval))),
            &large,
            1000
        ));
        assert!(FailureKind::may_be_size(&status(503), &large, 1000));
        assert!(FailureKind::may_be_size(&timed_out, &large, 1000));
        assert!(
            !FailureKind::may_be_size(&deferred(Some(Duration::from_secs(1))), &large, 0),
            "a 503 that asks for a later attempt"
        );
        assert!(
            !FailureKind::may_be_size(&deferred(None), &large, 0),
            "a 503 that says it applied nothing"
        );
        assert!(!FailureKind::may_be_size(&status(307), &large, 0));
        assert!(
            !FailureKind::may_be_size(&unanswered(), &large, 2 << 20),
            "as large as one published"
        );
        assert!(
            !FailureKind::may_be_size(&unanswered(), &took(400, 1 << 20), 0),
            "not over 1 MiB"
        );
        assert!(
            !FailureKind::may_be_size(&unanswered(), &took(400, 1000), 0),
            "small, with no baseline"
        );
        assert!(
            !FailureKind::may_be_size(&unanswered(), &Taken::default(), 0),
            "no body"
        );
        for (name, e) in [
            (
                "refused",
                ureq::Error::Io(ErrorKind::ConnectionRefused.into()),
            ),
            ("unresolved", ureq::Error::HostNotFound),
            ("failed", ureq::Error::ConnectionFailed),
            (
                "resolve timeout",
                ureq::Error::Timeout(ureq::Timeout::Resolve),
            ),
            (
                "connect timeout",
                ureq::Error::Timeout(ureq::Timeout::Connect),
            ),
        ] {
            assert!(
                !FailureKind::may_be_size(&unconnected(e), &large, 0),
                "{name}"
            );
        }
        assert!(!FailureKind::may_be_size(
            &too_large().unwrap_err(),
            &large,
            0
        ));
    }

    // Once the stream accepted the publish, what fails after it is not about the body's size: a
    // read-back answered 502 (a node restarting) or 504 (a slow read of a cold body), or the HEAD
    // of a covered snapshot. It is an outage, with no probe, however large the body.
    #[test]
    fn a_large_body_that_fails_after_its_publish_is_an_outage() {
        let published = Taken {
            published: true,
            ..took(400, 2 << 20)
        };
        let now = Instant::now();
        for status in [502, 504] {
            let read_back = Error::Status {
                op: "get snapshot",
                url: "u".into(),
                status,
                body: String::new(),
            };
            assert!(!FailureKind::may_be_size(&read_back, &published, 0));
            assert!(!FailureKind::may_be_size(
                &read_back,
                &took(400, 2 << 20),
                0
            ));
            let mut d = Db::for_tests();
            record_attempt(&mut d, &Err(read_back), published, None, now);
            assert_eq!(
                d.snapshot_failing.as_ref().map(|f| f.kind),
                Some(FailureKind::Outage),
                "{status}"
            );
        }
        assert!(!FailureKind::may_be_size(&unanswered(), &published, 0));
    }

    // A publish the server put off with a Retry-After is not attempted again sooner, whatever the
    // run's wait (100 ms here, after a first failure).
    #[test]
    fn a_failed_attempt_waits_at_least_the_retry_after_it_got() {
        let now = Instant::now();
        let mut d = Db::for_tests();
        let put_off = Error::PutUnknown {
            url: "u".into(),
            last: Attempt::Deferred {
                status: 503,
                body: String::new(),
                retry_after: Some(Duration::from_secs(30)),
            },
        };
        record_attempt(&mut d, &Err(put_off), took(400, 2 << 20), None, now);
        let failing = d.snapshot_failing.as_ref().unwrap();
        assert_eq!(failing.kind, FailureKind::Outage);
        assert!(failing.wait < Duration::from_secs(1));
        assert_eq!(failing.until.duration_since(now), Duration::from_secs(30));
    }

    // A 413 is about the body's size, and so is a failure that may be (above) when the stream
    // answers a probe right after it below 500. When it does not, that failure is an outage. Without
    // a probe, no answer or a 5xx is an outage, and so is a request that failed at the transport.
    // Anything else is neither.
    #[test]
    fn failures_are_told_apart_by_what_they_failed_on() {
        assert_eq!(
            FailureKind::of(&too_large().unwrap_err(), None),
            FailureKind::Size
        );
        assert_eq!(
            FailureKind::of(&unanswered(), Some(true)),
            FailureKind::Size
        );
        assert_eq!(
            FailureKind::of(&unanswered(), Some(false)),
            FailureKind::Outage
        );
        assert_eq!(FailureKind::of(&unanswered(), None), FailureKind::Outage);
        let http = Error::Http {
            op: "put",
            url: "u".into(),
            source: Box::new(ureq::Error::ConnectionFailed),
        };
        assert_eq!(FailureKind::of(&http, None), FailureKind::Outage);
        let status = |status| Error::Status {
            op: "get snapshot",
            url: "u".into(),
            status,
            body: String::new(),
        };
        assert_eq!(FailureKind::of(&status(500), None), FailureKind::Outage);
        assert_eq!(
            FailureKind::of(&status(408), Some(false)),
            FailureKind::Outage
        );
        assert_eq!(FailureKind::of(&status(404), None), FailureKind::Other);
        assert_eq!(
            FailureKind::of(&failed().unwrap_err(), None),
            FailureKind::Other
        );
    }

    // Each failure doubles the wait before the next attempt, from 100 ms up to an hour for a body
    // too large and five minutes otherwise, give or take a fifth, and a run that ends starts the
    // next one from 100 ms again. The run keeps the last error and the size of the image that last
    // failed, not of the database by the time it was recorded (an upload can take minutes).
    #[test]
    fn failures_double_the_wait_up_to_their_kinds_limit() {
        let mut d = Db::for_tests();
        d.pages = 999;
        let now = Instant::now();
        let mut waits = Vec::new();
        for _ in 0..18 {
            record_attempt(&mut d, &failed(), took(400, 10), None, now);
            let failing = d.snapshot_failing.as_ref().unwrap();
            let after = failing.until.duration_since(now);
            assert!(
                after >= failing.wait.mul_f64(0.8) && after <= failing.wait.mul_f64(1.2),
                "{after:?} for {:?}",
                failing.wait
            );
            waits.push(failing.wait);
        }
        assert_eq!(waits[0], Duration::from_millis(100));
        assert_eq!(waits[1], Duration::from_millis(200));
        assert_eq!(waits[11], Duration::from_millis(100 << 11));
        assert_eq!(waits[12], RETRY_MAX);
        assert_eq!(waits[17], RETRY_MAX);
        let failing = d.snapshot_failing.as_ref().unwrap();
        assert_eq!((failing.count, failing.pages), (18, 400));
        assert_eq!(failing.error, "the snapshot at 7 does not read back");
        for _ in 0..4 {
            record_attempt(&mut d, &too_large(), took(400, 40 << 20), None, now);
        }
        assert_eq!(
            d.snapshot_failing.as_ref().map(|f| f.wait),
            Some(SIZE_RETRY_MAX)
        );
        // An outage after failures about the size waits five minutes at most again.
        record_attempt(&mut d, &Err(unanswered()), took(400, 10), None, now);
        assert_eq!(d.snapshot_failing.as_ref().map(|f| f.wait), Some(RETRY_MAX));
        // Outages alone stop at five minutes too.
        let mut outages = Db::for_tests();
        for _ in 0..18 {
            record_attempt(&mut outages, &Err(unanswered()), took(400, 10), None, now);
        }
        let failing = outages.snapshot_failing.as_ref().unwrap();
        assert_eq!(
            (failing.kind, failing.wait),
            (FailureKind::Outage, RETRY_MAX)
        );
        record_attempt(&mut d, &Ok(Outcome::Published), Taken::default(), None, now);
        record_attempt(&mut d, &failed(), Taken::default(), None, now);
        let failing = d.snapshot_failing.as_ref().unwrap();
        assert_eq!(
            (failing.count, failing.wait, failing.pages),
            (1, Duration::from_millis(100), 999)
        );
    }

    // The next attempt waits for the end of the wait, unless the database shrank below three
    // quarters of the image that last failed (a VACUUM): a body over a size limit would fail the
    // same way before. A database that grew, or shrank less, waits, and so does one that was empty.
    #[test]
    fn a_failure_holds_the_next_attempt_back_until_its_wait_ends_or_the_database_shrinks() {
        let mut d = Db::for_tests();
        let now = Instant::now();
        record_attempt(&mut d, &too_large(), took(400, 40 << 20), None, now);
        let failing = d.snapshot_failing.as_ref().unwrap();
        for pages in [400, 401, 4000, 300] {
            assert!(failing.holds_back(pages, now), "{pages}");
        }
        assert!(!failing.holds_back(299, now));
        assert!(!failing.holds_back(0, now));
        assert!(!failing.holds_back(400, failing.until));
        record_attempt(&mut d, &too_large(), took(0, 10), None, now);
        assert!(d.snapshot_failing.as_ref().unwrap().holds_back(0, now));
    }

    // After an outage, the first commit acknowledged brings the next attempt forward: the stream
    // takes writes again. Once a run, so that a snapshot that keeps failing while commits succeed
    // still backs off, and not after a failure about the body's size or anything else.
    #[test]
    fn a_commit_after_an_outage_brings_the_next_attempt_forward_once() {
        let mut d = Db::for_tests();
        let now = Instant::now();
        let outage = Err(unanswered());
        record_attempt(&mut d, &outage, Taken::default(), None, now);
        let failing = d.snapshot_failing.as_mut().unwrap();
        let until = failing.until;
        assert!(failing.commit_acknowledged(now));
        assert!(failing.until >= now && failing.until <= until);
        assert!(!failing.holds_back(0, failing.until));
        assert!(!failing.commit_acknowledged(now));
        let later = now + Duration::from_secs(1);
        record_attempt(&mut d, &outage, Taken::default(), None, later);
        let failing = d.snapshot_failing.as_mut().unwrap();
        assert!(!failing.commit_acknowledged(later), "once a run");
        assert!(failing.holds_back(0, later));
        let mut d = Db::for_tests();
        record_attempt(&mut d, &too_large(), took(400, 40 << 20), None, now);
        assert!(
            !d.snapshot_failing
                .as_mut()
                .unwrap()
                .commit_acknowledged(now)
        );
        record_attempt(&mut d, &failed(), Taken::default(), None, now);
        assert!(
            !d.snapshot_failing
                .as_mut()
                .unwrap()
                .commit_acknowledged(now)
        );
    }

    // A commit brings an attempt that waits long forward to a random point within 30 s, so that
    // owners an outage failed together do not all try again at the same moment.
    #[test]
    fn a_brought_forward_attempt_is_spread_over_30_s() {
        let now = Instant::now();
        let mut starts = Vec::new();
        for _ in 0..100 {
            let mut d = Db::for_tests();
            for _ in 0..12 {
                record_attempt(&mut d, &Err(unanswered()), Taken::default(), None, now);
            }
            let failing = d.snapshot_failing.as_mut().unwrap();
            assert!(failing.until.duration_since(now) > BROUGHT_FORWARD_SPREAD);
            assert!(failing.commit_acknowledged(now));
            starts.push(failing.until.duration_since(now));
        }
        assert!(starts.iter().all(|start| *start <= BROUGHT_FORWARD_SPREAD));
        assert!(starts.iter().any(|start| *start < Duration::from_secs(10)));
        assert!(starts.iter().any(|start| *start > Duration::from_secs(20)));
    }

    // The wait is spread over a fifth on each side, so owners that failed together do not try again
    // together.
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

    // The wait between failed attempts ends at a request (to check whether the database shrank),
    // at its deadline, or at the stop.
    #[test]
    fn a_wait_until_ends_at_a_request_its_deadline_or_the_stop() {
        let snapper = Snapper::default();
        let later = Instant::now() + Duration::from_secs(3600);
        snapper.request();
        assert!(snapper.wait_until(later));
        let started = Instant::now();
        assert!(snapper.wait_until(started + Duration::from_millis(20)));
        assert!(started.elapsed() >= Duration::from_millis(20));
        snapper.stop();
        assert!(!snapper.wait_until(later));
    }
}
