//! The per-database snapshot thread: snapshots, their read-back, and retention.

use std::sync::Condvar;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use crate::client::get_snapshot;
use crate::client::head;
use crate::client::header_offset;
use crate::client::put_idempotent;
use crate::client::recreated;
use crate::db::Db;
use crate::db::SnapshotStat;
use crate::db::lock;
use crate::error::Error;
use crate::host::Checkpoint;
use crate::host::Private;
use crate::log;
use crate::log::Level;
use crate::snapshot;

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

pub(crate) fn snapshot_loop(db: &Mutex<Db>, snapper: &Snapper) {
    // A pinned or busy checkpoint clears up quickly; a failing server may not.
    let (mut busy, mut failing) = (Duration::from_millis(10), Duration::from_millis(100));
    while snapper.wait() {
        let (backoff, cap) = match snapshot_once(db, snapper) {
            Ok(true) => {
                (busy, failing) = (Duration::from_millis(10), Duration::from_millis(100));
                let mut d = lock(db);
                d.snapshot_failures = 0;
                d.snapshot_error = None;
                drop(d);
                continue;
            }
            Ok(false) => (&mut busy, Duration::from_secs(1)),
            Err(e) => {
                let mut d = lock(db);
                d.snapshot_failures = d.snapshot_failures.saturating_add(1);
                log::emit(Level::Warn, "snapshot_failed", &[
                    ("file", &d.path),
                    ("stream", &d.url),
                    ("failures", &d.snapshot_failures),
                    ("reason", &e),
                ]);
                d.snapshot_error = Some(e.to_string());
                drop(d);
                (&mut failing, Duration::from_secs(30))
            }
        };
        if !snapper.pause(*backoff) {
            return;
        }
        *backoff = backoff.saturating_mul(2).min(cap);
        if lock(db).snapshot_due() {
            snapper.request();
        }
    }
}

/// One snapshot attempt (see the crate docs). `Ok(false)`: not possible right now (a reader pins
/// WAL frames the checkpoint needs, or another checkpoint kept it busy), try again shortly.
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
fn snapshot_once(db: &Mutex<Db>, snapper: &Snapper) -> Result<bool, Error> {
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
            if !d.snapshot_due() || d.poisoned.is_some() || stopped() {
                d.window_wanted = false;
                snapper.window_cv.notify_all();
                return Ok(true);
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
            _ => return Ok(false),
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
    // it first (a lost page read back, a failed fsync of the db file) and is poisoned.
    let synced = conn.sync_files();
    drop(conn);
    match synced {
        Err(e) if e.is_local_damage() => {
            lock(db).set_poisoned(e, Some(&format!("snapshot at {offset} not published")));
            return Ok(true);
        }
        Err(e) => return Err(e),
        Ok(()) => {}
    }
    if lock(db)
        .poisoned
        .as_ref()
        .is_some_and(Error::is_local_damage)
    {
        return Ok(true);
    }
    let body = snapshot::encode(&offset, epoch, &image).map_err(|source| Error::Snapshot {
        offset: offset.clone(),
        source,
    })?;
    // This state is a prefix of the incarnation it was attached to, not of a stream recreated at
    // the same path since: the publish and the retention move carry the incarnation, so the
    // server refuses them there (412), and this owner's commits stop now rather than at its next
    // append (which the recreated stream refuses the same way).
    let fence = |what: String, e: Error| {
        // Not `poison()`: the overlay belongs to a write transaction that may be in flight; it
        // clears it itself when it ends, and `x_write` refuses its commit (`poisoned`).
        lock(db).set_poisoned(e, Some(&what));
    };
    match put_idempotent(
        &format!("{url}/snapshot/{offset}"),
        &incarnation,
        &body,
        &stopped,
    )? {
        (200..=299, _) => {}
        (412, _) => {
            fence(
                format!("snapshot at {offset} not published"),
                recreated(&url, &incarnation),
            );
            return Ok(true);
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
                return Ok(true);
            }
            let newer = head.snapshot;
            let mut d = lock(db);
            if let Some(newer) = newer.filter(|n| *n > d.snapshot) {
                d.snapshot = newer;
                d.snapshot_published_at = Some(Instant::now());
            }
            d.log = d.log.saturating_sub(log);
            return Ok(true);
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
            return Ok(true);
        }
        match get_snapshot(&url, &incarnation, &offset, &stopped) {
            Ok(read) if read.as_deref() == Some(&body[..]) => {
                verified = true;
                break;
            }
            Ok(_) => {}
            Err(e) if e.is_recreated() => {
                fence(format!("snapshot at {offset} not read back"), e);
                return Ok(true);
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
        if d.snapshot_stats.len() < 100_000 {
            d.snapshot_stats.push(SnapshotStat {
                offset: offset.clone(),
                bytes: body.len(),
                raw: image.len(),
                copy,
                total: started.elapsed(),
            });
        }
        (previous, d.retained.clone())
    };
    // Retention trails one snapshot behind: a host that read the previous snapshot (or whose file
    // is past it) still finds the frames after it, and the newer snapshot has read back.
    if previous > retained && previous < offset {
        match put_idempotent(
            &format!("{url}/retention/{previous}"),
            &incarnation,
            &[],
            &stopped,
        )? {
            (412, _) => {
                fence(
                    format!("retention not moved to {previous}"),
                    recreated(&url, &incarnation),
                );
                return Ok(true);
            }
            (200..=299, r) => {
                let effective = header_offset(&r, "stream-retained-offset").unwrap_or(previous);
                let mut d = lock(db);
                if effective > d.retained {
                    d.retained = effective;
                }
            }
            // Already past it (another owner, or this file before a re-attach).
            (409 | 410, _) => {}
            (status, mut r) => {
                return Err(Error::Status {
                    op: "move retention",
                    url: format!("{url}/retention/{previous}"),
                    status,
                    body: r.body_mut().read_to_string().unwrap_or_default(),
                });
            }
        }
    }
    Ok(true)
}
