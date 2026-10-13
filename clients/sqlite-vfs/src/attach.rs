//! `ursula_attach`: decide whether the local files can be trusted, bring them to the stream's
//! tail (snapshot install and replay), claim the stream (unless read-only), and bind the
//! attachment.

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::fs::{self};
use std::os::unix::fs::FileExt;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use crate::claim::claim;
use crate::client::START;
use crate::client::advanced;
use crate::client::create_stream;
use crate::client::get_snapshot;
use crate::client::head_explained;
use crate::client::producer_id;
use crate::client::read_from;
use crate::client::recreated;
use crate::config::abort_in_replay;
use crate::db::Db;
use crate::db::Mode;
use crate::db::SnapshotThread;
use crate::db::WalSeen;
use crate::db::lock;
use crate::db::registry;
use crate::error::Error;
use crate::error::Fence;
use crate::error::Gone;
use crate::frame;
use crate::frame::Decoded;
use crate::frame::PAGE;
use crate::frame::Record;
use crate::guard::contain;
use crate::host::full_pathname;
use crate::host::init_wal_format;
use crate::local::Discarded;
use crate::local::Sidecar;
use crate::local::Untrusted;
use crate::local::Verdict;
use crate::local::boot_id;
use crate::local::db_fingerprint;
use crate::local::discard_local;
use crate::local::empty_again;
use crate::local::holds_data;
use crate::local::holds_no_data;
use crate::local::lock_unused;
use crate::local::open_in_process;
use crate::local::probe_unused;
use crate::local::read_sidecar;
use crate::local::remove_if_exists;
use crate::local::seal_sidecar;
use crate::local::sidecar_line;
use crate::local::stamp;
use crate::local::stream_key;
use crate::local::write_sidecar;
use crate::log;
use crate::log::Level;
use crate::snapshot;
use crate::snapshotter::Snapper;
use crate::snapshotter::snapshot_loop;
use crate::stats::Stats;
use crate::wal::WalClaim;
use crate::wal::db_len;
use crate::wal::fold_wal;
use crate::wal::page_offset;
use crate::wal::pages_in;

/// Applies records to the db file, deduplicating page writes per batch.
///
/// Runs only inside `ursula_attach`, which refuses while any connection of this process has the
/// file open (or opens one), or any descriptor of this process outside the extension is open on
/// the file, its -wal or -shm, and has stopped the previous attachment's snapshot thread, and
/// `lock_unused` keeps other processes off the file from its first write until the Applier drops.
/// So its own descriptor on the db file cannot drop a writer's POSIX locks when it closes. Outside
/// `ursula_attach`, the extension touches the db file, -wal or -shm only through SQLite's handles
/// (the private connections of `Private`).
pub(crate) struct Applier<'a> {
    pub(crate) path: String,
    /// The sidecar, its stamp, and the offset, epoch and log the replay starts from: rewritten once
    /// the WAL is folded (`file`).
    pub(crate) sidecar: String,
    pub(crate) stamp: String,
    pub(crate) from: (String, u64, u64),
    /// Offset of the snapshot installed (`START`: none).
    pub(crate) installed: String,
    /// The size of the installed snapshot's body as it came over the wire (compressed), 0 for
    /// none: the first baseline of `Db::snapshot_published_bytes`.
    pub(crate) installed_bytes: usize,
    /// Frame bytes since the latest snapshot (`Db::log`): `from`'s, plus every frame replayed;
    /// reset by an install.
    pub(crate) log: u64,
    /// Page images written (for the `URSULA_VFS_ABORT_IN_REPLAY` test hook).
    pub(crate) written: u64,
    pub(crate) file: Option<fs::File>,
    /// Final image per page of the current batch (pages past a later shrink removed).
    pub(crate) pages: BTreeMap<u32, Vec<u8>>,
    /// Smallest db size within the batch, and the size after it.
    pub(crate) min_size: Option<u32>,
    pub(crate) size: Option<u32>,
    /// Highest claimed epoch seen.
    pub(crate) epoch: u64,
    /// The db file held no data when the attach began: data found in it when the Applier opens it
    /// came from a writer outside the extension, and is never folded or replayed over.
    pub(crate) fresh: bool,
    /// The trusted local files, checked again under the lock before the first write
    /// (`Applier::file`). `None` for a path without local state.
    pub(crate) trusted: Option<Trusted<'a>>,
    /// What a failed attach undoes: the Applier records that it opened the db file to write it, and
    /// that it wrote the sidecar saying an attach is in progress (`Undo`).
    pub(crate) undo: &'a Undo,
}

/// Trusted local files as the verdict found them (`Sidecar::verdict`), checked again wherever the
/// attach could otherwise adopt what a writer outside the extension wrote since the verdict: under
/// the Applier's lock before its first write (`Applier::file`), and at the end for files the
/// attach never wrote (`attach_files`).
pub(crate) struct Trusted<'a> {
    pub(crate) sidecar: &'a Sidecar,
    pub(crate) boot: Option<&'a str>,
    pub(crate) incarnation: &'a str,
    /// The WAL's last commit frame the verdict found (`Verdict::Trust`).
    pub(crate) wal_end: u32,
}

impl Trusted<'_> {
    /// The files of `path` still get the verdict they got: otherwise a writer outside the extension
    /// changed them during the attach, and what it wrote may be in no stream.
    fn check(&self, path: &str) -> Result<(), Error> {
        match self.sidecar.verdict(path, self.boot, self.incarnation) {
            Verdict::Trust { wal_end } if wal_end == self.wal_end => Ok(()),
            _ => Err(Error::WrittenDuringAttach {
                path: path.to_owned(),
            }),
        }
    }
}

impl Applier<'_> {
    /// A failed operation on the db file.
    fn io(&self, op: &'static str, source: std::io::Error) -> Error {
        Error::Io {
            op,
            path: self.path.clone(),
            source,
        }
    }

    fn apply(&mut self, record: Record) {
        match record {
            Record::Claim { epoch, .. } => self.epoch = self.epoch.max(epoch),
            Record::Commit { size, pages } => {
                if size < self.size.unwrap_or(u32::MAX) {
                    self.pages.retain(|&p, _| p <= size);
                }
                self.min_size = Some(self.min_size.map_or(size, |m| m.min(size)));
                self.size = Some(size);
                for (pgno, data) in pages {
                    self.pages.insert(pgno, data);
                }
            }
        }
    }

    /// Opens the db file (creating it) and locks it against other processes (`lock_unused`, held
    /// until the descriptor closes), and returns it with whether it holds data. A fresh path's
    /// (`fresh`) must still hold none: data there came from a writer outside the extension since
    /// the attach began, and is refused, not replayed over. From here on the attach writes the
    /// file (`wrote`).
    fn open_locked(&self) -> Result<(fs::File, bool), Error> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.path);
        let f = f.map_err(|source| self.io("open", source))?;
        lock_unused(&self.path, &f)?;
        let existing = f
            .metadata()
            .map_err(|source| self.io("stat", source))?
            .len()
            > 0;
        if self.fresh && existing {
            return Err(Error::WrittenDuringAttach {
                path: self.path.clone(),
            });
        }
        self.undo.wrote.store(true, Ordering::Release);
        Ok((f, existing))
    }

    /// The db file, opened once and locked (`open_locked`, held on `self.file` until the Applier
    /// drops; `discard_local` hands over its locked file). Before its first write, a file with
    /// content (trusted, never opened through SQLite here) must still hold what the verdict trusted
    /// (`Trusted::check`, under the lock now), and gets its WAL folded in (`fold_wal`); then the db
    /// file is fsynced, the sidecar keeps its offset but claims no WAL frame and no fingerprint (an
    /// attach is in progress), and the WAL is deleted, all before replay writes a page. So no stale
    /// WAL sits next to pages replay moves past it (once a crash midway has dropped the lock, a
    /// plain SQLite connection opening the file would checkpoint it over them when it closes), and
    /// a recovery that dies midway leaves files the next attach trusts and replays again from the
    /// same offset (folded pages hold the state at the WAL's last commit, replayed ones later
    /// commits). A failed attach then gives that sidecar the file's fingerprint (`Undo`). A crash
    /// between that sidecar and the delete leaves a WAL with commits next to a claim of none, which
    /// the next attach refuses (`Untrusted::CommitsUnderNone`).
    pub(crate) fn file(&mut self) -> Result<&fs::File, Error> {
        if let Some(f) = self.file.take() {
            return Ok(self.file.insert(f));
        }
        let (f, existing) = self.open_locked()?;
        if existing {
            match &self.trusted {
                Some(trusted) => trusted.check(&self.path)?,
                None => {
                    return Err(Error::WrittenDuringAttach {
                        path: self.path.clone(),
                    });
                }
            }
            fold_wal(&self.path, &f)?;
            f.sync_all().map_err(|source| self.io("fsync", source))?;
            let (offset, epoch, log) = &self.from;
            let line = sidecar_line(offset, *epoch, *log, &self.stamp, WalClaim::NONE);
            self.undo.sidecar.store(true, Ordering::Release);
            write_sidecar(&self.sidecar, &line)?;
            remove_if_exists(&format!("{}-wal", self.path))?;
            remove_if_exists(&format!("{}-shm", self.path))?;
        }
        Ok(self.file.insert(f))
    }

    /// Writes the batch: truncate to its smallest size, write the final page images, set the size.
    fn flush(&mut self) -> Result<(), Error> {
        let (Some(min), Some(size)) = (self.min_size.take(), self.size) else {
            return Ok(());
        };
        let pages = std::mem::take(&mut self.pages);
        let mut written = self.written;
        let path = self.path.clone();
        let err = |source| Error::Io {
            op: "replay into",
            path: path.clone(),
            source,
        };
        let f = self.file()?;
        let len = f.metadata().map_err(err)?.len();
        if len > db_len(min) {
            f.set_len(db_len(min)).map_err(err)?;
        }
        for (pgno, data) in pages {
            f.write_all_at(&data, page_offset(pgno)).map_err(err)?;
            written = written.saturating_add(1);
            if abort_in_replay() == Some(written) {
                log::emit(Level::Error, "test_hook_abort", &[
                    ("hook", &"URSULA_VFS_ABORT_IN_REPLAY"),
                    ("file", &self.path),
                ]);
                std::process::abort();
            }
        }
        f.set_len(db_len(size)).map_err(err)?;
        self.written = written;
        Ok(())
    }

    /// Writes a snapshot's image over the db file in place (same inode, under the lock: see
    /// `lock_unused`) and continues the batch from it. The file is prepared as for any first write
    /// (`file`: no WAL left next to the image, the sidecar saying an attach is in progress). A
    /// crash midway leaves a mix of the old file's pages and the image's (a state past the
    /// sidecar's offset), which the next attach trusts and installs the snapshot over again (every
    /// page holds the state at its offset or a later one).
    pub(crate) fn install(&mut self, snap: snapshot::Snapshot) -> Result<(), Error> {
        let path = self.path.clone();
        let err = |source| Error::Io {
            op: "install snapshot into",
            path: path.clone(),
            source,
        };
        let f = self.file()?;
        f.write_all_at(&snap.image, 0).map_err(err)?;
        f.set_len(snap.image.len() as u64).map_err(err)?;
        self.pages.clear();
        self.min_size = None;
        self.size = Some((snap.image.len() / PAGE) as u32);
        self.epoch = self.epoch.max(snap.epoch);
        self.installed = snap.offset;
        self.log = 0;
        Ok(())
    }
}

/// Reads and applies frames from `pos` (a frame boundary) until the tail (`until == None`) or
/// until `pos` reaches `until`. Offsets are opaque, so `pos` only ever takes a read's
/// `Stream-Next-Offset`, once no partial frame is buffered: a frame boundary at or before
/// everything applied (replay from there is idempotent).
fn catch_up(
    url: &str,
    incarnation: &str,
    pos: &mut String,
    until: Option<&str>,
    applier: &mut Applier,
) -> Result<(), Error> {
    let (mut buf, mut at) = (Vec::new(), pos.clone());
    loop {
        if buf.is_empty() && until.is_some_and(|u| pos.as_str() >= u) {
            break;
        }
        let (bytes, next) = read_from(url, incarnation, &at)?;
        if bytes.is_empty() {
            if !buf.is_empty() || until.is_some() {
                return Err(Error::StreamEnded {
                    url: url.to_owned(),
                    at,
                    until: until.map(str::to_owned),
                });
            }
            break;
        }
        advanced(url, &at, bytes.len(), &next)?;
        buf.extend_from_slice(&bytes);
        at = next;
        let mut used = 0;
        while let Decoded::Frame { record, len } =
            frame::decode(buf.get(used..).unwrap_or_default()).map_err(|source| Error::Frame {
                url: url.to_owned(),
                after: pos.clone(),
                source,
            })?
        {
            applier.apply(record);
            // Frames lie within `buf`: no overflow.
            used = used.saturating_add(len);
        }
        buf.drain(..used);
        applier.log = applier.log.saturating_add(used as u64);
        applier.flush()?;
        if buf.is_empty() {
            pos.clone_from(&at);
        }
    }
    Ok(())
}

/// Brings the db file from `pos` to the stream's tail and claims the stream: installs the latest
/// snapshot when the file is behind it (or below the stream's retention), replays the frames after
/// it, claims, and replays up to the claim, all from the stream's `incarnation` (every request
/// carries it as a precondition: a stream deleted and recreated meanwhile fails this round with
/// [`Error::Recreated`], and `attach_files_rebuilding` rebuilds against the new one; a newer owner's
/// claim replayed after ours fails the attach). Returns the epoch claimed and the latest
/// snapshot's offset (`START` for none). Read-only (`mode`), it stops at the tail without a claim
/// and returns the highest epoch claimed up to it.
pub(crate) fn sync(
    url: &str,
    incarnation: &str,
    pos: &mut String,
    applier: &mut Applier,
    mode: Mode,
) -> Result<(u64, String), Error> {
    let head = head_explained(url)?;
    if head.incarnation.as_deref() != Some(incarnation) {
        return Err(recreated(url, incarnation));
    }
    if let Some(s) = head.snapshot.as_deref()
        && pos.as_str() < s
    {
        let Some(body) = get_snapshot(url, incarnation, s, &|| false)? else {
            return Err(Error::Gone(Gone::SnapshotSuperseded {
                offset: s.to_owned(),
            }));
        };
        let snap = snapshot::decode(&body).map_err(|source| Error::Snapshot {
            offset: s.to_owned(),
            source,
        })?;
        if snap.offset != s {
            return Err(Error::SnapshotOffset {
                at: s.to_owned(),
                reflects: snap.offset,
            });
        }
        applier.install(snap)?;
        applier.installed_bytes = body.len();
        s.clone_into(pos);
    } else if *pos != START && *pos < head.retained {
        return Err(Error::Gone(Gone::RetentionPassed {
            offset: pos.clone(),
            retained: head.retained,
        }));
    }
    // From the beginning (`START`), a stream trimmed with no snapshot visible answers 410: `Gone`.
    catch_up(url, incarnation, pos, None, applier)?;
    let snapshot = head.snapshot.unwrap_or_else(|| START.into());
    if mode == Mode::ReadOnly {
        return Ok((applier.epoch, snapshot));
    }
    // `pos` is now the tail as catch-up found it: the claim is checked from there.
    let producer = producer_id(incarnation);
    let (epoch, claimed) = claim(
        url,
        incarnation,
        &producer,
        applier.epoch.saturating_add(1),
        pos,
    )?;
    catch_up(url, incarnation, pos, Some(claimed.as_str()), applier)?;
    // The replay may run past our claim into a higher one: another owner claimed meanwhile
    // (normally after ours, as the server refuses a lower epoch from this producer; a stray claim
    // under a deleted incarnation's producer is not epoch-fenced and may precede it). Either way
    // this owner is fenced, and a snapshot it took would record an epoch below the highest
    // claimed before it.
    if applier.epoch > epoch {
        return Err(Error::Fenced(Fence::ClaimedDuringAttach {
            epoch: applier.epoch,
        }));
    }
    Ok((epoch, snapshot))
}

pub(crate) fn attach(path: &str, url: &str, mode: Mode) -> Result<String, Arc<Error>> {
    let path = full_pathname(path)?;
    let url = url.trim_end_matches('/').to_owned();
    let (previous, distrusted) = {
        let mut reg = registry();
        if reg.open.get(&path).copied().unwrap_or(0) > 0 {
            return Err(Arc::new(Error::OpenConnections { path }));
        }
        if reg.attaching.contains(&path) {
            return Err(Arc::new(Error::AttachInProgress { path }));
        }
        if !reg.locks.contains_key(&path) {
            let lock_path = format!("{path}-ursula.lock");
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&lock_path);
            let f = f.map_err(|source| Error::Io {
                op: "open",
                path: lock_path.clone(),
                source,
            })?;
            f.try_lock().map_err(|source| Error::AttachedElsewhere {
                path: path.clone(),
                lock_path,
                source,
            })?;
            reg.locks.insert(path.clone(), f);
        }
        // A poison of the attachment being replaced distrusted the files: kept until an attach
        // succeeds, so the files are refused even if their sidecar could not say so. No connection
        // is open (checked above), so no commit holds the attachment's mutex.
        if reg.dbs.get(&path).is_some_and(|db| lock(db).damaged()) {
            reg.distrusted.insert(path.clone());
        }
        let distrusted = reg.distrusted.contains(&path);
        // The path keeps its binding (refused to `x_open` meanwhile) until the outcome replaces
        // it below. Everything in between, a panic included, is contained (`attach_files`) and
        // taken as a failed attach, so this always ends there: otherwise every open of the file
        // would answer SQLITE_BUSY until the process exits.
        reg.attaching.insert(path.clone());
        (reg.snappers.remove(&path), distrusted)
    };
    // What a failure undoes (`Undo`), which depends on whether the db file held data when the
    // attach began.
    let undo = Undo {
        fresh: holds_no_data(&path),
        ..Undo::default()
    };
    let outcome = contain(
        "attach_files",
        || {
            Err(Unbound::Failed(Error::Panicked {
                entry: "attach_files",
            }))
        },
        || {
            // The previous attachment's snapshot thread may hold a private connection on the
            // file.
            let snapper = previous.map(|(snapper, thread)| {
                snapper.stop();
                if thread.join().is_err() {
                    log::emit(Level::Error, "snapshot_thread_panicked", &[("file", &path)]);
                }
                snapper
            });
            // Now nothing of the extension has the file open: a descriptor on it, its -wal, -shm or
            // -journal shows a SQLite connection outside the extension, a read-only one included,
            // or another open of the file (`open_in_process`), and another process that holds the
            // file refuses the lock attach takes to write it (`probe_unused`). Both are refused
            // before anything changes, so the path keeps its binding, like the refusals above.
            if let Some((fd, file)) = open_in_process(&path) {
                let e = Error::OpenOutsideVfs {
                    path: path.clone(),
                    fd,
                    file,
                };
                return Err(Unbound::Refused(e, snapper));
            }
            if let Err(e) = probe_unused(&path) {
                return Err(Unbound::Refused(e, snapper));
            }
            attach_files_rebuilding(&path, &url, mode, distrusted, &undo).map_err(Unbound::Failed)
        },
    );
    // Outside `contain`, so a panic midway still undoes what the attach wrote, and before the
    // registry is locked (file I/O), while `attaching` keeps opens of the path refused.
    if matches!(outcome, Err(Unbound::Failed(_))) {
        undo.run(&path);
    }
    // A refused attach leaves the previous binding as it was: its snapshot thread runs again.
    let resumed = match &outcome {
        Err(Unbound::Refused(_, Some(snapper))) => {
            let db = registry().dbs.get(&path).cloned();
            db.map(|db| {
                snapper.resume();
                if lock(&db).snapshot_wanted() {
                    snapper.request();
                }
                spawn_snapshots(&db, snapper)
            })
        }
        _ => None,
    };
    let mut reg = registry();
    reg.attaching.remove(&path);
    match outcome {
        Ok((offset, db, fence, snapper)) => {
            reg.failed.remove(&path);
            reg.distrusted.remove(&path);
            reg.dbs.insert(path.clone(), db);
            reg.fences.insert(path.clone(), fence);
            if let Some(snapper) = snapper {
                reg.snappers.insert(path, snapper);
            }
            Ok(offset)
        }
        Err(Unbound::Refused(e, _)) => {
            match resumed {
                Some(Ok(thread)) => {
                    reg.snappers.insert(path, thread);
                }
                Some(Err(spawn)) => {
                    log::emit(Level::Error, "snapshot_thread_not_resumed", &[
                        ("file", &path),
                        ("reason", &spawn),
                    ]);
                }
                None => {}
            }
            Err(Arc::new(e))
        }
        Err(Unbound::Failed(e)) => {
            // The binding no longer describes the files (they may hold anything between its state
            // and the stream's, the stream a newer claim): without it, `x_open` refuses the path
            // until an attach succeeds, whether or not it has a sidecar (`refused`).
            let e = Arc::new(e);
            reg.dbs.remove(&path);
            reg.fences.remove(&path);
            reg.failed.insert(path, Arc::clone(&e));
            Err(e)
        }
    }
}

/// What a failed attach undoes (`Undo::run`), so nothing it leaves vouches for data written to the
/// path later, and nothing that was there before it is touched.
#[derive(Default)]
pub(crate) struct Undo {
    /// The db file held no data when the attach began (missing, or empty).
    pub(crate) fresh: bool,
    /// The attach opened the db file to write it (`Applier::open_locked`), or gave it its WAL
    /// format.
    pub(crate) wrote: AtomicBool,
    /// The attach wrote a sidecar that says an attach is in progress (a frame 0 claim without a
    /// fingerprint): the first one of files without local state, or the one `Applier::file`
    /// writes before it rewrites trusted files.
    pub(crate) sidecar: AtomicBool,
}

impl Undo {
    /// After a failed attach. For a path whose db file held no data: empties the db file again when
    /// the attach wrote into it (`local::empty_again`, only while nothing else can have written it
    /// since), then removes the sidecar the attach wrote, so data found there later is refused as
    /// never replicated (`NoSidecar`). For one that held data: the sidecar the attach wrote, which
    /// says an attach is in progress, gets the file's fingerprint (`local::seal_sidecar`), so a
    /// write outside the extension before the next attach refuses the files. Files the attach
    /// refused stay as they were. What fails is logged (`attach_restore_failed`).
    fn run(&self, path: &str) {
        let failed = |e: Error| {
            log::emit(Level::Warn, "attach_restore_failed", &[
                ("file", &path),
                ("reason", &e),
            ]);
        };
        if !self.fresh {
            if self.sidecar.load(Ordering::Acquire)
                && let Err(e) = seal_sidecar(path)
            {
                failed(e);
            }
            return;
        }
        if self.wrote.load(Ordering::Acquire)
            && let Err(e) = empty_again(path)
        {
            failed(e);
        }
        if self.sidecar.load(Ordering::Acquire)
            && let Err(e) = remove_if_exists(&format!("{path}-ursula"))
        {
            failed(e);
        }
    }
}

/// How an attach that got past its first checks ended without binding the path.
enum Unbound {
    /// Refused before it changed anything (a descriptor of this process outside the extension on
    /// the file, or another process holding it): the path keeps its binding, if any, whose
    /// snapshot thread (stopped for the check) runs again.
    Refused(Error, Option<Arc<Snapper>>),
    /// Failed: the path loses its binding, and opens through the extension only once an attach
    /// succeeds (`Registry::failed`).
    Failed(Error),
}

/// Starts the snapshot thread of the attachment `db`.
fn spawn_snapshots(db: &Arc<Mutex<Db>>, snapper: &Arc<Snapper>) -> Result<SnapshotThread, Error> {
    let (db, snapper) = (Arc::clone(db), Arc::clone(snapper));
    let thread = {
        let snapper = Arc::clone(&snapper);
        std::thread::Builder::new()
            .name("ursula-snapshot".into())
            .spawn(move || snapshot_loop(&db, &snapper))
            .map_err(Error::SpawnSnapshotThread)?
    };
    Ok((snapper, thread))
}

/// A bound attachment: the offset its file reflects, its state, its fence flag (`Db::fence`), and
/// its snapshot thread (none when read-only).
type Attached = (
    String,
    Arc<Mutex<Db>>,
    Arc<AtomicBool>,
    Option<SnapshotThread>,
);

/// `attach_files`, again when the stream was deleted and recreated during it (a 412): the files,
/// stamped with the old incarnation, are then discarded and rebuilt from the new stream. Bounded,
/// so a stream recreated over and over fails the attach instead of looping.
fn attach_files_rebuilding(
    path: &str,
    url: &str,
    mode: Mode,
    distrusted: bool,
    undo: &Undo,
) -> Result<Attached, Error> {
    let mut tries: u64 = 0;
    loop {
        match attach_files(path, url, mode, distrusted, undo) {
            Err(e) if e.is_recreated() && tries < 3 => {
                tries = tries.saturating_add(1);
                log::emit(Level::Warn, "attach_rebuilding", &[
                    ("file", &path),
                    ("stream", &url),
                    ("reason", &e),
                ]);
            }
            outcome => return outcome,
        }
    }
}

/// Decides whether the local files can be trusted (or discards them, or refuses them), brings them
/// to the stream's tail and claims it unless read-only (`sync`), then builds the new attachment for
/// `attach` to bind. Files a poison in this process distrusted (`distrusted`) are refused whatever
/// their sidecar says. A read-only attachment gets no snapshot thread: it never writes the stream.
fn attach_files(
    path: &str,
    url: &str,
    mode: Mode,
    distrusted: bool,
    undo: &Undo,
) -> Result<Attached, Error> {
    let started = Instant::now();
    let sidecar = format!("{path}-ursula");
    let boot = boot_id();
    let boot = boot.as_deref();
    // The sidecar of a file that holds data (`None` without data; `Some(None)`: torn).
    let found = if holds_data(path) {
        // A file with content but no sidecar was never attached: its pages are not in the stream.
        let s = read_sidecar(&sidecar).map_err(|e| Error::NoSidecar {
            path: path.to_owned(),
            source: Box::new(e),
        })?;
        if let Some(k) = s.as_ref().and_then(|s| s.stream.as_deref())
            && k != stream_key(url)
        {
            return Err(Error::OtherStream {
                path: path.to_owned(),
                cached: k.to_owned(),
                wanted: stream_key(url).to_owned(),
            });
        }
        Some(s)
    } else {
        None
    };
    let head = match head_explained(url) {
        Ok(head) => head,
        // A missing stream is created for a file that holds no data, unless the attach is
        // read-only (it never writes the stream). Behind one that does, the stream was deleted (by
        // mistake, by TTL expiry, or by a fresh install that did not carry it over) and the files
        // may be the only copy left: refused, and kept.
        Err(Error::Status { status: 404, .. }) if found.is_none() && mode == Mode::Owner => {
            create_stream(url)?;
            head_explained(url)?
        }
        Err(Error::Status { status: 404, .. }) if found.is_none() => {
            return Err(Error::NoStream {
                url: url.to_owned(),
            });
        }
        Err(Error::Status { status: 404, .. }) => {
            return Err(Error::StreamMissing {
                path: path.to_owned(),
                url: url.to_owned(),
            });
        }
        Err(e) => return Err(e),
    };
    let incarnation = head.incarnation.ok_or_else(|| Error::NoIncarnation {
        url: url.to_owned(),
    })?;
    // A path without data: anything found in the db file later came from outside (`Applier::fresh`).
    let fresh = found.is_none();
    // Trusted local files (and their WAL's last commit frame), or files discarded and locked for the
    // rebuild (`Sidecar::verdict`). Files that may hold data in no stream are refused and kept.
    let (mut local, mut emptied) = (None, None);
    if let Some(s) = found {
        let verdict = match &s {
            _ if distrusted => Verdict::Refuse(Untrusted::Distrusted),
            Some(s) => s.verdict(path, boot, &incarnation),
            None => Verdict::Discard(Discarded::Torn),
        };
        match (verdict, s) {
            (Verdict::Trust { wal_end }, Some(s)) => local = Some((s, wal_end)),
            (Verdict::Refuse(why), _) => {
                return Err(Error::Untrusted {
                    path: path.to_owned(),
                    why,
                });
            }
            (Verdict::Discard(why), s) => {
                // Files of this stream, unless the stream lost acknowledged data: a sidecar offset
                // never exceeds an acknowledged one, so a read there answering 416 (beyond the end)
                // refuses and keeps the files, which may hold the only copy, instead of rebuilding
                // an older database. `Gone` (below retention) is fine: the rebuild starts from a
                // snapshot.
                if why.same_stream()
                    && let Some(offset) = s.as_ref().map(|s| s.offset.as_str())
                    && offset != START
                    && let Err(e) = read_from(url, &incarnation, offset)
                    && !e.is_gone()
                {
                    return Err(e);
                }
                emptied = Some(discard_local(path)?);
                log::emit(Level::Info, "local_files_discarded", &[
                    ("file", &path),
                    ("stream", &url),
                    ("why", &why),
                ]);
            }
            // A torn sidecar is discarded above: no trust without one.
            (Verdict::Trust { .. }, None) => {}
        }
    }
    // The only rollback journal an attached file can have is `init_wal_format`'s, left by a crash
    // mid-switch: the file is an empty database with or without it, but SQLite's first open would
    // roll it back, truncating whatever attach writes after it.
    remove_if_exists(&format!("{path}-journal"))?;
    if local.is_none() {
        // A path that held no data must still hold none before its WAL goes: a writer outside the
        // extension may have written it meanwhile, and its WAL may hold commits not yet in the db
        // file.
        if fresh && holds_data(path) {
            return Err(Error::WrittenDuringAttach {
                path: path.to_owned(),
            });
        }
        // The db file exists before the first sidecar, so the sidecar names it (`file=`).
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|source| Error::Io {
                op: "create",
                path: path.to_owned(),
                source,
            })?;
    }
    let initial = stamp(path, url, boot, &incarnation);
    let (from, epoch, log) = match &local {
        Some((s, _)) => (s.offset.clone(), s.epoch, s.log),
        None => {
            // Nothing local: a WAL next to an empty db file holds nothing committed. The sidecar
            // is written before anything lands in the file, so a file an attach leaves
            // half-written by a crash is known as this stream's cache, resumed or rebuilt. It
            // records no fingerprint (an attach is in progress), and a failed attach removes it
            // (`Undo`).
            remove_if_exists(&format!("{path}-wal"))?;
            remove_if_exists(&format!("{path}-shm"))?;
            undo.sidecar.store(true, Ordering::Release);
            write_sidecar(
                &sidecar,
                &sidecar_line(START, 0, 0, &initial, WalClaim::NONE),
            )?;
            (START.to_owned(), 0, 0)
        }
    };
    let mut applier = Applier {
        path: path.to_owned(),
        sidecar: sidecar.clone(),
        stamp: initial,
        from: (from.clone(), epoch, log),
        installed: START.into(),
        installed_bytes: 0,
        log,
        written: 0,
        file: emptied,
        pages: BTreeMap::new(),
        min_size: None,
        size: None,
        epoch,
        fresh,
        trusted: local.as_ref().map(|(sidecar, wal_end)| Trusted {
            sidecar,
            boot,
            incarnation: &incarnation,
            wal_end: *wal_end,
        }),
        undo,
    };
    let mut pos = from.clone();
    let mut tries: u64 = 0;
    // `Gone`: retention moved past the file (or the snapshot read was superseded) under a HEAD
    // that did not show it yet; the next round installs the newer snapshot.
    let (epoch, snapshot) = loop {
        match sync(url, &incarnation, &mut pos, &mut applier, mode) {
            Ok(r) => break r,
            Err(e) if e.is_gone() && tries < 10 => {
                tries = tries.saturating_add(1);
                log::emit(Level::Info, "attach_retrying", &[
                    ("file", &path),
                    ("stream", &url),
                    ("reason", &e),
                ]);
                std::thread::sleep(Duration::from_millis(50_u64.saturating_mul(tries)));
            }
            Err(e) => return Err(e),
        }
    };
    let (installed, installed_bytes, log) = (
        std::mem::take(&mut applier.installed),
        applier.installed_bytes,
        applier.log,
    );
    // Files this attach never wrote, so nothing checked them since the verdict: a writer outside the
    // extension may have written them meanwhile, and what it wrote may be in no stream. Data in a
    // path that held none (the stream had no page to replay) is refused, and the sidecar this
    // attach wrote goes (`Undo`), so a later attach refuses the data as never replicated rather
    // than adopt it. Trusted files must still get the verdict they got (`Trusted::check`).
    let untouched_files = applier.file.is_none();
    if untouched_files {
        if fresh && holds_data(path) {
            return Err(Error::WrittenDuringAttach {
                path: path.to_owned(),
            });
        }
        if let Some(trusted) = &applier.trusted {
            trusted.check(path)?;
        }
    }
    drop(applier);
    // Untouched trusted files keep their WAL and its claim when it names frames (the header a
    // recovery rebuilds from it is this VFS's, any other a commit outside the extension's:
    // `WalSeen`); otherwise the WAL is gone (`Applier::file`, or never there) and the db file
    // alone holds the state.
    let kept = local
        .and_then(|(s, end)| Some((s.wal?, end)))
        .filter(|(claim, _)| claim.frame > 0 && untouched_files);
    if !holds_data(path) {
        undo.wrote.store(true, Ordering::Release);
        init_wal_format(path)?;
    }
    let wal_seen = WalSeen::attached(kept);
    let (claim, untouched) = match kept {
        Some((claim, _)) => (claim, None),
        None => {
            // The db file alone must be on disk before the sidecar says so (no connection is open,
            // so this descriptor's close drops no lock). Its fingerprint, in the claim, makes any
            // later write to it outside the extension refuse the files (`Sidecar::verdict`), and
            // until the first commit poison the attachment (`Db::untouched`).
            fs::File::open(path)
                .and_then(|f| f.sync_all())
                .map_err(|source| Error::Io {
                    op: "fsync",
                    path: path.to_owned(),
                    source,
                })?;
            let fingerprint = db_fingerprint(path);
            (WalClaim::none_with(fingerprint.unwrap_or(0)), fingerprint)
        }
    };
    // Stamped again: the db file may not have existed before (`file_id`).
    let stamp = stamp(path, url, boot, &incarnation);
    write_sidecar(&sidecar, &sidecar_line(&pos, epoch, log, &stamp, claim))?;
    let pages = pages_in(fs::metadata(path).map(|m| m.len()).unwrap_or(0));
    let snapper = Arc::new(Snapper::default());
    let fence = Arc::new(AtomicBool::new(false));
    let db = Arc::new(Mutex::new(Db {
        url: url.to_owned(),
        mode,
        producer: producer_id(&incarnation),
        incarnation,
        sidecar,
        stamp,
        path: path.to_owned(),
        epoch,
        seq: 0,
        offset: pos.clone(),
        log,
        poisoned: None,
        fence: Arc::clone(&fence),
        refused: None,
        layer_refused: None,
        refused_at: None,
        overlay: BTreeMap::new(),
        wal_written: BTreeMap::new(),
        wal_seen,
        untouched,
        committed: false,
        commit_frame_no: 0,
        wal_open: 0,
        exclusive: 0,
        writer: 0,
        acked: 0,
        fault_fired: false,
        stats: Stats::default(),
        checkpoint_started: None,
        pages,
        snapshot,
        retained: START.into(),
        snapper: snapper.clone(),
        window: false,
        window_wanted: false,
        attach_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        append_retries: 0,
        snapshot_published_at: None,
        snapshot_failing: None,
        snapshot_bytes: None,
        snapshot_published_bytes: installed_bytes,
        attached_from: from,
        installed,
    }));
    let snapshots = match mode {
        Mode::Owner => Some(spawn_snapshots(&db, &snapper)?),
        Mode::ReadOnly => None,
    };
    Ok((pos, db, fence, snapshots))
}
