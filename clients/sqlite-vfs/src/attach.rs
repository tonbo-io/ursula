//! `ursula_attach`: decide whether the local files can be trusted, bring them to the stream's
//! tail (snapshot install and replay), claim the stream, and bind the attachment.

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::fs::{self};
use std::os::unix::fs::FileExt;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use crate::claim::claim;
use crate::client::START;
use crate::client::advanced;
use crate::client::create_stream;
use crate::client::get_snapshot;
use crate::client::head;
use crate::client::producer_id;
use crate::client::read_from;
use crate::client::recreated;
use crate::config::abort_in_replay;
use crate::db::Db;
use crate::db::SnapshotThread;
use crate::db::registry;
use crate::error::Error;
use crate::error::Fence;
use crate::error::Gone;
use crate::frame;
use crate::frame::Decoded;
use crate::frame::PAGE;
use crate::frame::Record;
use crate::host::full_pathname;
use crate::host::init_wal_format;
use crate::local::SIDECAR_VERSION;
use crate::local::boot_id;
use crate::local::discard_local;
use crate::local::lock_unused;
use crate::local::read_sidecar;
use crate::local::remove_if_exists;
use crate::local::sidecar_line;
use crate::local::stamp;
use crate::local::stream_key;
use crate::local::write_sidecar;
use crate::snapshot;
use crate::snapshotter::Snapper;
use crate::snapshotter::snapshot_loop;
use crate::wal::WalClaim;
use crate::wal::db_len;
use crate::wal::fold_wal;
use crate::wal::page_offset;
use crate::wal::pages_in;

/// Applies records to the db file, deduplicating page writes per batch.
///
/// Runs only inside `ursula_attach`, which refuses while any connection of this process has the
/// file open (or opens one) and has stopped the previous attachment's snapshot thread, and
/// `lock_unused` keeps other processes off the file from its first write until the Applier drops.
/// So its own descriptor on the db file cannot drop anyone else's POSIX locks when it closes.
/// Outside `ursula_attach`, the extension touches the db file, -wal or -shm only through SQLite's
/// handles (the private connections of `Private`).
pub(crate) struct Applier {
    pub(crate) path: String,
    /// The sidecar, its stamp, and the offset, epoch and log the replay starts from: rewritten once
    /// the WAL is folded (`file`).
    pub(crate) sidecar: String,
    pub(crate) stamp: String,
    pub(crate) from: (String, u64, u64),
    /// Offset of the snapshot installed (`START`: none).
    pub(crate) installed: String,
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
}

impl Applier {
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

    /// The db file, opened once and locked (`lock_unused`, held on `self.file` until the Applier
    /// drops; `discard_local` hands over its locked file). Before its first write, a file with
    /// content (trusted, never opened through SQLite here) gets its WAL folded in (`fold_wal`);
    /// then the db file is fsynced, the sidecar keeps its offset but claims no WAL frame, and the
    /// WAL is deleted, all before replay writes a page. So no stale WAL sits next to pages replay
    /// moves past it (once a crash midway has dropped the lock, a plain SQLite connection opening
    /// the file would checkpoint it over them when it closes), and a recovery that dies midway
    /// leaves files the next attach trusts and replays again from the same offset (folded pages
    /// hold the state at the WAL's last commit, replayed ones later commits). A crash between that
    /// sidecar and the delete leaves a WAL with commits next to a claim of none: a rebuild.
    pub(crate) fn file(&mut self) -> Result<&fs::File, Error> {
        if let Some(f) = self.file.take() {
            return Ok(self.file.insert(f));
        }
        let existing = fs::metadata(&self.path).is_ok_and(|m| m.len() > 0);
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.path);
        let f = f.map_err(|source| self.io("open", source))?;
        lock_unused(&self.path, &f)?;
        if existing {
            fold_wal(&self.path, &f)?;
            f.sync_all().map_err(|source| self.io("fsync", source))?;
            let (offset, epoch, log) = &self.from;
            let line = sidecar_line(offset, *epoch, *log, &self.stamp, WalClaim::NONE);
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
                eprintln!("sqlite-ursula-vfs: URSULA_VFS_ABORT_IN_REPLAY: aborting mid-replay");
                std::process::abort();
            }
        }
        f.set_len(db_len(size)).map_err(err)?;
        self.written = written;
        Ok(())
    }

    /// Writes a snapshot's image over the db file in place (same inode, under the lock: see
    /// `lock_unused`) and continues the batch from it. The old file is not folded, and its WAL is
    /// deleted first so none sits next to the new image. A crash midway leaves a mix of the old
    /// file's pages and the image's (a state past the sidecar's offset): if the sidecar claims no
    /// WAL frame the next attach trusts it and installs the snapshot again (every page holds the
    /// state at its offset or a later one), otherwise the deleted WAL is behind it: a rebuild.
    pub(crate) fn install(&mut self, snap: snapshot::Snapshot) -> Result<(), Error> {
        let path = self.path.clone();
        let err = |source| Error::Io {
            op: "install snapshot into",
            path: path.clone(),
            source,
        };
        let f = match self.file.take() {
            Some(f) => f,
            None => {
                let f = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&self.path)
                    .map_err(err)?;
                lock_unused(&self.path, &f)?;
                f
            }
        };
        remove_if_exists(&format!("{}-wal", self.path))?;
        remove_if_exists(&format!("{}-shm", self.path))?;
        f.write_all_at(&snap.image, 0).map_err(err)?;
        f.set_len(snap.image.len() as u64).map_err(err)?;
        self.file = Some(f);
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
/// snapshot's offset (`START` for none).
pub(crate) fn sync(
    url: &str,
    incarnation: &str,
    pos: &mut String,
    applier: &mut Applier,
) -> Result<(u64, String), Error> {
    let head = head(url, &|| false)?;
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
        s.clone_into(pos);
    } else if *pos != START && *pos < head.retained {
        return Err(Error::Gone(Gone::RetentionPassed {
            offset: pos.clone(),
            retained: head.retained,
        }));
    }
    // From the beginning (`START`), a stream trimmed with no snapshot visible answers 410: `Gone`.
    catch_up(url, incarnation, pos, None, applier)?;
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
    Ok((epoch, head.snapshot.unwrap_or_else(|| START.into())))
}

pub(crate) fn attach(path: &str, url: &str) -> Result<String, Arc<Error>> {
    let path = full_pathname(path)?;
    let url = url.trim_end_matches('/').to_owned();
    let previous = {
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
        // The path keeps its binding (refused to `x_open` meanwhile) until the outcome replaces
        // it below. No panic gets past `fn_attach` (extern "C" aborts), so this always ends there.
        reg.attaching.insert(path.clone());
        reg.snappers.remove(&path)
    };
    // The previous attachment's snapshot thread may hold a private connection on the file.
    if let Some((snapper, thread)) = previous {
        snapper.stop();
        if thread.join().is_err() {
            eprintln!("sqlite-ursula-vfs: {path}: the previous snapshot thread panicked");
        }
    }
    let outcome = attach_files_rebuilding(&path, &url);
    let mut reg = registry();
    reg.attaching.remove(&path);
    match outcome {
        Ok((offset, db, snapper)) => {
            reg.failed.remove(&path);
            reg.dbs.insert(path.clone(), db);
            reg.snappers.insert(path, snapper);
            Ok(offset)
        }
        Err(e) => {
            // The binding no longer describes the files (they may hold anything between its state
            // and the stream's, the stream a newer claim): without it, `x_open` refuses the path
            // while it has a sidecar (`refused`) until an attach succeeds.
            let e = Arc::new(e);
            reg.dbs.remove(&path);
            reg.failed.insert(path, Arc::clone(&e));
            Err(e)
        }
    }
}

/// `attach_files`, again when the stream was deleted and recreated during it (a 412): the files,
/// stamped with the old incarnation, are then discarded and rebuilt from the new stream. Bounded,
/// so a stream recreated over and over fails the attach instead of looping.
fn attach_files_rebuilding(
    path: &str,
    url: &str,
) -> Result<(String, Arc<Mutex<Db>>, SnapshotThread), Error> {
    let mut tries: u64 = 0;
    loop {
        match attach_files(path, url) {
            Err(e) if e.is_recreated() && tries < 3 => {
                tries = tries.saturating_add(1);
                eprintln!("sqlite-ursula-vfs: {path}: attach: {e}; rebuilding");
            }
            outcome => return outcome,
        }
    }
}

/// Decides whether the local files can be trusted (or discards them), brings them to the stream's
/// tail and claims it (`sync`), then builds the new attachment for `attach` to bind.
fn attach_files(path: &str, url: &str) -> Result<(String, Arc<Mutex<Db>>, SnapshotThread), Error> {
    let sidecar = format!("{path}-ursula");
    let boot = boot_id();
    let boot = boot.as_deref();
    // The sidecar of a file that holds data (`None` without data; `Some(None)`: torn).
    let found = if fs::metadata(path).is_ok_and(|m| m.len() > 0) {
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
    let head = match head(url, &|| false) {
        Ok(head) => head,
        // A missing stream is created for a file that holds no data. Behind one that does, the
        // stream was deleted (by mistake, by TTL expiry, or by a fresh install that did not carry
        // it over) and the files may be the only copy left: refused, and kept.
        Err(Error::Status { status: 404, .. }) if found.is_none() => {
            create_stream(url)?;
            head(url, &|| false)?
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
    let (mut local, mut emptied) = (None, None);
    if let Some(s) = found {
        match s {
            Some(s) if s.trusted(path, boot, &incarnation) => local = Some(s),
            // Written before a reboot (a power loss may have left any prefix of any write), by an
            // older version, torn, from another incarnation of the stream, for another db file,
            // or a disk image whose WAL lost frames the sidecar counts on: the stream has
            // everything committed.
            s => {
                let why: String = match s
                    .as_ref()
                    .map(|s| (s.incarnation.as_deref(), s.version, s.offset.as_str()))
                {
                    // The stream at the path is another one: nothing of the old one is wanted,
                    // whatever the new one's length.
                    Some((Some(old), _, _)) if old != incarnation => format!(
                        "a cache of stream incarnation {old}, but {url} is now incarnation \
                         {incarnation}: deleted and recreated"
                    ),
                    // The same incarnation, or an older version's sidecar (incarnation unknown:
                    // possibly the same stream), unless the stream lost acknowledged data: a
                    // sidecar offset never exceeds an acknowledged one, so a read there answering
                    // 416 (beyond the end) refuses, as for trusted files, instead of rebuilding an
                    // older database. `Gone` (below retention) is fine: the rebuild starts from a
                    // snapshot.
                    Some((old, version, offset)) => {
                        if offset != START
                            && let Err(e) = read_from(url, &incarnation, offset)
                            && !e.is_gone()
                        {
                            return Err(e);
                        }
                        if old.is_none() {
                            "an older version's sidecar, without the stream incarnation".into()
                        } else if version != SIDECAR_VERSION {
                            format!("a sidecar of format {version}, not {SIDECAR_VERSION}")
                        } else {
                            "another boot, a replaced db file, or a WAL behind the sidecar".into()
                        }
                    }
                    None => "a torn sidecar".into(),
                };
                emptied = Some(discard_local(path)?);
                eprintln!(
                    "sqlite-ursula-vfs: {path}: local files untrusted ({why}); discarded them, \
                     rebuilding from the stream"
                );
            }
        }
    }
    // The only rollback journal an attached file can have is `init_wal_format`'s, left by a crash
    // mid-switch: the file is an empty database with or without it, but SQLite's first open would
    // roll it back, truncating whatever attach writes after it.
    remove_if_exists(&format!("{path}-journal"))?;
    let initial = stamp(path, url, boot, &incarnation);
    let (from, epoch, log) = match &local {
        Some(s) => (s.offset.clone(), s.epoch, s.log),
        None => {
            // Nothing local: a WAL next to an empty db file holds nothing committed. The sidecar
            // is written before anything lands in the file, so a file an attach leaves
            // half-written is known as this stream's cache (resumed when the sidecar names it,
            // otherwise discarded and rebuilt) instead of being refused as never attached.
            remove_if_exists(&format!("{path}-wal"))?;
            remove_if_exists(&format!("{path}-shm"))?;
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
        log,
        written: 0,
        file: emptied,
        pages: BTreeMap::new(),
        min_size: None,
        size: None,
        epoch,
    };
    let mut pos = from.clone();
    let mut tries: u64 = 0;
    // `Gone`: retention moved past the file (or the snapshot read was superseded) under a HEAD
    // that did not show it yet; the next round installs the newer snapshot.
    let (epoch, snapshot) = loop {
        match sync(url, &incarnation, &mut pos, &mut applier) {
            Ok(r) => break r,
            Err(e) if e.is_gone() && tries < 10 => {
                tries = tries.saturating_add(1);
                eprintln!("sqlite-ursula-vfs: {url}: attach: {e}; retrying");
                std::thread::sleep(Duration::from_millis(50_u64.saturating_mul(tries)));
            }
            Err(e) => return Err(e),
        }
    };
    let (installed, log) = (std::mem::take(&mut applier.installed), applier.log);
    // Untouched trusted files keep their claim; otherwise the WAL is gone (`Applier::file`,
    // `install`, or never there) and the db file alone holds the state.
    let wal = local.and_then(|s| s.wal.filter(|_| applier.file.is_none()));
    drop(applier);
    if fs::metadata(path).map(|m| m.len()).unwrap_or(0) == 0 {
        init_wal_format(path)?;
    }
    // The db file alone must be on disk before the sidecar says so (no connection is open, so
    // this descriptor's close drops no lock).
    let wal = match wal {
        Some(wal) => wal,
        None => {
            fs::File::open(path)
                .and_then(|f| f.sync_all())
                .map_err(|source| Error::Io {
                    op: "fsync",
                    path: path.to_owned(),
                    source,
                })?;
            WalClaim::NONE
        }
    };
    // Stamped again: the db file may not have existed before (`file_id`).
    let stamp = stamp(path, url, boot, &incarnation);
    write_sidecar(&sidecar, &sidecar_line(&pos, epoch, log, &stamp, wal))?;
    let pages = pages_in(fs::metadata(path).map(|m| m.len()).unwrap_or(0));
    let snapper = Arc::new(Snapper::default());
    let db = Arc::new(Mutex::new(Db {
        url: url.to_owned(),
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
        overlay: BTreeMap::new(),
        wal_written: BTreeMap::new(),
        committed: false,
        commit_frame_no: 0,
        wal_open: 0,
        exclusive: 0,
        writer: 0,
        acked: 0,
        fault_fired: false,
        stats: Vec::new(),
        checkpoint_started: None,
        checkpoints: Vec::new(),
        pages,
        snapshot,
        retained: START.into(),
        snapper: snapper.clone(),
        window: false,
        window_wanted: false,
        snapshot_stats: Vec::new(),
        attached_from: from,
        installed,
    }));
    let thread = {
        let (db, snapper) = (db.clone(), snapper.clone());
        std::thread::Builder::new()
            .name("ursula-snapshot".into())
            .spawn(move || snapshot_loop(&db, &snapper))
            .map_err(Error::SpawnSnapshotThread)?
    };
    Ok((pos, db, (snapper, thread)))
}
