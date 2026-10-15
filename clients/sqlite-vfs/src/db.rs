//! Bound databases ([`Binding`]) and the process-wide registry: what `ursula_open` bound, keyed
//! by the db file's path (the VFS's key) and by the canonical stream name (`ursula_open`'s).
//!
//! A binding is shared by every connection of its database in the process. Its [`Commit`] state,
//! under one mutex, is the commit path's: the write transaction's capture, the main db handles
//! whose logged commits are undecided, the last logged, confirmed and based positions, the
//! WAL-write bookkeeping and the measurements. The reset lives in the driver's
//! [`BindingHandle`], read without the mutex. After its last connection closes the binding drains
//! and retires, unless an open takes it back first ([`BindingHandle::revive`]).

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::OnceLock;
use std::sync::PoisonError;
use std::time::Duration;
use std::time::Instant;

use crate::capture::WriteTxn;
use crate::descriptors::open_in_process;
use crate::driver::BindingHandle;
use crate::driver::BindingId;
use crate::driver::Bound;
use crate::driver::OpenId;
use crate::driver::runtime::driver;
use crate::error::Error;
use crate::error::WriteRefusal;
use crate::generation::canonical_path;
use crate::host::full_pathname;
use crate::host::init_wal_format;
use crate::ids::Activation;
use crate::ids::Position;
use crate::log;
use crate::log::Level;
use crate::open::OpenSpec;
use crate::options::OpenOptions;
use crate::replicator::ResetKind;
use crate::stats::Stats;

/// How long a driver-side open may take when `ursula_open` returns at once (`timeout_ms` 0).
const DEFAULT_OPEN_MS: u64 = 30_000;

/// A database `ursula_open` bound.
#[derive(Debug)]
pub(crate) struct Binding {
    pub(crate) id: BindingId,
    pub(crate) handle: Arc<BindingHandle>,
    /// The db file, as SQLite names it.
    pub(crate) path: String,
    pub(crate) url: String,
    state: Mutex<Commit>,
    /// The crc32c of every WAL page this process wrote in the WAL's current generation, by file
    /// offset: a page read back with other bytes was lost on its way to the disk. Under a lock of
    /// its own, so WAL reads never wait behind the commit path's fsyncs. Taken after `state`.
    pub(crate) wal_written: Mutex<BTreeMap<i64, u32>>,
    /// Signalled whenever an undecided commit is decided, or the binding resets.
    decided: Condvar,
}

/// The commit path's state of a binding.
#[derive(Debug, Default)]
pub(crate) struct Commit {
    /// The write transaction's capture, from its first WAL write (admission) to the WAL write
    /// lock's release.
    pub(crate) txn: Option<WriteTxn>,
    /// The main db handle holding the WAL write lock (0: none), by address.
    pub(crate) writer: usize,
    /// The main db handle holding an EXCLUSIVE file lock (0: none): a closing connection's
    /// checkpoint, the one main-db write that takes no checkpoint lock.
    pub(crate) exclusive: usize,
    /// Open WAL handles.
    pub(crate) wal_open: usize,
    /// The checkpoint in progress (checkpoint lock held), and whether it wrote a db page yet.
    pub(crate) checkpoint: Option<(Instant, bool)>,
    /// Main db handles whose logged commit is undecided (design §4.2), by address.
    pub(crate) undecided: BTreeMap<usize, Position>,
    /// The last commit SQLite published (PHASETWO), and the last BASE record written.
    pub(crate) confirmed: Position,
    pub(crate) based: Position,
    /// The latest write refused, until a commit is logged.
    pub(crate) refused: Option<WriteRefusal>,
    pub(crate) stats: Stats,
}

impl Commit {
    /// Whether a write to the main db file is a checkpoint: under the checkpoint lock, or the
    /// closing connection's checkpoint (EXCLUSIVE file lock with the WAL still open). Anything else
    /// (a rollback journal mode, `journal_mode=MEMORY` or `OFF`) would bypass the log.
    pub(crate) fn db_write_allowed(&self) -> bool {
        self.checkpoint.is_some() || (self.exclusive != 0 && self.wal_open > 0)
    }
}

impl Binding {
    pub(crate) fn lock(&self) -> MutexGuard<'_, Commit> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Waits on [`Binding::decided`] for at most `left`.
    pub(crate) fn wait_decided<'a>(
        &self,
        state: MutexGuard<'a, Commit>,
        left: Duration,
    ) -> MutexGuard<'a, Commit> {
        self.decided
            .wait_timeout(state, left)
            .unwrap_or_else(PoisonError::into_inner)
            .0
    }

    pub(crate) fn wal_written(&self) -> MutexGuard<'_, BTreeMap<i64, u32>> {
        self.wal_written
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn notify_decided(&self) {
        self.decided.notify_all();
    }

    pub(crate) fn reset_kind(&self) -> Option<ResetKind> {
        self.handle.reset()
    }

    /// The binding's connections found it damaged: it resets (the first reset stays), the driver
    /// fails its `synced()` calls, and its statements fail from now on.
    pub(crate) fn reset(&self, kind: ResetKind, why: &dyn std::fmt::Display) {
        if self.handle.mark_reset(kind) {
            log::emit(Level::Error, "reset", &[
                ("file", &self.path),
                ("stream", &self.url),
                ("kind", &kind.kind()),
                ("reason", why),
            ]);
            if driver().reset(self.id, kind).is_err() {
                log::emit(Level::Error, "driver_gone", &[("file", &self.path)]);
            }
        }
        self.notify_decided();
    }

    /// A main db handle opened: a retiring binding is taken back. False once it retired.
    pub(crate) fn connect(&self) -> bool {
        self.handle.connect()
    }

    /// A main db handle closed: after the last one, the binding drains and retires.
    pub(crate) fn disconnect(&self) {
        if self.handle.disconnect() && driver().retire(self.id).is_err() {
            log::emit(Level::Error, "driver_gone", &[("file", &self.path)]);
        }
    }
}

#[derive(Default)]
struct Registry {
    /// By canonical stream name.
    names: HashMap<String, Arc<Binding>>,
    /// By db file path.
    paths: HashMap<String, Arc<Binding>>,
    /// Opens in progress, by canonical stream name.
    opens: HashMap<String, OpenEntry>,
    /// Streams whose open resolved and whose binding one caller is registering ([`register`]):
    /// another open of one waits on [`registered`] until it is in `names`.
    registering: HashSet<String>,
}

/// An open in progress, and who asked for it.
struct OpenEntry {
    open: OpenId,
    /// The URL it was asked with.
    url: String,
    /// Blocking `ursula_open` calls waiting for it now.
    waiting: usize,
    /// A call that returned at once asked for it: it comes back for the result, so no blocking
    /// call's timeout gives the open up.
    returned: bool,
}

/// Marks a stream registering until dropped, a panic in [`register`] included.
struct Registering<'a>(&'a str);

impl Drop for Registering<'_> {
    fn drop(&mut self) {
        registry().registering.remove(self.0);
        registered().notify_all();
    }
}

fn registry_lock() -> &'static Mutex<Registry> {
    static R: OnceLock<Mutex<Registry>> = OnceLock::new();
    R.get_or_init(Default::default)
}

fn registry() -> MutexGuard<'static, Registry> {
    registry_lock()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// Signalled when a registration ends.
fn registered() -> &'static Condvar {
    static C: OnceLock<Condvar> = OnceLock::new();
    C.get_or_init(Condvar::new)
}

/// How long an open waits for another caller's registration of the same stream: local work only.
const REGISTERING_MS: u64 = 10_000;

/// The binding of the db file `path` (as SQLite names it), for the VFS.
pub(crate) fn lookup(path: &str) -> Option<Arc<Binding>> {
    registry().paths.get(path).cloned()
}

/// The binding of `path` (any spelling of the db file's path), for the SQL functions.
pub(crate) fn bound(path: &str) -> Result<Arc<Binding>, Error> {
    let full = full_pathname(path)?;
    lookup(&full).ok_or(Error::NotBound { path: full })
}

/// The binding of driver binding `id`, for events.
pub(crate) fn by_id(id: BindingId) -> Option<Arc<Binding>> {
    registry().paths.values().find(|b| b.id == id).cloned()
}

/// What `ursula_open` came to.
#[derive(Debug)]
pub(crate) enum Opened {
    Bound(Arc<Binding>),
    /// Still opening: the request id to wait on (another `ursula_open`) or cancel.
    Pending(OpenId),
}

/// `ursula_open(url, dir, options_json, timeout_ms)`: the binding of the stream at `url`, bound
/// now or already, or the open in progress after `timeout_ms`.
pub(crate) fn open(
    url: &str,
    dir: Option<&str>,
    options: &str,
    timeout_ms: u64,
) -> Result<Opened, Error> {
    let name = canonical_path(url)
        .map_err(|e| Error::Open(Arc::new(crate::open::OpenError::InvalidUrl(e))))?;
    let options = OpenOptions::try_from(options)?;
    if let Some(token) = &options.token {
        crate::auth::set_token(Some(token));
    }
    loop {
        let open = {
            let mut reg = registry();
            // Another caller registers the binding of this stream: it is local work.
            let deadline = Instant::now().checked_add(Duration::from_millis(REGISTERING_MS));
            while reg.registering.contains(name.as_str()) {
                let left = deadline
                    .and_then(|d| d.checked_duration_since(Instant::now()))
                    .unwrap_or_default();
                if left.is_zero() {
                    return Err(Error::Open(Arc::new(crate::open::OpenError::InUse {
                        root: PathBuf::from(name.as_str()),
                    })));
                }
                reg = registered()
                    .wait_timeout(reg, left)
                    .unwrap_or_else(PoisonError::into_inner)
                    .0;
            }
            if let Some(b) = reg.names.get(name.as_str()).cloned() {
                if b.reset_kind().is_none() && b.handle.revive() {
                    handed_out(&b);
                    return Ok(Opened::Bound(b));
                }
                // Reset, or retired: a new open (refused `in_use` while old handles hold its root).
                reg.names.remove(name.as_str());
                reg.paths.remove(&b.path);
            }
            let blocking = timeout_ms > 0;
            match reg.opens.get_mut(name.as_str()) {
                Some(entry) => {
                    if blocking {
                        entry.waiting = entry.waiting.saturating_add(1);
                    } else {
                        entry.returned = true;
                    }
                    entry.open
                }
                None => {
                    let spec = OpenSpec {
                        url: url.to_owned(),
                        dir: dir.map_or_else(default_dir, PathBuf::from),
                        activation: options.activation.unwrap_or_else(process_activation),
                        max_pending_bytes: options.max_pending_bytes,
                        max_request_bytes: crate::config::max_request_bytes(),
                        timeout_ms: if timeout_ms == 0 {
                            options.timeout_ms.unwrap_or(DEFAULT_OPEN_MS)
                        } else {
                            timeout_ms
                        },
                    };
                    let open = driver().open(spec)?;
                    reg.opens.insert(name.as_str().to_owned(), OpenEntry {
                        open,
                        url: url.to_owned(),
                        waiting: usize::from(blocking),
                        returned: !blocking,
                    });
                    open
                }
            }
        };
        if let Some(opened) = take(name.as_str(), open, timeout_ms)? {
            return Ok(opened);
        }
        // Another caller took the open's result: its registration, or a new open, is next.
    }
}

/// How much longer than the open's own deadline `ursula_open` waits, so the driver's failure (the
/// last condition) is what it reports.
const DEADLINE_GRACE_MS: u64 = 1_000;

/// The outcome of open `open` of stream `name`: with `timeout_ms` 0 if it resolved, without
/// waiting; otherwise waiting for it (the open's own deadline is `timeout_ms`). Bound, it is
/// registered. The result is taken under the registry lock, which marks the stream registering,
/// so a concurrent open of it waits for the registration instead of opening again. `None` when
/// another caller took the result.
fn take(name: &str, open: OpenId, timeout_ms: u64) -> Result<Option<Opened>, Error> {
    if timeout_ms > 0 {
        let wait = Duration::from_millis(timeout_ms.saturating_add(DEADLINE_GRACE_MS));
        if !driver().wait_resolved(open, wait) {
            // Given up only when no other caller still waits for it, or comes back for it.
            let alone = {
                let mut reg = registry();
                match reg.opens.get_mut(name) {
                    Some(entry) if entry.open == open => {
                        entry.waiting = entry.waiting.saturating_sub(1);
                        let alone = entry.waiting == 0 && !entry.returned;
                        if alone {
                            reg.opens.remove(name);
                        }
                        alone
                    }
                    _ => false,
                }
            };
            if alone && driver().cancel(open).is_err() {
                log::emit(Level::Warn, "driver_gone", &[]);
            }
            return Err(Error::Open(Arc::new(crate::open::OpenError::Cancelled)));
        }
    }
    let mut reg = registry();
    let Some(outcome) = driver().try_open(open) else {
        let ours = reg.opens.get(name).is_some_and(|entry| entry.open == open);
        return Ok(ours.then_some(Opened::Pending(open)));
    };
    let url = reg
        .opens
        .remove(name)
        .map(|entry| entry.url)
        .unwrap_or_default();
    let bound = match outcome {
        Ok(bound) => bound,
        Err(e) => return Err(Error::Open(e)),
    };
    reg.registering.insert(name.to_owned());
    drop(reg);
    let _registering = Registering(name);
    register(bound, url).map(|b| Some(Opened::Bound(b)))
}

/// A database the driver bound: its empty db file gets WAL format, and it joins the registry.
fn register(bound: Bound, url: String) -> Result<Arc<Binding>, Error> {
    let Bound { binding, handle } = bound;
    let refuse = |e: Error| {
        handle.retiring();
        if driver().retire(binding).is_err() {
            log::emit(Level::Error, "driver_gone", &[("stream", &url)]);
        }
        e
    };
    let db = handle.generation.db();
    let path = full_pathname(&db.to_string_lossy()).map_err(|e| refuse(e.into()))?;
    if std::fs::metadata(&db).map_or(0, |m| m.len()) == 0 {
        init_wal_format(&path).map_err(|e| refuse(e.into()))?;
    }
    if open_in_process(&path).is_some() {
        return Err(refuse(Error::OpenOutside { path }));
    }
    let at_bind = match handle.log() {
        Ok(log) => Position(log.next_position().0.saturating_sub(1)),
        Err(_poisoned) => return Err(refuse(Error::Reset(ResetKind::Internal))),
    };
    let b = Arc::new(Binding {
        id: binding,
        handle: Arc::clone(&handle),
        path: path.clone(),
        url,
        state: Mutex::new(Commit {
            confirmed: at_bind,
            based: at_bind,
            ..Commit::default()
        }),
        wal_written: Mutex::new(BTreeMap::new()),
        decided: Condvar::new(),
    });
    let mut reg = registry();
    reg.names
        .insert(handle.name.as_str().to_owned(), Arc::clone(&b));
    reg.paths.insert(path, Arc::clone(&b));
    drop(reg);
    log::emit(Level::Info, "open", &[
        ("file", &b.path),
        ("stream", &b.url),
    ]);
    handed_out(&b);
    Ok(b)
}

/// `ursula_open` hands `b` out: unless a connection opens it soon, it retires again.
fn handed_out(b: &Binding) {
    if driver().unconnected(b.id).is_err() {
        log::emit(Level::Error, "driver_gone", &[("file", &b.path)]);
    }
}

/// `ursula_cancel(request)`: gives up an open in progress. One that bound meanwhile and was not
/// taken retires (the driver's cancel).
pub(crate) fn cancel(open: OpenId) -> Result<Option<Arc<crate::open::OpenError>>, Error> {
    if forget(open).is_none() {
        return Err(Error::NoRequest { request: open.0 });
    }
    Ok(driver().cancel_reporting(open, Duration::from_millis(CANCEL_REPORT_MS))?)
}

/// How long `ursula_cancel` waits for what the open ended with. The driver answers a cancel at
/// once unless a disk job of the open runs.
const CANCEL_REPORT_MS: u64 = 1_000;

/// Drops open `open` from the opens in progress: the URL it was asked with.
fn forget(open: OpenId) -> Option<String> {
    let mut reg = registry();
    let name = reg
        .opens
        .iter()
        .find(|(_, entry)| entry.open == open)
        .map(|(name, _)| name.clone())?;
    reg.opens.remove(&name).map(|entry| entry.url)
}

/// The default cache root (design §5.1): `<user cache>/sqlite-ursula/0.8`.
fn default_dir() -> PathBuf {
    let home = || std::env::var_os("HOME").map(PathBuf::from);
    let cache = std::env::var_os("XDG_CACHE_HOME")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            if cfg!(target_os = "macos") {
                home().map(|h| h.join("Library").join("Caches"))
            } else {
                home().map(|h| h.join(".cache"))
            }
        })
        .unwrap_or_else(std::env::temp_dir);
    cache.join("sqlite-ursula").join("0.8")
}

/// This process's activation when the open names none: random, once per process.
fn process_activation() -> Activation {
    static A: OnceLock<Activation> = OnceLock::new();
    *A.get_or_init(|| {
        let bytes = crate::ids::random16().unwrap_or_else(|_unavailable| {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos());
            (nanos ^ u128::from(std::process::id())).to_le_bytes()
        });
        Activation(bytes)
    })
}

/// A binding of a fresh generation under `dir`, for tests of the file methods. It is in no
/// registry, and the driver never heard of it: a reset only marks it.
#[cfg(test)]
pub(crate) fn for_tests(dir: &std::path::Path) -> Arc<Binding> {
    use ursula_headers::Incarnation;
    use ursula_headers::Offset;

    use crate::codec::segment::SegmentHeader;
    use crate::driver::Bind;
    use crate::generation::Root;
    use crate::ids::Lineage;
    use crate::replog::Log;

    let name = canonical_path("https://h/v1/stream/db").unwrap();
    let root = Root::lock(dir, &name).unwrap();
    let generation = root
        .build(SegmentHeader {
            lineage: Lineage([7; 16]),
            first_position: Position(1),
            supersedes: None,
            cluster_id: "cluster".into(),
            stream_path: name.as_str().to_owned(),
            incarnation: Incarnation::try_from(1 << 52).unwrap(),
            base: Position(0),
            acked: Position(0),
            acked_offset: Offset::from_u64(0),
            log_bytes: 0,
            claim: None,
        })
        .unwrap()
        .publish()
        .unwrap();
    let (log, _) = Log::open(&generation.log_dir()).unwrap();
    let path = generation.db().to_string_lossy().into_owned();
    let handle = Arc::new(BindingHandle::new(
        Bind {
            generation,
            name,
            cluster_id: "cluster".into(),
            incarnation: (1u64 << 52).to_string(),
            log,
            pending_bytes: 0,
            max_pending_bytes: 1 << 20,
            max_frame_bytes: 1 << 20,
            acked: Position(0),
        },
        Arc::new(root),
    ));
    Arc::new(Binding {
        id: BindingId(u64::MAX),
        handle,
        path,
        url: "https://h/v1/stream/db".into(),
        state: Mutex::new(Commit::default()),
        wal_written: Mutex::new(BTreeMap::new()),
        decided: Condvar::new(),
    })
}

#[cfg(test)]
mod tests {

    use super::Commit;
    use super::default_dir;

    // The cache root ends in `sqlite-ursula/0.8`, so 0.7's caches are never read.
    #[test]
    fn the_default_dir_is_versioned() {
        assert!(default_dir().ends_with(std::path::Path::new("sqlite-ursula").join("0.8")));
    }

    // A main-db write is a checkpoint's, or a closing connection's checkpoint, and nothing else.
    #[test]
    fn db_writes_only_checkpoint() {
        let mut c = Commit::default();
        assert!(!c.db_write_allowed());
        c.checkpoint = Some((std::time::Instant::now(), false));
        assert!(c.db_write_allowed());
        c.checkpoint = None;
        c.exclusive = 1;
        assert!(!c.db_write_allowed());
        c.wal_open = 1;
        assert!(c.db_write_allowed());
    }
}
