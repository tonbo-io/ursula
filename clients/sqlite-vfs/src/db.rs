//! Bound databases ([`Binding`]) and the process-wide registry: what `ursula_open` bound, keyed
//! by the db file's path (the VFS's key) and by the canonical stream name (`ursula_open`'s).
//!
//! A binding is shared by every connection of its database in the process. Its [`Commit`] state,
//! under one mutex, is the commit path's: the write transaction's capture, the main db handles
//! whose logged commits are undecided, the last logged, confirmed and based positions, the
//! WAL-write bookkeeping and the measurements. The reset lives in the driver's
//! [`BindingHandle`], read without the mutex. After its last connection closes the binding drains
//! and retires, unless an open takes it back first ([`BindingHandle::revive`]). A reset binding
//! leaves the registry at the next open of its stream, which binds anew beside the connections
//! still open on it: they keep their generation's files, and the new binding works on a copy.
//!
//! The path an open hands out stays openable until a connection opens it, or until it waited for
//! one too long ([`BindingHandle::revive`]). A stream binds under one cache root at a time in the
//! process: an open under another root gives the binding up when nothing of the host holds it
//! ([`BindingHandle::give_up`]), waits for it to retire and binds anew, and is refused `in_use`
//! otherwise.
//!
//! A read-only open (design: Readers) in a process that owns the stream under the same cache root
//! gets the owner's binding, so its read-only connection sees the owner's latest commits. Nothing
//! in the VFS makes that connection read-only: a write on the owner's binding is the owning
//! process's own, so a host opens the returned path with `SQLITE_OPEN_READONLY`. Under another
//! cache root it meets the owner's binding as an owner's open would: given up when nothing of the
//! host holds it, and refused otherwise. Only a process with no binding of the stream opens a
//! reader binding, in a reader directory of its own, which later read-only opens of the process
//! share while it is bound: a reader sees the stream as of the open that bound it, and the first
//! read-only open after it retired catches up again.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Component;
use std::path::Path;
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
use crate::driver::BindingHandle;
use crate::driver::BindingId;
use crate::driver::Bound;
use crate::driver::Connected;
use crate::driver::Holder;
use crate::driver::Kept;
use crate::driver::LogPoisoned;
use crate::driver::OpenId;
use crate::driver::Role;
use crate::driver::Undone;
use crate::driver::runtime::driver;
use crate::error::Error;
use crate::error::WriteRefusal;
use crate::generation::canonical_path;
use crate::host::full_pathname;
use crate::ids::Activation;
use crate::ids::Position;
use crate::log;
use crate::log::Level;
use crate::open::OpenSpec;
use crate::options::Access;
use crate::options::OpenOptions;
use crate::replicator::ResetKind;
use crate::replog::Log;
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
    /// The cache root the binding's files are under.
    dir: CacheDir,
    state: Mutex<Commit>,
    /// The crc32c of every WAL page this process wrote in the WAL's current generation, by file
    /// offset: a page read back with other bytes was lost on its way to the disk. Under a lock of
    /// its own, so WAL reads never wait behind the commit path's fsyncs. Taken after `state`.
    pub(crate) wal_written: Mutex<BTreeMap<i64, u32>>,
    /// Signalled whenever an undecided commit is decided, or the binding resets.
    decided: Condvar,
}

/// Why [`Binding::log_unless_reset`] gave no log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum LogRefused {
    #[error("the binding reset ({0:?})")]
    Reset(ResetKind),
    #[error(transparent)]
    Poisoned(#[from] LogPoisoned),
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
    /// Main db handles whose logged commit is undecided (design: Publication), by address.
    pub(crate) undecided: BTreeMap<usize, Position>,
    /// The last commit SQLite published (PHASETWO), and the last BASE record written.
    pub(crate) confirmed: Position,
    pub(crate) based: Position,
    /// The latest write refused, until a commit is logged.
    pub(crate) refused: Option<WriteRefusal>,
    /// The snapshot thread's window (design: Snapshots).
    pub(crate) window: Window,
    pub(crate) stats: Stats,
}

/// The snapshot thread's window: while it is open no commit is logged, so the snapshot's
/// checkpoint and the start of its read transaction see exactly its `k`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum Window {
    #[default]
    Closed,
    /// A snapshot waits for no undecided commit and no other checkpointer: a commit point waits
    /// for it at most a second.
    Wanted,
    /// Commit points wait until it closes.
    Open,
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

    /// The log, for a connection's append (a COMMIT, an ACK or a BASE record), unless the binding
    /// reset. The reset is checked under the log's lock (design: Materialize), and the driver takes
    /// that lock once it marks a reset, so no connection appends to the log after the reset: an
    /// open may copy it (design: Materialize).
    pub(crate) fn log_unless_reset(&self) -> Result<MutexGuard<'_, Log>, LogRefused> {
        let log = self.handle.log()?;
        match self.reset_kind() {
            Some(kind) => Err(LogRefused::Reset(kind)),
            None => Ok(log),
        }
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

    /// A main db handle of `holder` opens ([`BindingHandle::connect`]). `None` once the binding
    /// retired or was given up.
    pub(crate) fn connect(&self, holder: Holder) -> Option<Connected> {
        self.handle.connect(holder)
    }

    /// The open of a main db handle failed after [`Binding::connect`]: undone exactly.
    pub(crate) fn unconnect(&self, connected: Connected) {
        let told = match self.handle.unconnect(connected) {
            Undone::Stays => Ok(()),
            Undone::Retires => driver().retire(self.id),
            Undone::WaitsAgain => driver().unconnected(self.id),
        };
        if told.is_err() {
            log::emit(Level::Error, "driver_gone", &[("file", &self.path)]);
        }
    }

    /// A main db handle of `holder` closed: after the last one, the binding drains and retires,
    /// unless `ursula_open` handed it out since a connection last opened it.
    pub(crate) fn disconnect(&self, holder: Holder) {
        if self.handle.disconnect(holder) && driver().retire(self.id).is_err() {
            log::emit(Level::Error, "driver_gone", &[("file", &self.path)]);
        }
    }
}

/// An open's stream and role: the registry keeps owners' and readers' opens apart.
type OpenKey = (String, Role);

#[derive(Default)]
struct Registry {
    /// Owners' bindings, by canonical stream name.
    names: HashMap<String, Arc<Binding>>,
    /// Reader bindings, by canonical stream name.
    readers: HashMap<String, Arc<Binding>>,
    /// By db file path, both roles.
    paths: HashMap<String, Arc<Binding>>,
    /// Opens in progress, by canonical stream name and role.
    opens: HashMap<OpenKey, OpenEntry>,
    /// Opens that resolved and whose binding one caller is registering ([`register`]): another
    /// open of one waits on [`registered`] until it is registered.
    registering: HashSet<OpenKey>,
}

impl Registry {
    /// The bound databases of `role`, by canonical stream name.
    fn bindings(&mut self, role: Role) -> &mut HashMap<String, Arc<Binding>> {
        match role {
            Role::Owner => &mut self.names,
            Role::Reader => &mut self.readers,
        }
    }

    /// Whether `name` is registering for an open of `role`: a reader's open waits for the owner's
    /// registration too, whose binding it would share.
    fn registering(&self, name: &str, role: Role) -> bool {
        let of = |role| self.registering.contains(&(name.to_owned(), role));
        match role {
            Role::Owner => of(Role::Owner),
            Role::Reader => of(Role::Owner) || of(Role::Reader),
        }
    }
}

/// An open in progress, and who asked for it.
struct OpenEntry {
    open: OpenId,
    /// The URL it was asked with.
    url: String,
    /// The cache root it opens under.
    dir: CacheDir,
    /// Blocking `ursula_open` calls waiting for it now.
    waiting: usize,
    /// A call that returned at once asked for it: it comes back for the result, so no blocking
    /// call's timeout gives the open up.
    returned: bool,
}

/// Marks an open registering until dropped, a panic in [`register`] included.
struct Registering<'a>(&'a OpenKey);

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
    let open_ms = if timeout_ms == 0 {
        options.timeout_ms.unwrap_or(DEFAULT_OPEN_MS)
    } else {
        timeout_ms
    };
    // Resolved before the registry's lock is taken.
    let requested = CacheDir::new(&dir.map_or_else(default_dir, PathBuf::from));
    let role = match options.access {
        Access::Owner { .. } => Role::Owner,
        Access::Reader => Role::Reader,
    };
    let key: OpenKey = (name.as_str().to_owned(), role);
    loop {
        let open = {
            let mut reg = registry();
            // Another caller registers the binding of this stream: it is local work.
            let deadline = Instant::now().checked_add(Duration::from_millis(REGISTERING_MS));
            while reg.registering(name.as_str(), role) {
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
            match claim(&mut reg, &key, &requested, timeout_ms > 0)? {
                Claim::Bound(b) => {
                    handed_out(&b);
                    return Ok(Opened::Bound(b));
                }
                Claim::Join(open) => open,
                Claim::Start => {
                    let spec = OpenSpec {
                        url: url.to_owned(),
                        dir: requested.path.clone(),
                        activation: options.activation.unwrap_or_else(Activation::draw),
                        max_pending_bytes: options.max_pending_bytes,
                        max_request_bytes: crate::config::max_request_bytes(),
                        max_commit_bytes: crate::config::max_commit_bytes(),
                        access: options.access,
                        timeout_ms: open_ms,
                    };
                    let open = driver().open(spec)?;
                    reg.opens.insert(key.clone(), OpenEntry {
                        open,
                        url: url.to_owned(),
                        dir: requested.clone(),
                        waiting: usize::from(timeout_ms > 0),
                        returned: timeout_ms == 0,
                    });
                    open
                }
            }
        };
        if let Some(opened) = take(&key, open, timeout_ms)? {
            return Ok(opened);
        }
        // Another caller took the open's result: its registration, or a new open, is next.
    }
}

/// Where `ursula_open` of a stream goes.
#[derive(Debug)]
enum Claim {
    /// Its binding, handed out again.
    Bound(Arc<Binding>),
    /// The open of it in progress.
    Join(OpenId),
    /// A new open. The driver starts it once a binding of the stream given up for it retired.
    Start,
}

/// Decides, under the registry's lock, where an open of stream `name` under the cache root
/// `requested` goes (a `blocking` call waits for an open in progress, another comes back for it).
/// A stream binds under one cache root at a time: an open in progress under another root refuses
/// the open, and so does a binding under another root that the host holds. One nothing of the host
/// holds is given up ([`BindingHandle::give_up`]) and leaves the registry, and the driver starts
/// the new open once it retired. A reset or retired binding leaves the registry for a new open,
/// which beside a reset binding's open connections shares their root and builds from a copy of
/// their generation (design: Materialize).
///
/// A reader's open first meets the owner's binding of the stream by the same rule (design:
/// Readers): under the same root it shares it, under another one it gives it up or is refused. Only
/// with no owner's binding left does it go to its own reader binding or open.
fn claim(
    reg: &mut Registry,
    key: &OpenKey,
    requested: &CacheDir,
    blocking: bool,
) -> Result<Claim, Error> {
    let (name, role) = (key.0.as_str(), key.1);
    if let Some(entry) = reg.opens.get(key)
        && entry.dir != *requested
    {
        return Err(elsewhere(&entry.dir, requested, Kept::Opening));
    }
    let roles: &[Role] = match role {
        Role::Owner => &[Role::Owner],
        Role::Reader => &[Role::Owner, Role::Reader],
    };
    for &of in roles {
        if let Some(b) = reg.bindings(of).get(name).cloned() {
            if b.reset_kind().is_none() {
                if b.dir != *requested {
                    b.handle
                        .give_up()
                        .map_err(|kept| elsewhere(&b.dir, requested, kept))?;
                } else if b.handle.revive() {
                    return Ok(Claim::Bound(b));
                }
            }
            reg.bindings(of).remove(name);
            reg.paths.remove(&b.path);
        }
    }
    match reg.opens.get_mut(key) {
        Some(entry) => {
            if blocking {
                entry.waiting = entry.waiting.saturating_add(1);
            } else {
                entry.returned = true;
            }
            Ok(Claim::Join(entry.open))
        }
        None => Ok(Claim::Start),
    }
}

/// A cache root in one form, for the open and for every comparison: absolute, its deepest
/// existing ancestor resolved (symlinks, `..` as the file system follows it), the rest appended
/// lexically (`.` and `..` resolved).
#[derive(Debug, Clone, PartialEq, Eq)]
struct CacheDir {
    path: PathBuf,
}

impl CacheDir {
    fn new(path: &Path) -> Self {
        let absolute = std::path::absolute(path).unwrap_or_else(|_no_cwd| path.to_owned());
        let mut resolved = PathBuf::new();
        for part in absolute.components() {
            match part {
                Component::CurDir => {}
                // Every existing prefix of `resolved` is resolved: the parent `..` reaches is the
                // one the file system reaches, and a missing one is lexical.
                Component::ParentDir => {
                    resolved.pop();
                }
                other => {
                    resolved.push(other);
                    // Missing, it stays as spelled; `..` may lead back to existing ones.
                    if let Ok(real) = std::fs::canonicalize(&resolved) {
                        resolved = real;
                    }
                }
            }
        }
        Self { path: resolved }
    }
}

/// The open of a stream under `requested` while it is open under `bound` in this process.
fn elsewhere(bound: &CacheDir, requested: &CacheDir, kept: Kept) -> Error {
    Error::Open(Arc::new(crate::open::OpenError::BoundElsewhere {
        bound: bound.path.clone(),
        requested: requested.path.clone(),
        kept,
    }))
}

/// How much longer than the open's own deadline `ursula_open` waits, so the driver's failure (the
/// last condition) is what it reports.
const DEADLINE_GRACE_MS: u64 = 1_000;

/// The outcome of open `open` of stream `name`: with `timeout_ms` 0 if it resolved, without
/// waiting; otherwise waiting for it (the open's own deadline is `timeout_ms`). Bound, it is
/// registered. The result is taken under the registry lock, which marks the stream registering,
/// so a concurrent open of it waits for the registration instead of opening again. `None` when
/// another caller took the result.
fn take(name: &OpenKey, open: OpenId, timeout_ms: u64) -> Result<Option<Opened>, Error> {
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
    reg.registering.insert(name.clone());
    drop(reg);
    let _registering = Registering(name);
    register(bound, url).map(|b| Some(Opened::Bound(b)))
}

/// A database the driver bound joins the registry. Its db file was published with page 1 written
/// and marked (WAL format), so no connection ever commits through a rollback journal.
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
    // Every commit the log holds at bind is published: `confirmed` starts at the last one, as
    // `logged` does, not at the log's last record (an ACK, a CLAIM or a BASE may follow it).
    let at_bind = handle.last_commit();
    let dir = CacheDir::new(&handle.dir);
    let b = Arc::new(Binding {
        id: binding,
        handle: Arc::clone(&handle),
        path: path.clone(),
        url,
        dir,
        state: Mutex::new(Commit {
            confirmed: at_bind,
            based: at_bind,
            ..Commit::default()
        }),
        wal_written: Mutex::new(BTreeMap::new()),
        decided: Condvar::new(),
    });
    let mut reg = registry();
    reg.bindings(handle.role)
        .insert(handle.name.as_str().to_owned(), Arc::clone(&b));
    reg.paths.insert(path, Arc::clone(&b));
    drop(reg);
    log::emit(Level::Info, "open", &[
        ("file", &b.path),
        ("stream", &b.url),
        ("role", &handle.role.as_str()),
    ]);
    // A reader publishes no snapshot: it claims nothing.
    if handle.role == Role::Owner {
        crate::snapshotter::spawn(&b);
    }
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

/// The default cache root (design: Layout): `<user cache>/sqlite-ursula/0.8`.
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

/// A binding of a fresh generation under `dir`, for tests of the file methods. It is in no
/// registry, and the driver never heard of it: a reset only marks it.
#[cfg(test)]
pub(crate) fn for_tests(dir: &std::path::Path) -> Arc<Binding> {
    for_tests_with_limit(dir, crate::capture::DEFAULT_MAX_COMMIT_BYTES)
}

/// [`for_tests`] with a commit limit of `max_commit_bytes`.
#[cfg(test)]
pub(crate) fn for_tests_with_limit(dir: &std::path::Path, max_commit_bytes: u64) -> Arc<Binding> {
    use ursula_headers::Incarnation;
    use ursula_headers::Offset;

    use crate::codec::segment::SegmentHeader;
    use crate::driver::Bind;
    use crate::generation::Fate;
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
        .publish(Fate::Trash)
        .unwrap();
    let (log, _) = Log::open(&generation.log_dir()).unwrap();
    let path = generation.db().to_string_lossy().into_owned();
    let handle = Arc::new(BindingHandle::new(
        Bind {
            generation,
            name,
            cluster_id: "cluster".into(),
            incarnation: Incarnation::try_from(1 << 52).unwrap(),
            log,
            pending_bytes: 0,
            max_pending_bytes: 1 << 20,
            max_commit_bytes,
            acked: Position(0),
            attach: crate::driver::Attach::Resumed,
            provisional: false,
            dir: dir.to_owned(),
            role: crate::driver::Role::Owner,
            closed: false,
        },
        Arc::new(root),
    ));
    Arc::new(connection_for_tests(BindingId(u64::MAX), handle, path))
}

/// The commit path's view of a binding the driver bound (`id`, `handle`), for driver tests of
/// what a connection may still do once the binding reset. It is in no registry.
#[cfg(test)]
pub(crate) fn connection_for_tests(
    id: BindingId,
    handle: Arc<BindingHandle>,
    path: String,
) -> Binding {
    Binding {
        id,
        dir: CacheDir::new(&handle.dir),
        handle,
        path,
        url: "https://h/v1/stream/db".into(),
        state: Mutex::new(Commit::default()),
        wal_written: Mutex::new(BTreeMap::new()),
        decided: Condvar::new(),
    }
}

/// Puts `b` in the registry under its path, for tests of the VFS methods that look a db up, until
/// the guard drops.
#[cfg(test)]
pub(crate) fn registered_for_tests(b: &Arc<Binding>) -> impl Drop {
    struct Registered(String);
    impl Drop for Registered {
        fn drop(&mut self) {
            registry().paths.remove(&self.0);
        }
    }
    registry().paths.insert(b.path.clone(), Arc::clone(b));
    Registered(b.path.clone())
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use super::CacheDir;
    use super::Claim;
    use super::Commit;
    use super::OpenEntry;
    use super::Registry;
    use super::claim;
    use super::default_dir;
    use super::for_tests;
    use crate::driver::Holder;
    use crate::driver::Kept;
    use crate::driver::Life;
    use crate::driver::OpenId;
    use crate::driver::Role;
    use crate::error::Action;
    use crate::error::Error;
    use crate::error::ErrorKind;
    use crate::open::OpenError;
    use crate::testdir::TempDir;

    // A cache root has one form: two spellings of one directory, through a symlink, `.` or
    // `..`, existing or not, are one root, and `..` after a symlink goes where the file system
    // goes.
    #[test]
    fn a_cache_dir_has_one_form() {
        let tmp = TempDir::new("db-cache-dir");
        let dir = std::fs::canonicalize(tmp.path()).unwrap();
        let root = dir.join("root");
        std::fs::create_dir(&root).unwrap();
        let form = |p: &std::path::Path| CacheDir::new(p).path;
        // A symlink, and a missing directory under one (as `/var` is `/private/var` on macOS).
        let link = dir.join("link");
        std::os::unix::fs::symlink(&root, &link).unwrap();
        assert_eq!(form(&link), root);
        assert_eq!(
            form(&link.join("new").join("cache")),
            root.join("new").join("cache")
        );
        assert_eq!(form(&dir.join(".").join("root")), root);
        assert_ne!(form(&dir), root);
        // `..` after a symlink is the target's parent; in a missing tail it is lexical.
        let deep = dir.join("deep").join("a").join("b");
        std::fs::create_dir_all(&deep).unwrap();
        let l2 = dir.join("l2");
        std::os::unix::fs::symlink(&deep, &l2).unwrap();
        assert_eq!(
            form(&l2.join("..").join("z")),
            dir.join("deep").join("a").join("z")
        );
        assert_eq!(
            form(&dir.join("missing").join("x").join("..").join("y")),
            dir.join("missing").join("y")
        );
        // A missing component, `..` back to existing ones, then a symlink: resolved.
        assert_eq!(form(&dir.join("nope").join("..").join("link")), root);
        // Relative to the working directory.
        let cwd = std::fs::canonicalize(std::env::current_dir().unwrap()).unwrap();
        assert_eq!(
            form(std::path::Path::new("./no-such-a/../no-such-b")),
            cwd.join("no-such-b")
        );
    }

    /// The open error `e` is, with its kind's action.
    fn kept(e: &Error) -> (Kept, ErrorKind, Action) {
        let Error::Open(open) = e else {
            panic!("{e}");
        };
        let OpenError::BoundElsewhere { kept, .. } = **open else {
            panic!("{e}");
        };
        (kept, e.kind(), e.kind().action())
    }

    // The cache-root rule: under its own root the binding is handed out again; under another one it
    // is refused while the host holds it (`bound_elsewhere`, fixed by the caller), and otherwise
    // given up and dropped from the registry for a new open that starts after it. An open in
    // progress is joined under its own root and refused under another.
    #[test]
    fn an_open_under_another_cache_root_takes_only_a_binding_the_host_left() {
        let first = TempDir::new("db-claim-first");
        let second = TempDir::new("db-claim-second");
        let b = for_tests(first.path());
        let name = b.handle.name.as_str().to_owned();
        let key = (name.clone(), Role::Owner);
        let reader = (name.clone(), Role::Reader);
        let mut reg = Registry::default();
        reg.names.insert(name.clone(), Arc::clone(&b));
        reg.paths.insert(b.path.clone(), Arc::clone(&b));
        let here = CacheDir::new(first.path());
        let there = CacheDir::new(second.path());
        // Handed out, not connected yet.
        let e = claim(&mut reg, &key, &there, true).unwrap_err();
        assert_eq!(
            kept(&e),
            (Kept::HandedOut, ErrorKind::BoundElsewhere, Action::Fix)
        );
        assert!(e.to_string().contains(&format!(
            "bound_elsewhere: the stream is open in this process under {}, not {}",
            here.path.display(),
            there.path.display()
        )));
        // A connection opens: refused; under its own root it is handed out again.
        assert!(b.handle.connect(Holder::Host).is_some());
        let e = claim(&mut reg, &key, &there, true).unwrap_err();
        assert_eq!(kept(&e).0, Kept::Connections(1));
        // A read-only open meets the owner's binding by the same rule: refused under another root
        // while the host holds it, the owner's binding handed out under its own.
        let e = claim(&mut reg, &reader, &there, true).unwrap_err();
        assert_eq!(kept(&e).0, Kept::Connections(1));
        assert!(matches!(
            claim(&mut reg, &reader, &here, true),
            Ok(Claim::Bound(shared)) if Arc::ptr_eq(&shared, &b)
        ));
        assert!(b.handle.connect(Holder::Host).is_some());
        assert!(!b.handle.disconnect(Holder::Host));
        assert!(matches!(
            claim(&mut reg, &key, &here, true),
            Ok(Claim::Bound(_))
        ));
        assert!(b.handle.connect(Holder::Host).is_some());
        // Its connections close: given up, and gone from the registry.
        assert!(!b.handle.disconnect(Holder::Host));
        assert!(b.handle.disconnect(Holder::Host));
        assert!(matches!(
            claim(&mut reg, &key, &there, true),
            Ok(Claim::Start)
        ));
        assert_eq!(b.handle.life(), Life::GivenUp);
        assert!(reg.names.is_empty() && reg.paths.is_empty());
        // The open in progress under the second root.
        reg.opens.insert(key.clone(), OpenEntry {
            open: OpenId(7),
            url: b.url.clone(),
            dir: there.clone(),
            waiting: 0,
            returned: false,
        });
        assert!(matches!(
            claim(&mut reg, &key, &there, false),
            Ok(Claim::Join(OpenId(7)))
        ));
        assert!(reg.opens.get(&key).is_some_and(|entry| entry.returned));
        let e = claim(&mut reg, &key, &here, true).unwrap_err();
        assert_eq!(
            kept(&e),
            (Kept::Opening, ErrorKind::BoundElsewhere, Action::Fix)
        );
    }

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
