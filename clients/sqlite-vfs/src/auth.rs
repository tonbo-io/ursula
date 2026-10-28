//! The bearer token every request carries, one per process, from the first of three sources
//! ([`Source`]): the token the application set (`ursula_set_token`), else the content of
//! `URSULA_VFS_TOKEN_FILE`, read again whenever the file changes and after the endpoint refused a
//! token, else the token an API key was last exchanged for ([`use_key`]). A change of it sends
//! again at once every request a `401` blocked ([`crate::driver::runtime::token_changed`]): a
//! token set at once, a file whose token changed when a read finds the new one, by a request or by
//! the driver's check every [`FILE_CHECK_MS`] while requests carry it ([`watching`],
//! [`check_file`]), and a key's new token when its exchange ends.
//!
//! A key's exchanges run on a thread of their own, the refresher, by the key's schedule
//! (`key`), while the source is active: an open, a bound database (the driver says so,
//! [`requests_may_go_out`]) or an import ([`importing`]) may send requests and the key's token is
//! the one they carry, or a host waits for the key's token. A `401` to its token ([`refused`])
//! brings the refresh forward. The process holds one key: another one is refused while a host
//! waits for the key's token, for [`HANDED_OUT_HOLD_MS`] after a host was handed it (its open is
//! asked for meanwhile), and while requests may go out that can carry its token, since they would
//! all carry the other key's tokens from then on. A key whose exchanges never gave a token, or
//! were refused, gives way, and the key in its place carries its last token until a token of its
//! own arrives. Nothing that reads a file or writes a log runs under the key's lock. Neither the
//! key nor a token is ever logged or returned.
//!
//! Module map:
//!
//! - `exchange`: one exchange of a key at its token endpoint, and what the answer gives.
//! - `key`: a key's schedule: when it exchanges, and what a token, a failure or a `401` changes.

mod exchange;
mod key;

use std::fs;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::OnceLock;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

pub(crate) use self::exchange::ExchangeError;
use self::exchange::Key;
use self::exchange::KeyRefusal;
use self::key::Jitter;
#[cfg(test)]
pub(crate) use self::key::KeyState;
pub(crate) use self::key::KeyView;
use self::key::Schedule;
use crate::config::token_file;
use crate::error::ErrorKind;
use crate::log;
use crate::log::Level;

/// How often the driver checks the token file for a change ([`check_file`]) while requests carry
/// its token ([`watching`]) and an open or a writer may send requests.
pub(crate) const FILE_CHECK_MS: u64 = 1_000;

/// The token the application set, if any.
static SET: Mutex<Option<String>> = Mutex::new(None);
/// The token file as last read.
static FILE: Mutex<Option<Cached>> = Mutex::new(None);

struct Cached {
    /// The file's modification time and length when it was read.
    modified: Option<SystemTime>,
    len: Option<u64>,
    /// The last usable token the file held.
    token: Option<String>,
    /// The last read found no usable token (logged once, until a read finds one again).
    unusable: bool,
}

/// Why the token file gave no token.
#[derive(Debug, thiserror::Error)]
enum Unusable {
    #[error("{0}")]
    Read(#[source] std::io::Error),
    #[error("it holds no token (empty, or not printable ASCII without spaces)")]
    NoToken,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Sets the token every request carries from now on; `None` (or an empty token) goes back to the
/// token file.
pub(crate) fn set_token(token: Option<&str>) {
    let token = token.and_then(usable);
    let previous = std::mem::replace(&mut *lock(&SET), token.clone());
    if previous != token {
        changed();
    }
}

/// The token requests carry changed: the requests a `401` blocked go again at once. The key's
/// source may have started or stopped serving: its refresher looks again.
fn changed() {
    wake_refresher();
    crate::driver::runtime::token_changed();
}

/// A token set for a while ([`lend_token`]), and the token it replaced.
#[derive(Debug)]
pub(crate) struct Lent {
    ours: Option<String>,
    previous: Option<String>,
}

/// [`set_token`] for a while: [`take_back`] puts the token it replaced back.
pub(crate) fn lend_token(token: &str) -> Lent {
    let lent = lend(&SET, token);
    if lent.ours != lent.previous {
        changed();
    }
    lent
}

/// Puts the token `lent` replaced back, unless the application set one since: the token set is
/// then no longer the lent one, and stays.
pub(crate) fn take_back(lent: Lent) {
    if put_back(&SET, lent) {
        changed();
    }
}

fn lend(set: &Mutex<Option<String>>, token: &str) -> Lent {
    let ours = usable(token);
    let previous = std::mem::replace(&mut *lock(set), ours.clone());
    Lent { ours, previous }
}

/// Whether the token set changed.
fn put_back(set: &Mutex<Option<String>>, lent: Lent) -> bool {
    let mut set = lock(set);
    if *set != lent.ours {
        return false;
    }
    let changed = lent.previous != lent.ours;
    *set = lent.previous;
    changed
}

/// The token for the next request. `refresh`: read the token file again even if it looks unchanged
/// (after the endpoint refused the token).
pub(crate) fn token(refresh: bool) -> Option<String> {
    if let Some(token) = lock(&SET).clone() {
        return Some(token);
    }
    let Some(path) = token_file() else {
        return lock(&KEY)
            .as_ref()
            .and_then(|k| k.schedule.token().map(str::to_owned));
    };
    let read = file_token(path, refresh, &FILE);
    if read.changed {
        changed();
    }
    read.token
}

/// Where the token requests carry comes from, in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Source {
    /// `ursula_set_token`, or an open's `token`.
    Set,
    /// `URSULA_VFS_TOKEN_FILE`.
    File,
    /// An API key's exchanges.
    Key,
    None,
}

impl Source {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Source::Set => "set",
            Source::File => "file",
            Source::Key => "key",
            Source::None => "none",
        }
    }
}

/// The source of the token requests carry.
fn source() -> Source {
    if lock(&SET).is_some() {
        Source::Set
    } else if token_file().is_some() {
        Source::File
    } else if lock(&KEY).is_some() {
        Source::Key
    } else {
        Source::None
    }
}

/// The token's source and, while the process holds a key, the key's schedule, for
/// `ursula_status`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TokenStatus {
    pub(crate) source: Source,
    pub(crate) key: Option<KeyView>,
}

pub(crate) fn status() -> TokenStatus {
    let source = source();
    let key = lock(&KEY).as_ref().map(|k| k.schedule.view(clock_ms()));
    TokenStatus { source, key }
}

/// How long another key is refused after [`use_key`] handed a host the key's token: the host's
/// open is asked for within it, and from then on requests may go out carrying the key's token,
/// which keeps another key out as long as they may. A host that never opens holds the key no
/// longer than this.
const HANDED_OUT_HOLD_MS: u64 = 60_000;

/// The process's key and its schedule.
struct KeySource {
    key: Key,
    schedule: Schedule,
    /// Hosts waiting in [`use_key`] for its token: the source is active while one does.
    waiters: usize,
    /// Until when (on [`clock_ms`]) a host handed the key's token may yet ask for its open.
    held_until_ms: u64,
}

impl KeySource {
    /// `key`, in place of the key `previous` scheduled, if any: it carries the last token of that
    /// key until its own first one ([`Schedule::after`]).
    fn new(key: Key, previous: Option<&Schedule>, now_ms: u64) -> Self {
        Self {
            key,
            schedule: previous.map_or_else(
                || Schedule::new(now_ms),
                |previous| Schedule::after(previous, now_ms),
            ),
            waiters: 0,
            held_until_ms: 0,
        }
    }

    /// How the key is in use at `now_ms`, for [`admit`], where `requests_out` says whether an
    /// open, a bound database or an import may send requests ([`requests_out`]).
    fn usage(&self, requests_out: bool, now_ms: u64) -> Use {
        if self.waiters > 0 || now_ms < self.held_until_ms {
            Use::Claimed
        } else if requests_out && self.schedule.serves() {
            Use::Carried
        } else {
            Use::Idle
        }
    }
}

/// The key, once a host gave one.
static KEY: Mutex<Option<KeySource>> = Mutex::new(None);
/// Signalled under [`KEY`]'s lock when the key, its schedule or whether it is active changes.
static KEY_CHANGED: Condvar = Condvar::new();
/// Whether an open or a bound database may send requests ([`requests_may_go_out`]).
static REQUESTING: AtomicBool = AtomicBool::new(false);
/// Imports running ([`importing`]): their requests go out outside the driver.
static IMPORTS: AtomicUsize = AtomicUsize::new(0);
/// The refresher thread, started with the first key and again once it stopped.
static REFRESHER: Mutex<Option<JoinHandle<()>>> = Mutex::new(None);
/// Whether the refresher runs: set as it starts, cleared as it stops (only a panic stops it).
static REFRESHER_UP: AtomicBool = AtomicBool::new(false);

/// Milliseconds on the process's clock for key schedules.
fn clock_ms() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    let start = START.get_or_init(Instant::now);
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// A draw from the random device, or from the clock without one. It reads a file: never under
/// [`KEY`]'s lock.
fn jitter() -> Jitter {
    let draw = crate::ids::random16()
        .ok()
        .and_then(|b| b.first_chunk::<8>().copied())
        .map(u64::from_le_bytes)
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_or(0, |d| u64::from(d.subsec_nanos()))
        });
    Jitter::new(draw.checked_rem(1_001).unwrap_or_default())
}

/// Wakes the refresher to look at its schedule again. The lock taken first means it is either
/// waiting already, or has not yet looked at what changed.
fn wake_refresher() {
    drop(lock(&KEY));
    KEY_CHANGED.notify_all();
}

/// The driver says whether an open or a bound database may send requests: from when an open is
/// asked for until the last binding retires. A key's exchanges run only then (or while an import
/// runs or a host waits for its token).
pub(crate) fn requests_may_go_out(requesting: bool) {
    if REQUESTING.swap(requesting, Ordering::AcqRel) != requesting && requesting {
        wake_refresher();
    }
}

/// Whether requests may go out that carry the key's token, were it the source: an open, a bound
/// database or an import may send them.
fn requests_out() -> bool {
    REQUESTING.load(Ordering::Acquire) || IMPORTS.load(Ordering::Acquire) > 0
}

/// Whether requests carry the key's token: no token is set and no token file configured.
fn key_serves() -> bool {
    lock(&SET).is_none() && token_file().is_none()
}

/// Whether a key's exchanges go out: a host waits for its token, or requests may go out
/// carrying it.
fn active(waiting: bool, serving: bool, requests_out: bool) -> bool {
    waiting || (serving && requests_out)
}

/// An import running ([`importing`]). Dropped, the import no longer counts.
#[derive(Debug)]
pub(crate) struct Importing(());

/// An import starts: its requests may carry the key's token, so the key's exchanges go on while
/// it runs, as while a database may send requests.
pub(crate) fn importing() -> Importing {
    IMPORTS.fetch_add(1, Ordering::AcqRel);
    wake_refresher();
    Importing(())
}

impl Drop for Importing {
    fn drop(&mut self) {
        IMPORTS.fetch_sub(1, Ordering::AcqRel);
    }
}

/// A request that carried `token` was refused `401`: when it is the key's token, the key is
/// exchanged again at once (at least 1 s after the token arrived, `key`). Without a key, nothing
/// to do: no jitter is drawn, which reads a file.
pub(crate) fn refused(token: &str) {
    if lock(&KEY).is_none() {
        return;
    }
    let jitter = jitter();
    let moved = lock(&KEY)
        .as_mut()
        .is_some_and(|k| k.schedule.refused(token, clock_ms(), jitter));
    if moved {
        KEY_CHANGED.notify_all();
    }
}

/// Why a host's key gave it no token.
#[derive(Debug, thiserror::Error)]
pub(crate) enum KeyError {
    #[error(transparent)]
    Invalid(#[from] KeyRefusal),
    /// Another key, while a host is about to open with this one or requests may carry its
    /// tokens.
    #[error(
        "the process holds another API key, which a host is about to open with or requests may carry: one key per process"
    )]
    AnotherKey,
    /// The exchange failed in a way another try will not fix.
    #[error(transparent)]
    Exchange(ExchangeError),
    #[error(
        "no token within {timeout_ms} ms{}",
        .last.as_ref().map(|e| format!(": {e}")).unwrap_or_default()
    )]
    TimedOut {
        timeout_ms: u64,
        last: Option<ExchangeError>,
    },
    #[error("the token refresher thread could not be started")]
    NoRefresher(#[source] std::io::Error),
    /// The refresher stopped on a panic while the host waited. The next use starts another.
    #[error("the token refresher thread stopped")]
    RefresherStopped,
}

impl KeyError {
    /// The kind a host reports.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the Tonbo resolution reports it, in the change that adds it"
        )
    )]
    pub(crate) fn kind(&self) -> ErrorKind {
        match self {
            KeyError::Invalid(_) | KeyError::AnotherKey => ErrorKind::InvalidArgument,
            KeyError::Exchange(e) => e.kind(),
            KeyError::TimedOut { last, .. } => last
                .as_ref()
                .map_or(ErrorKind::Unavailable, ExchangeError::kind),
            KeyError::NoRefresher(_) | KeyError::RefresherStopped => ErrorKind::Internal,
        }
    }
}

/// How the key held is in use, for [`admit`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Use {
    /// A host waits for its token, or was handed one less than [`HANDED_OUT_HOLD_MS`] ago.
    Claimed,
    /// An open, a bound database or an import may send requests, and the key holds a token of
    /// its own they can carry: its exchanges were not refused.
    Carried,
    /// Neither: no request can carry a token of it, so another key may take its place.
    Idle,
}

/// Whether a host's key `next` can be the process's key, where it holds `held` in its use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Admit {
    /// The key held: its token and schedule serve on.
    Same,
    /// A first key, or another one in place of a key no request can carry: it starts afresh.
    Replace,
    /// Another key, while the one held is claimed or carried: refused.
    Refuse,
}

fn admit(held: Option<(&Key, Use)>, next: &Key) -> Admit {
    match held {
        Some((held, _)) if held == next => Admit::Same,
        Some((_, Use::Claimed | Use::Carried)) => Admit::Refuse,
        Some((_, Use::Idle)) | None => Admit::Replace,
    }
}

/// Makes `secret`, exchanged at `endpoint` for tokens of `audience`, the process's key, and waits
/// at most `timeout` for a token of it that has not expired and was not refused. A token held
/// serves at once. A key whose exchange is refused fails at once: a key refused since its last
/// token hands out none, unexpired or not, until the exchange this use starts at once gives it one.
/// Other failures are retried on the key's schedule until the time runs out. Another key than the
/// one held fails while a host waits for the held key's token or was handed it less than
/// [`HANDED_OUT_HOLD_MS`] ago, or while an open, a bound database or an import may send requests
/// carrying its token. The host asks for its open within [`HANDED_OUT_HOLD_MS`], so that the open
/// carries this key's tokens.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the Tonbo resolution installs the key, in the change that adds it"
    )
)]
pub(crate) fn use_key(
    secret: &str,
    endpoint: &str,
    audience: Option<&str>,
    timeout: Duration,
) -> Result<(), KeyError> {
    let key = Key::new(secret, endpoint, audience)?;
    start_refresher()?;
    let timeout_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
    let deadline = clock_ms().saturating_add(timeout_ms);
    let mut held = lock(&KEY);
    let usage = held
        .as_ref()
        .map(|k| (&k.key, k.usage(requests_out(), clock_ms())));
    match admit(usage, &key) {
        Admit::Refuse => return Err(KeyError::AnotherKey),
        Admit::Same => {}
        Admit::Replace => {
            let previous = held.as_ref().map(|k| &k.schedule);
            let next = KeySource::new(key.clone(), previous, clock_ms());
            *held = Some(next);
        }
    }
    let mut since = 0;
    if let Some(k) = held.as_mut() {
        k.schedule.wanted(clock_ms());
        k.waiters = k.waiters.saturating_add(1);
        since = k.schedule.ended();
    }
    KEY_CHANGED.notify_all();
    let outcome = loop {
        let now = clock_ms();
        // The key cannot be replaced while this host waits for it.
        let Some(k) = held.as_ref().filter(|k| k.key == key) else {
            break Err(KeyError::AnotherKey);
        };
        if k.schedule.valid(now) {
            break Ok(());
        }
        let last = k.schedule.last_failure();
        if k.schedule.ended() > since
            && let Some(e) = last.filter(|e| e.permanent())
        {
            break Err(KeyError::Exchange(e.clone()));
        }
        if !REFRESHER_UP.load(Ordering::Acquire) {
            break Err(KeyError::RefresherStopped);
        }
        if now >= deadline {
            break Err(KeyError::TimedOut {
                timeout_ms,
                last: last.cloned(),
            });
        }
        held = wait_until(held, Some(deadline), now);
    };
    if let Some(k) = held.as_mut().filter(|k| k.key == key) {
        k.waiters = k.waiters.saturating_sub(1);
        if outcome.is_ok() {
            let until = clock_ms().saturating_add(HANDED_OUT_HOLD_MS);
            k.held_until_ms = k.held_until_ms.max(until);
        }
    }
    drop(held);
    KEY_CHANGED.notify_all();
    outcome
}

/// Forgets the process's key, so that a test that gave the process one leaves none behind.
#[cfg(test)]
pub(crate) fn forget_key() {
    *lock(&KEY) = None;
    KEY_CHANGED.notify_all();
}

/// Starts the refresher thread unless it runs, outside [`KEY`]'s lock.
fn start_refresher() -> Result<(), KeyError> {
    let mut slot = lock(&REFRESHER);
    if REFRESHER_UP.load(Ordering::Acquire) {
        return Ok(());
    }
    REFRESHER_UP.store(true, Ordering::Release);
    let spawned = std::thread::Builder::new()
        .name("ursula-vfs-token".to_owned())
        .spawn(run_refresher);
    match spawned {
        Ok(handle) => {
            // A refresher that stopped before is done, or about to be: its handle goes.
            *slot = Some(handle);
            Ok(())
        }
        Err(e) => {
            REFRESHER_UP.store(false, Ordering::Release);
            drop(slot);
            log::emit(Level::Error, "token_refresher_failed", &[("error", &e)]);
            Err(KeyError::NoRefresher(e))
        }
    }
}

/// The refresher thread. Only a panic ends its loop (logged as `event=panic`): hosts waiting
/// then fail, and the next host's use starts another.
fn run_refresher() {
    crate::guard::contain("token_refresher", || (), refresh);
    REFRESHER_UP.store(false, Ordering::Release);
    wake_refresher();
}

/// The refresher's loop: exchanges the key whenever its schedule says so while it is active,
/// outside the lock, and otherwise waits for the schedule's next deadline or a change. Nothing
/// that reads a file or writes a log runs under the lock.
fn refresh() {
    let mut held = lock(&KEY);
    // A refresher that stopped may have left an exchange out that never ends.
    if let Some(k) = held.as_mut() {
        k.schedule.resume(clock_ms());
    }
    loop {
        let now = clock_ms();
        let (key, deadline) = match held.as_mut() {
            Some(k) => {
                let active = active(k.waiters > 0, key_serves(), requests_out());
                let start = k.schedule.start(now, active);
                (start.then(|| k.key.clone()), k.schedule.deadline(active))
            }
            None => (None, None),
        };
        let Some(key) = key else {
            held = wait_until(held, deadline, now);
            continue;
        };
        drop(held);
        let outcome = crate::guard::contain(
            "token_exchange",
            || Err(ExchangeError::Panicked),
            || exchange::exchange(&key),
        );
        let jitter = jitter();
        held = lock(&KEY);
        // A key replaced meanwhile drops the outcome: the new key's schedule starts afresh.
        let (changed, logged, kept_previous) = match held.as_mut().filter(|k| k.key == key) {
            Some(k) => {
                let before = k.schedule.view(clock_ms());
                let changed = k.schedule.exchanged(outcome, clock_ms(), jitter);
                let after = k.schedule.view(clock_ms());
                (
                    changed,
                    report(&before, after),
                    k.schedule.token().is_some(),
                )
            }
            None => (false, None, false),
        };
        // Whether requests carry the key's token now: a token set or taken back during the
        // exchange may have changed it.
        let serving = key_serves();
        drop(held);
        KEY_CHANGED.notify_all();
        match logged {
            Some(Report::Failed(e)) => log::emit(Level::Warn, "token_exchange_failed", &[
                ("kind", &e.kind().as_str()),
                ("error", &e),
                ("permanent", &e.permanent()),
                ("kept_previous", &kept_previous),
            ]),
            Some(Report::Recovered { failures }) => {
                log::emit(Level::Info, "token_exchange_recovered", &[(
                    "failures", &failures,
                )]);
            }
            None => {}
        }
        if changed && serving {
            crate::driver::runtime::token_changed();
        }
        held = lock(&KEY);
    }
}

/// What an exchange's end gives the log.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Report {
    /// The first failure of a run, or the first in it another try is unlikely to fix: the one a
    /// reader acts on.
    Failed(ExchangeError),
    /// A token after `failures` failed exchanges in a row.
    Recovered { failures: u32 },
}

/// What the log says of an exchange that moved the key's schedule from `before` to `after`.
fn report(before: &KeyView, after: KeyView) -> Option<Report> {
    match (&before.error, after.error) {
        (None, Some(e)) => Some(Report::Failed(e)),
        (Some(was), Some(e)) if e.permanent() && !was.permanent() => Some(Report::Failed(e)),
        (Some(_), Some(_)) | (None, None) => None,
        (Some(_), None) => Some(Report::Recovered {
            failures: before.failures,
        }),
    }
}

/// Waits on [`KEY_CHANGED`] with `held` until `deadline` (on [`clock_ms`], `now` now), or a change
/// without one.
fn wait_until(
    held: MutexGuard<'static, Option<KeySource>>,
    deadline: Option<u64>,
    now: u64,
) -> MutexGuard<'static, Option<KeySource>> {
    match deadline {
        Some(at) => {
            KEY_CHANGED
                .wait_timeout(held, Duration::from_millis(at.saturating_sub(now)))
                .unwrap_or_else(PoisonError::into_inner)
                .0
        }
        None => KEY_CHANGED
            .wait(held)
            .unwrap_or_else(PoisonError::into_inner),
    }
}

/// Whether requests carry the token file's token: one is configured and no token is set. Only
/// then can [`check_file`] find a change.
pub(crate) fn watching() -> bool {
    token_file().is_some() && lock(&SET).is_none()
}

/// Reads the token file again if it changed since the last read, while no token is set: a new
/// token in it is a change ([`token`]). A stat, and a read only after a change.
pub(crate) fn check_file() {
    drop(token(false));
}

/// What a look at the token file found.
struct FileToken {
    token: Option<String>,
    /// A read found a token other than the one the file held before: the token requests carry
    /// changed.
    changed: bool,
}

/// The token in the file at `path`, read again when it changed (or on `refresh`), outside the
/// cache's lock. A read that finds no usable token (the file is missing, empty, or caught
/// half-written by a writer that truncates it first) keeps the last token the file held: a
/// request sent without one is refused as if the stream did not exist (an authorizer conceals it
/// with a 404, which an open takes for a missing stream), whereas a stale token is refused with a
/// 401, which is retried with the file read again.
fn file_token(path: &str, refresh: bool, cache: &Mutex<Option<Cached>>) -> FileToken {
    let (modified, len) = stat(path);
    if !refresh
        && let Some(c) = lock(cache)
            .as_ref()
            .filter(|c| c.modified == modified && c.len == len)
    {
        return FileToken {
            token: c.token.clone(),
            changed: false,
        };
    }
    let read = fs::read_to_string(path)
        .map_err(Unusable::Read)
        .and_then(|text| usable(&text).ok_or(Unusable::NoToken));
    let mut cached = lock(cache);
    let read_before = cached.is_some();
    let last = cached.as_ref().and_then(|c| c.token.clone());
    // `changed`: a token other than the one the file held before, once it was read.
    let (token, unusable, changed) = match read {
        Ok(token) => {
            let changed = read_before && last.as_deref() != Some(token.as_str());
            (Some(token), false, changed)
        }
        Err(e) => {
            if !cached.as_ref().is_some_and(|c| c.unusable) {
                log::emit(Level::Warn, "token_file_unreadable", &[
                    ("path", &path),
                    ("error", &e),
                    ("kept_previous", &last.is_some()),
                ]);
            }
            (last, true, false)
        }
    };
    *cached = Some(Cached {
        modified,
        len,
        token: token.clone(),
        unusable,
    });
    FileToken { token, changed }
}

/// The file's modification time and length, `None` when it cannot be read.
fn stat(path: &str) -> (Option<SystemTime>, Option<u64>) {
    let meta = fs::metadata(path).ok();
    (
        meta.as_ref().and_then(|m| m.modified().ok()),
        meta.as_ref().map(fs::Metadata::len),
    )
}

/// A token as `Authorization: Bearer` carries it: trimmed, non-empty, printable ASCII without
/// spaces.
fn usable(token: &str) -> Option<String> {
    let token = token.trim();
    (!token.is_empty() && token.bytes().all(|b| b.is_ascii_graphic())).then(|| token.to_owned())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Mutex;

    use super::Admit;
    use super::HANDED_OUT_HOLD_MS;
    use super::KeyError;
    use super::KeySource;
    use super::KeyState;
    use super::KeyView;
    use super::Report;
    use super::Use;
    use super::active;
    use super::admit;
    use super::exchange::ExchangeError;
    use super::exchange::Exchanged;
    use super::exchange::Key;
    use super::exchange::KeyRefusal;
    use super::file_token;
    use super::key::Jitter;
    use super::key::Schedule;
    use super::lend;
    use super::put_back;
    use super::report;
    use super::usable;
    use crate::error::ErrorKind;

    // One key per process: the key held serves on. Another is refused while a host waits for
    // the held key's token or was handed it within the hold, and while requests may go out that
    // can carry its token, since they would all carry the other key's tokens from then on. A key
    // that never got a token of its own, or whose exchanges were refused, gives way, and the key
    // in its place carries its last token.
    #[test]
    fn another_key_is_refused_while_requests_may_go_out() {
        let key = |secret: &str, audience: &str| {
            Key::new(secret, "https://auth.example/token", Some(audience)).unwrap()
        };
        let held = key("k1", "a");
        let other = key("k2", "a");
        assert_eq!(admit(None, &held), Admit::Replace);
        for usage in [Use::Claimed, Use::Carried, Use::Idle] {
            assert_eq!(admit(Some((&held, usage)), &key("k1", "a")), Admit::Same);
        }
        assert_eq!(admit(Some((&held, Use::Claimed)), &other), Admit::Refuse);
        assert_eq!(admit(Some((&held, Use::Carried)), &other), Admit::Refuse);
        assert_eq!(
            admit(Some((&held, Use::Carried)), &key("k1", "b")),
            Admit::Refuse
        );
        assert_eq!(admit(Some((&held, Use::Idle)), &other), Admit::Replace);

        let jitter = Jitter::new(0);
        let pending = Schedule::new(0);
        let mut served = Schedule::new(0);
        assert!(served.start(0, true));
        let t1 = Exchanged {
            token: "t1".to_owned(),
            lifetime_ms: 100_000,
        };
        assert!(served.exchanged(Ok(t1), 0, jitter));
        let mut refused = served.clone();
        assert!(refused.start(70_000, true));
        let refusal = Err(ExchangeError::Refused { status: 401 });
        assert!(!refused.exchanged(refusal, 70_000, jitter));
        let mut failing = served.clone();
        assert!(failing.start(70_000, true));
        let down = Err(ExchangeError::Unavailable { status: 503 });
        assert!(!failing.exchanged(down, 70_000, jitter));
        let source = |schedule: &Schedule, waiters: usize, held_until_ms: u64| KeySource {
            key: held.clone(),
            schedule: schedule.clone(),
            waiters,
            held_until_ms,
        };
        let usage = |schedule: &Schedule, requests_out: bool| {
            source(schedule, 0, 0).usage(requests_out, 80_000)
        };
        assert_eq!(usage(&served, true), Use::Carried);
        assert_eq!(usage(&failing, true), Use::Carried, "its last token serves");
        assert_eq!(usage(&served, false), Use::Idle);
        assert_eq!(usage(&pending, true), Use::Idle, "never got a token");
        assert_eq!(usage(&refused, true), Use::Idle, "refused");
        assert_eq!(
            source(&pending, 1, 0).usage(false, 80_000),
            Use::Claimed,
            "a host waits"
        );
        let until = 80_000_u64.saturating_add(HANDED_OUT_HOLD_MS);
        let handed_out = source(&refused, 0, until);
        assert_eq!(handed_out.usage(true, 80_000), Use::Claimed, "handed out");
        assert_eq!(
            handed_out.usage(true, until),
            Use::Idle,
            "a host that never opened holds it no longer"
        );
        assert_eq!(
            source(&served, 0, 1).usage(true, 80_000),
            Use::Carried,
            "the open asked for within the hold carries it on"
        );

        // The key in place of a refused one carries its last token, never as valid or its own.
        let next = KeySource::new(other.clone(), Some(&refused), 80_000);
        assert_eq!(next.schedule.token(), Some("t1"));
        assert!(!next.schedule.valid(80_000));
        assert_eq!(next.usage(true, 80_000), Use::Idle);
        let first = KeySource::new(other, None, 80_000);
        assert_eq!(first.schedule.token(), None);
    }

    // The log reports a run's first failure, the first failure in it another try is unlikely to
    // fix, and the token that ends it.
    #[test]
    fn the_log_reports_a_runs_start_its_turn_to_permanent_and_its_end() {
        let view = |failures: u32, error: Option<ExchangeError>| KeyView {
            state: KeyState::Failing,
            expires_in_ms: None,
            refresh_in_ms: None,
            failures,
            error,
        };
        let down = || Some(ExchangeError::Unavailable { status: 503 });
        let refused = || Some(ExchangeError::Refused { status: 401 });
        assert_eq!(
            report(&view(0, None), view(1, down())),
            Some(Report::Failed(ExchangeError::Unavailable { status: 503 }))
        );
        assert_eq!(report(&view(1, down()), view(2, down())), None);
        assert_eq!(
            report(&view(2, down()), view(3, refused())),
            Some(Report::Failed(ExchangeError::Refused { status: 401 }))
        );
        assert_eq!(report(&view(3, refused()), view(4, refused())), None);
        assert_eq!(report(&view(4, refused()), view(5, down())), None);
        assert_eq!(
            report(&view(5, down()), view(0, None)),
            Some(Report::Recovered { failures: 5 })
        );
        assert_eq!(report(&view(0, None), view(0, None)), None);
    }

    // A key's exchanges go out while a host waits for its token, or while requests may go out
    // that carry it.
    #[test]
    fn a_key_is_exchanged_only_while_active() {
        assert!(active(true, false, false));
        assert!(active(false, true, true));
        assert!(!active(false, true, false), "nothing may send requests");
        assert!(!active(false, false, true), "requests carry another token");
    }

    // A host reports a key's failure by its kind: a key given wrong is an invalid argument, an
    // exchange's failure keeps its own kind.
    #[test]
    fn key_errors_have_kinds() {
        assert_eq!(
            KeyError::from(KeyRefusal::Unusable).kind(),
            ErrorKind::InvalidArgument
        );
        assert_eq!(KeyError::AnotherKey.kind(), ErrorKind::InvalidArgument);
        let refused = ExchangeError::Refused { status: 403 };
        assert_eq!(
            KeyError::Exchange(refused.clone()).kind(),
            ErrorKind::Forbidden
        );
        assert_eq!(
            KeyError::Exchange(refused.clone()).to_string(),
            refused.to_string(),
            "printed once"
        );
        let timed_out = |last| KeyError::TimedOut {
            timeout_ms: 10,
            last,
        };
        assert_eq!(timed_out(None).kind(), ErrorKind::Unavailable);
        let limited = ExchangeError::RateLimited {
            retry_after_ms: None,
        };
        assert_eq!(timed_out(Some(limited)).kind(), ErrorKind::RateLimited);
        assert_eq!(KeyError::RefresherStopped.kind(), ErrorKind::Internal);
    }

    // A lent token is taken back to the one it replaced, unless the application set another since.
    #[test]
    fn a_lent_token_goes_back_unless_another_was_set_since() {
        let set = Mutex::new(Some("app".to_owned()));
        let lent = lend(&set, "import");
        assert_eq!(set.lock().unwrap().as_deref(), Some("import"));
        assert!(put_back(&set, lent), "the token changed back");
        assert_eq!(set.lock().unwrap().as_deref(), Some("app"));

        let lent = lend(&set, "import");
        *set.lock().unwrap() = Some("newer".to_owned());
        assert!(!put_back(&set, lent));
        assert_eq!(set.lock().unwrap().as_deref(), Some("newer"));

        let lent = lend(&set, "import");
        *set.lock().unwrap() = None;
        assert!(!put_back(&set, lent));
        assert_eq!(*set.lock().unwrap(), None, "cleared since: stays cleared");

        let lent = lend(&set, "import");
        assert!(!put_back(&set, lend(&set, "import")), "the same token");
        assert!(put_back(&set, lent));
    }

    #[test]
    fn tokens_are_trimmed_printable_ascii() {
        assert_eq!(usable(" abc.def-_~+/=\n").as_deref(), Some("abc.def-_~+/="));
        assert_eq!(usable(""), None);
        assert_eq!(usable("  \n"), None);
        assert_eq!(usable("two words"), None);
        assert_eq!(usable("caf\u{e9}"), None);
    }

    // A token file that holds no usable token (empty, half-written, or missing for a moment while
    // it is replaced) keeps the last token it held, so requests never go out without one.
    #[test]
    fn a_token_file_without_a_token_keeps_the_last_one() {
        let dir = std::env::temp_dir().join(format!("ursula-token-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("token");
        let file = path.to_str().unwrap();
        let cache = Mutex::new(None);
        fs::write(&path, "").unwrap();
        assert_eq!(file_token(file, true, &cache).token, None);
        fs::write(&path, "t1\n").unwrap();
        assert_eq!(file_token(file, true, &cache).token.as_deref(), Some("t1"));
        fs::write(&path, "").unwrap();
        assert_eq!(file_token(file, true, &cache).token.as_deref(), Some("t1"));
        fs::write(&path, "half written").unwrap();
        assert_eq!(file_token(file, true, &cache).token.as_deref(), Some("t1"));
        fs::remove_file(&path).unwrap();
        assert_eq!(file_token(file, true, &cache).token.as_deref(), Some("t1"));
        fs::write(&path, "t2").unwrap();
        assert_eq!(file_token(file, true, &cache).token.as_deref(), Some("t2"));
        // Unchanged since the last read: the cached token, without reading the file.
        assert_eq!(file_token(file, false, &cache).token.as_deref(), Some("t2"));
        fs::remove_dir_all(&dir).unwrap();
    }

    // Whichever read first finds a new token in the file signals the change, once: a request's
    // read or the driver's check alike. A first read, a file read again unchanged, and a file
    // without a token (which keeps the last one) signal none.
    #[test]
    fn the_read_that_finds_a_new_token_signals_the_change() {
        let dir = std::env::temp_dir().join(format!("ursula-token-change-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("token");
        let file = path.to_str().unwrap();
        let cache = Mutex::new(None);
        fs::write(&path, "t1").unwrap();
        let first = file_token(file, false, &cache);
        assert_eq!(first.token.as_deref(), Some("t1"));
        assert!(!first.changed, "the first read");
        assert!(
            !file_token(file, true, &cache).changed,
            "read again, the same token"
        );

        fs::write(&path, "token-2").unwrap();
        let read = file_token(file, false, &cache);
        assert_eq!(read.token.as_deref(), Some("token-2"));
        assert!(read.changed);
        assert!(
            !file_token(file, false, &cache).changed,
            "seen by the read before"
        );
        assert!(!file_token(file, true, &cache).changed);

        fs::write(&path, "").unwrap();
        let read = file_token(file, false, &cache);
        assert_eq!(read.token.as_deref(), Some("token-2"));
        assert!(!read.changed, "no token: the last one kept");
        fs::write(&path, "token-3").unwrap();
        assert!(file_token(file, false, &cache).changed);
        fs::remove_dir_all(&dir).unwrap();
    }
}
