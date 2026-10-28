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
//! (`key`), while the source is active: an open or a bound database may send requests (the
//! driver says so, [`requests_may_go_out`]) and the key's token is the one they carry, or a host
//! waits for the key's token. A `401` to its token ([`refused`]) brings the refresh forward. The
//! process holds one key: another one is refused while requests may go out, since they would all
//! carry its tokens from then on. Neither the key nor a token is ever logged or returned.
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

/// The process's key and its schedule.
struct KeySource {
    key: Key,
    schedule: Schedule,
    /// Hosts waiting in [`use_key`] for its token.
    waiters: usize,
}

/// The key, once a host gave one.
static KEY: Mutex<Option<KeySource>> = Mutex::new(None);
/// Signalled under [`KEY`]'s lock when the key, its schedule or whether it is active changes.
static KEY_CHANGED: Condvar = Condvar::new();
/// Whether an open or a bound database may send requests ([`requests_may_go_out`]).
static REQUESTING: AtomicBool = AtomicBool::new(false);
/// The refresher thread, started with the first key. `None` when it could not be made.
static REFRESHER: OnceLock<Option<JoinHandle<()>>> = OnceLock::new();

/// Milliseconds on the process's clock for key schedules.
fn clock_ms() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    let start = START.get_or_init(Instant::now);
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// A draw from the random device, or from the clock without one.
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

/// The driver says whether an open or a bound database may send requests: a key's exchanges run
/// only then (or while a host waits for its token).
pub(crate) fn requests_may_go_out(requesting: bool) {
    if REQUESTING.swap(requesting, Ordering::AcqRel) != requesting && requesting {
        wake_refresher();
    }
}

/// A request that carried `token` was refused `401`: when it is the key's token, the key is
/// exchanged again at once (at least 1 s after the token arrived, `key`).
pub(crate) fn refused(token: &str) {
    let moved = lock(&KEY)
        .as_mut()
        .is_some_and(|k| k.schedule.refused(token, clock_ms(), jitter()));
    if moved {
        KEY_CHANGED.notify_all();
    }
}

/// Why a host's key gave it no token.
#[derive(Debug, thiserror::Error)]
pub(crate) enum KeyError {
    #[error("api_key: {0}")]
    Refused(#[from] KeyRefusal),
    /// Another key, while requests may go out with this one's tokens.
    #[error(
        "api_key: the process holds another API key, whose tokens its databases' requests carry: one key per process"
    )]
    AnotherKey,
    /// The exchange failed in a way another try will not fix.
    #[error("{0}")]
    Exchange(#[source] ExchangeError),
    #[error(
        "no token within {timeout_ms} ms{}",
        .last.as_ref().map(|e| format!(": {e}")).unwrap_or_default()
    )]
    TimedOut {
        timeout_ms: u64,
        last: Option<ExchangeError>,
    },
    #[error("the refresher thread could not be started")]
    NoRefresher,
}

/// Whether a host's key `next` can be the process's key, where it holds `held` and requests may
/// go out (`in_use`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Admit {
    /// The key held: its token and schedule serve on.
    Same,
    /// A first key, or another one while no request may go out: it starts afresh.
    Replace,
    /// Another key while requests may go out: refused.
    Refuse,
}

fn admit(held: Option<&Key>, next: &Key, in_use: bool) -> Admit {
    match held {
        Some(held) if held == next => Admit::Same,
        Some(_) if in_use => Admit::Refuse,
        Some(_) | None => Admit::Replace,
    }
}

/// Makes `secret`, exchanged at `endpoint` for tokens of `audience`, the process's key, and waits
/// at most `timeout` for a token of it that has not expired. A token held serves at once. A key
/// whose exchange is refused fails at once, and other failures are retried until the time runs
/// out. Another key than the one held fails while an open or a bound database may send requests,
/// or another host waits for the held key's token.
#[expect(
    dead_code,
    reason = "the Tonbo resolution installs the key, in the change that adds it"
)]
pub(crate) fn use_key(
    secret: &str,
    endpoint: &str,
    audience: Option<&str>,
    timeout: Duration,
) -> Result<(), KeyError> {
    let key = Key::new(secret, endpoint, audience)?;
    let timeout_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
    let deadline = clock_ms().saturating_add(timeout_ms);
    let mut held = lock(&KEY);
    let in_use = REQUESTING.load(Ordering::Acquire) || held.as_ref().is_some_and(|k| k.waiters > 0);
    match admit(held.as_ref().map(|k| &k.key), &key, in_use) {
        Admit::Refuse => return Err(KeyError::AnotherKey),
        Admit::Same => {}
        Admit::Replace => {
            *held = Some(KeySource {
                key,
                schedule: Schedule::new(clock_ms()),
                waiters: 0,
            });
        }
    }
    if REFRESHER.get_or_init(start_refresher).is_none() {
        return Err(KeyError::NoRefresher);
    }
    let since = match held.as_mut() {
        Some(k) => {
            k.schedule.wanted(clock_ms());
            k.waiters = k.waiters.saturating_add(1);
            k.schedule.ended()
        }
        None => return Err(KeyError::NoRefresher),
    };
    KEY_CHANGED.notify_all();
    let outcome = loop {
        let now = clock_ms();
        let Some(k) = held.as_ref() else {
            break Err(KeyError::NoRefresher);
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
        if now >= deadline {
            break Err(KeyError::TimedOut {
                timeout_ms,
                last: last.cloned(),
            });
        }
        held = wait_until(held, Some(deadline), now);
    };
    if let Some(k) = held.as_mut() {
        k.waiters = k.waiters.saturating_sub(1);
    }
    outcome
}

/// Starts the refresher thread.
fn start_refresher() -> Option<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("ursula-vfs-token".to_owned())
        .spawn(|| {
            crate::guard::contain("token_refresher", || (), refresh);
        })
        .inspect_err(|e| {
            log::emit(Level::Error, "token_refresher_failed", &[("error", e)]);
        })
        .ok()
}

/// The refresher's loop: exchanges the key whenever its schedule says so while it is active,
/// outside the lock, and otherwise waits for the schedule's next deadline or a change.
fn refresh() {
    let mut held = lock(&KEY);
    loop {
        let serving = held.is_some() && lock(&SET).is_none() && token_file().is_none();
        let now = clock_ms();
        let (key, deadline) = match held.as_mut() {
            Some(k) => {
                let active = k.waiters > 0 || (serving && REQUESTING.load(Ordering::Acquire));
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
            || {
                Err(ExchangeError::Unreachable {
                    reason: "the exchange panicked inside the extension".to_owned(),
                })
            },
            || exchange::exchange(&key),
        );
        held = lock(&KEY);
        // A key replaced meanwhile drops the outcome: the new key's schedule starts afresh.
        let changed = match held.as_mut().filter(|k| k.key == key) {
            Some(k) => {
                let failed = outcome.as_ref().err().cloned();
                let changed = k.schedule.exchanged(outcome, clock_ms(), jitter());
                if let Some(e) = failed {
                    let view = k.schedule.view(clock_ms());
                    if view.failures == 1 {
                        log::emit(Level::Warn, "token_exchange_failed", &[
                            ("kind", &e.kind().as_str()),
                            ("error", &e),
                            ("kept_previous", &k.schedule.token().is_some()),
                        ]);
                    }
                }
                changed
            }
            None => false,
        };
        KEY_CHANGED.notify_all();
        if changed && serving {
            drop(held);
            crate::driver::runtime::token_changed();
            held = lock(&KEY);
        }
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
    use super::admit;
    use super::exchange::Key;
    use super::file_token;
    use super::lend;
    use super::put_back;
    use super::usable;

    // One key per process: the key held serves on, another replaces it only while no request may
    // go out, since every request would carry the new key's tokens from then on.
    #[test]
    fn another_key_is_refused_while_requests_may_go_out() {
        let key = |secret: &str, audience: &str| {
            Key::new(secret, "https://auth.example/token", Some(audience)).unwrap()
        };
        let held = key("k1", "a");
        assert_eq!(admit(None, &held, true), Admit::Replace);
        assert_eq!(admit(Some(&held), &key("k1", "a"), true), Admit::Same);
        assert_eq!(admit(Some(&held), &key("k2", "a"), true), Admit::Refuse);
        assert_eq!(admit(Some(&held), &key("k1", "b"), true), Admit::Refuse);
        assert_eq!(admit(Some(&held), &key("k2", "a"), false), Admit::Replace);
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
