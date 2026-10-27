//! The bearer token every request carries: the one the application set (`ursula_set_token`), else
//! the content of `URSULA_VFS_TOKEN_FILE`, read again whenever the file changes and after the
//! endpoint refused a token. One token per process. A change of it sends again at once every
//! request a `401` blocked ([`crate::driver::runtime::token_changed`]): a token set at once, and a
//! file whose token changed when a read finds the new one, by a request or by the driver's check
//! every [`FILE_CHECK_MS`] while requests carry it ([`watching`], [`check_file`]).

use std::fs;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::time::SystemTime;

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

/// The token requests carry changed: the requests a `401` blocked go again at once.
fn changed() {
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
    let read = file_token(token_file()?, refresh, &FILE);
    if read.changed {
        changed();
    }
    read.token
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

    use super::file_token;
    use super::lend;
    use super::put_back;
    use super::usable;

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
