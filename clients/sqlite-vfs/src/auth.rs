//! The bearer token every request carries: the one the application set (`ursula_set_token`), else
//! the content of `URSULA_VFS_TOKEN_FILE`, read again whenever the file changes and after the
//! endpoint refused a token. One token per process.

use std::fs;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::time::SystemTime;

use crate::config::token_file;
use crate::log;
use crate::log::Level;

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
    *lock(&SET) = token.and_then(usable);
}

/// The token for the next request. `refresh`: read the token file again even if it looks unchanged
/// (after the endpoint refused the token).
pub(crate) fn token(refresh: bool) -> Option<String> {
    if let Some(token) = lock(&SET).clone() {
        return Some(token);
    }
    file_token(token_file()?, refresh, &FILE)
}

/// The token in the file at `path`, read again when it changed (or on `refresh`), outside the
/// cache's lock. A read that finds no usable token (the file is missing, empty, or caught
/// half-written by a writer that truncates it first) keeps the last token the file held: a
/// request sent without one is refused as if the stream did not exist (an authorizer conceals it
/// with a 404), which fails an attach and poisons a commit at once, whereas a stale token is
/// refused with a 401, which is retried with the file read again.
fn file_token(path: &str, refresh: bool, cache: &Mutex<Option<Cached>>) -> Option<String> {
    let meta = fs::metadata(path).ok();
    let modified = meta.as_ref().and_then(|m| m.modified().ok());
    let len = meta.as_ref().map(fs::Metadata::len);
    if !refresh
        && let Some(c) = lock(cache)
            .as_ref()
            .filter(|c| c.modified == modified && c.len == len)
    {
        return c.token.clone();
    }
    let read = fs::read_to_string(path)
        .map_err(Unusable::Read)
        .and_then(|text| usable(&text).ok_or(Unusable::NoToken));
    let mut cached = lock(cache);
    let last = cached.as_ref().and_then(|c| c.token.clone());
    let (token, unusable) = match read {
        Ok(token) => (Some(token), false),
        Err(e) => {
            if !cached.as_ref().is_some_and(|c| c.unusable) {
                log::emit(Level::Warn, "token_file_unreadable", &[
                    ("path", &path),
                    ("error", &e),
                    ("kept_previous", &last.is_some()),
                ]);
            }
            (last, true)
        }
    };
    *cached = Some(Cached {
        modified,
        len,
        token: token.clone(),
        unusable,
    });
    token
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
    use super::usable;

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
        assert_eq!(file_token(file, true, &cache), None);
        fs::write(&path, "t1\n").unwrap();
        assert_eq!(file_token(file, true, &cache).as_deref(), Some("t1"));
        fs::write(&path, "").unwrap();
        assert_eq!(file_token(file, true, &cache).as_deref(), Some("t1"));
        fs::write(&path, "half written").unwrap();
        assert_eq!(file_token(file, true, &cache).as_deref(), Some("t1"));
        fs::remove_file(&path).unwrap();
        assert_eq!(file_token(file, true, &cache).as_deref(), Some("t1"));
        fs::write(&path, "t2").unwrap();
        assert_eq!(file_token(file, true, &cache).as_deref(), Some("t2"));
        // Unchanged since the last read: the cached token, without reading the file.
        assert_eq!(file_token(file, false, &cache).as_deref(), Some("t2"));
        fs::remove_dir_all(&dir).unwrap();
    }
}
