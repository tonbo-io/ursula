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
    modified: Option<SystemTime>,
    len: Option<u64>,
    token: Option<String>,
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
    let path = token_file()?;
    let meta = fs::metadata(path).ok();
    let modified = meta.as_ref().and_then(|m| m.modified().ok());
    let len = meta.as_ref().map(fs::Metadata::len);
    let mut cached = lock(&FILE);
    if refresh
        || cached
            .as_ref()
            .is_none_or(|c| c.modified != modified || c.len != len)
    {
        let token = match fs::read_to_string(path) {
            Ok(text) => usable(&text),
            Err(e) => {
                log::emit(Level::Warn, "token_file_unreadable", &[
                    ("path", &path),
                    ("error", &e),
                ]);
                None
            }
        };
        *cached = Some(Cached {
            modified,
            len,
            token,
        });
    }
    cached.as_ref().and_then(|c| c.token.clone())
}

/// A token as `Authorization: Bearer` carries it: trimmed, non-empty, printable ASCII without
/// spaces.
fn usable(token: &str) -> Option<String> {
    let token = token.trim();
    (!token.is_empty() && token.bytes().all(|b| b.is_ascii_graphic())).then(|| token.to_owned())
}

#[cfg(test)]
mod tests {
    use super::usable;

    #[test]
    fn tokens_are_trimmed_printable_ascii() {
        assert_eq!(usable(" abc.def-_~+/=\n").as_deref(), Some("abc.def-_~+/="));
        assert_eq!(usable(""), None);
        assert_eq!(usable("  \n"), None);
        assert_eq!(usable("two words"), None);
        assert_eq!(usable("caf\u{e9}"), None);
    }
}
