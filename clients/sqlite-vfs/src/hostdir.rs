//! The host directory (design: Hosts): where an owner open records the cache directory it locks a
//! stream's root in, before the root can hold a commit, so a host that snapshots or moves the
//! process, or the VM it runs in, knows every directory whose roots may hold unacknowledged
//! commits.
//!
//! ```text
//! <host dir>/                 created by the host, e.g. owned by root with mode 1777
//!   dir-<32 hex>              a record: {"v":1,"dir":"<real path>"} and LF, mode 0444
//!   .tmp-<pid>-<16 hex>       a record being published
//!   hold                      the host's lock file
//!   host<anything>            reserved for the host
//! ```
//!
//! The host directory is [`DEFAULT`] when `lstat` finds anything there, whatever its type, and
//! otherwise the test hook `URSULA_VFS_HOST_DIR` when `lstat` finds that. Without either the
//! extension records nothing and refuses nothing. With one, a record that cannot be written fails
//! the owner open (`local_setup`), before it claims anything: a symlink or a file that is not a
//! directory included, so the extension never writes through a link.
//!
//! A record is a flat JSON object (every value a string, a number, a boolean or null), so a later
//! line may add only such keys: a reader takes its `dir` whatever its `v` and ignores the rest. It
//! is named by the first 32 hex digits of the BLAKE3 of the directory's real path. It is written
//! once (a temp file fsynced, linked into place, then the host directory fsynced) and never
//! changed: `link` never replaces a file, so a record is complete or absent. An open that finds the
//! record, or loses the race to link it, checks that it names the same directory, and fsyncs the
//! host directory too, so a record another process linked and never synced is durable before this
//! open logs anything.

use std::fs;
use std::fs::File;
use std::fs::Permissions;
use std::io::Read;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;
use std::str::Utf8Error;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use crate::config;
use crate::config::Step as AbortStep;
use crate::config::abort_at;
use crate::ids::URANDOM;
use crate::ids::random16;
use crate::json;
use crate::json::SyntaxError;
use crate::json::Value;
use crate::log;
use crate::log::Level;

/// The compiled-in host directory. No process can move away from it, as it can from its
/// environment.
pub(crate) const DEFAULT: &str = "/var/lib/sqlite-ursula/host";
/// A record's name before its key.
const RECORD: &str = "dir-";
/// A temp file's name before `<pid>-<16 hex>`.
const TMP: &str = ".tmp-";
/// The most bytes of a record read back.
const MAX_RECORD_BYTES: usize = 64 << 10;
/// The record format's version.
const VERSION: u64 = 1;

/// A host directory that cannot take the record of an owner open's cache directory. The open
/// fails `local_setup`, having claimed nothing.
#[derive(Debug, thiserror::Error)]
pub(crate) enum HostDirError {
    #[error("the host directory {}: {op} {}: {source}", .host.display(), .path.display())]
    Io {
        host: PathBuf,
        op: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// The record under the cache directory's name is not its record.
    #[error("the host directory {}: {} {found}, not {cache:?}", .host.display(), .record.display())]
    Conflict {
        host: PathBuf,
        record: PathBuf,
        /// The cache directory's real path.
        cache: String,
        found: Found,
    },
    /// A record holds UTF-8 JSON, so it cannot name this directory.
    #[error("the host directory {}: the real path of the cache directory, {}, is not UTF-8", .host.display(), .cache.display())]
    NotUtf8 { host: PathBuf, cache: PathBuf },
}

/// What a conflicting record holds.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Found {
    #[error("records {0:?}")]
    Other(String),
    /// A FIFO, a socket, a device or a directory: never read, so it cannot block the open.
    #[error("is not a regular file")]
    NotAFile,
    #[error("is not a record: {0}")]
    Malformed(#[source] Malformed),
}

/// Why bytes are not a record.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Malformed {
    #[error("longer than {MAX_RECORD_BYTES} bytes")]
    TooLong,
    #[error("not UTF-8: {0}")]
    NotUtf8(#[source] Utf8Error),
    #[error(transparent)]
    Syntax(#[from] SyntaxError),
    #[error("no `dir` string")]
    NoDir,
}

/// Records the cache directory `cache` of an owner open that locked a root in it, when this
/// process has a host directory (design: Hosts).
pub(crate) fn record(cache: &Path) -> Result<(), HostDirError> {
    match host_dir() {
        Some(host) => record_dir(&host, cache),
        None => Ok(()),
    }
}

/// The host directory, resolved now: [`DEFAULT`] when `lstat` finds it, else the test hook
/// `URSULA_VFS_HOST_DIR` when `lstat` finds that, else none.
pub(crate) fn host_dir() -> Option<PathBuf> {
    resolve(
        Path::new(DEFAULT),
        &config::host_dir_override,
        &|path: &Path| fs::symlink_metadata(path).map(drop),
    )
}

/// [`host_dir`] with the default, the hook and `lstat` given. The hook is read only when the
/// default is absent. A default `lstat` cannot reach (a confinement policy's `EACCES`) counts as
/// absent: an error other than a missing path is logged once (`host_dir_unreadable`).
pub(crate) fn resolve<'a>(
    default: &Path,
    hook: &dyn Fn() -> Option<&'a Path>,
    lstat: &dyn Fn(&Path) -> std::io::Result<()>,
) -> Option<PathBuf> {
    match lstat(default) {
        Ok(()) => return Some(default.to_owned()),
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
            ) => {}
        Err(e) => unreadable_once(default, &e),
    }
    let hook = hook()?;
    lstat(hook).ok().map(|()| hook.to_owned())
}

fn unreadable_once(path: &Path, e: &std::io::Error) {
    static LOGGED: AtomicBool = AtomicBool::new(false);
    if !LOGGED.swap(true, Ordering::Relaxed) {
        log::emit(Level::Warn, "host_dir_unreadable", &[
            ("path", &path.display()),
            ("error", e),
        ]);
    }
}

/// The record's name for the cache directory whose real path is `cache`.
pub(crate) fn record_name(cache: &str) -> String {
    let hash = blake3::hash(cache.as_bytes()).to_hex();
    format!("{RECORD}{}", hash.as_str().get(..32).unwrap_or_default())
}

/// The record of the cache directory whose real path is `cache`: one line of JSON.
pub(crate) fn record_line(cache: &str) -> String {
    let object = json::object(&[("v", VERSION.to_string()), ("dir", json::str(cache))]);
    format!("{object}\n")
}

/// The directory a record names, whatever its `v`: a later line's record of the same directory
/// serves this one too, and keys it does not know are ignored.
pub(crate) fn recorded_dir(bytes: &[u8]) -> Result<String, Malformed> {
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(Malformed::TooLong);
    }
    let text = std::str::from_utf8(bytes).map_err(Malformed::NotUtf8)?;
    json::parse_object(text)?
        .into_iter()
        .find_map(|(key, value)| match (key.as_str(), value) {
            ("dir", Value::Text(dir)) => Some(dir),
            _ => None,
        })
        .ok_or(Malformed::NoDir)
}

/// The real path `real` of a cache directory as text.
fn utf8(host: &Path, real: PathBuf) -> Result<String, HostDirError> {
    real.into_os_string()
        .into_string()
        .map_err(|cache| HostDirError::NotUtf8 {
            host: host.to_owned(),
            cache: PathBuf::from(cache),
        })
}

fn io(host: &Path, op: &'static str, path: &Path) -> impl FnOnce(std::io::Error) -> HostDirError {
    let (host, path) = (host.to_owned(), path.to_owned());
    move |source| HostDirError::Io {
        host,
        op,
        path,
        source,
    }
}

/// Records the cache directory `cache` in the host directory `host` (design: Hosts): checks the
/// record that exists, or publishes it, then fsyncs `host`.
pub(crate) fn record_dir(host: &Path, cache: &Path) -> Result<(), HostDirError> {
    // A directory, not a link to one and not another file: the kernel says so at the open.
    let dir = File::options()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(host)
        .map_err(io(host, "open", host))?;
    let real = fs::canonicalize(cache).map_err(io(host, "resolve", cache))?;
    let cache = utf8(host, real)?;
    let record = host.join(record_name(&cache));
    match read(&record) {
        Ok(existing) => check(host, &record, &cache, existing)?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => publish(host, &record, &cache)?,
        Err(e) => return Err(io(host, "read", &record)(e)),
    }
    dir.sync_all().map_err(io(host, "fsync", host))?;
    abort_at(AbortStep::HostDirSynced);
    Ok(())
}

/// What a record's name holds.
enum Existing {
    /// A regular file's bytes, at most one past [`MAX_RECORD_BYTES`].
    File(Vec<u8>),
    /// Anything else, unread.
    NotAFile,
}

/// What the record's name `path` holds. Never through a link, and never blocking: a FIFO opens at
/// once (`O_NONBLOCK`, which a regular file ignores) and only a regular file is read.
fn read(path: &Path) -> std::io::Result<Existing> {
    let file = File::options()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Ok(Existing::NotAFile);
    }
    let limit = u64::try_from(MAX_RECORD_BYTES.saturating_add(1)).unwrap_or(u64::MAX);
    let mut bytes = Vec::new();
    file.take(limit).read_to_end(&mut bytes)?;
    Ok(Existing::File(bytes))
}

/// What the record's name `record` holds, `existing`, is the record of `cache`, or the open fails.
fn check(host: &Path, record: &Path, cache: &str, existing: Existing) -> Result<(), HostDirError> {
    let conflict = |found| HostDirError::Conflict {
        host: host.to_owned(),
        record: record.to_owned(),
        cache: cache.to_owned(),
        found,
    };
    let bytes = match existing {
        Existing::File(bytes) => bytes,
        Existing::NotAFile => return Err(conflict(Found::NotAFile)),
    };
    match recorded_dir(&bytes) {
        Ok(dir) if dir == cache => Ok(()),
        Ok(other) => Err(conflict(Found::Other(other))),
        Err(malformed) => Err(conflict(Found::Malformed(malformed))),
    }
}

/// Publishes the record of `cache` at `record`: a temp file of mode 0444 written and fsynced, then
/// linked into place. When another open linked it first, its record is checked instead.
fn publish(host: &Path, record: &Path, cache: &str) -> Result<(), HostDirError> {
    let random = random16().map_err(io(host, "read", Path::new(URANDOM)))?;
    let suffix: String = random.iter().take(8).map(|b| format!("{b:02x}")).collect();
    let tmp = host.join(format!("{TMP}{}-{suffix}", std::process::id()));
    let mut file = File::options()
        .write(true)
        .create_new(true)
        .mode(0o444)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&tmp)
        .map_err(io(host, "create", &tmp))?;
    let written = fill(&mut file, host, &tmp, cache);
    drop(file);
    if let Err(e) = written {
        discard(&tmp);
        return Err(e);
    }
    abort_at(AbortStep::HostTmpSynced);
    match fs::hard_link(&tmp, record) {
        Ok(()) => {
            abort_at(AbortStep::HostLinked);
            fs::remove_file(&tmp).map_err(io(host, "unlink", &tmp))
        }
        // An open of another stream in the same directory linked it first.
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            discard(&tmp);
            let existing = read(record).map_err(io(host, "read", record))?;
            check(host, record, cache, existing)
        }
        Err(e) => {
            discard(&tmp);
            Err(io(host, "link", record)(e))
        }
    }
}

/// Writes the record of `cache` into the temp file `tmp`, mode 0444 whatever the umask, and fsyncs
/// it.
fn fill(file: &mut File, host: &Path, tmp: &Path, cache: &str) -> Result<(), HostDirError> {
    file.set_permissions(Permissions::from_mode(0o444))
        .map_err(io(host, "chmod", tmp))?;
    file.write_all(record_line(cache).as_bytes())
        .map_err(io(host, "write", tmp))?;
    file.sync_all().map_err(io(host, "fsync", tmp))
}

/// Removes the temp file of a publish that failed. One left behind is ignored by readers, and a
/// host deletes it once its process is gone.
fn discard(tmp: &Path) {
    if let Err(e) = fs::remove_file(tmp) {
        log::emit(Level::Warn, "host_dir_leftover", &[
            ("path", &tmp.display()),
            ("error", &e),
        ]);
    }
}

#[cfg(test)]
mod tests;
