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
//! extension records nothing and refuses nothing. With one, a host directory that cannot be opened
//! as a directory (a symlink or another file in its place, so the extension never writes through a
//! link), a missing record that cannot be written, or a record that is not the directory's fails
//! the owner open (`local_setup`), before it claims anything. An existing matching record is only
//! read, so a read-only, full or unwritable host directory refuses nothing an earlier open recorded.
//!
//! A record is a flat JSON object (every value a string, a number, a boolean or null), so a later
//! line may add only such keys: a reader takes its `dir` whatever its `v` and ignores the rest. It
//! is named by the first 32 hex digits of the BLAKE3 of the directory's real path. It is written
//! once (a temp file fsynced, linked into place, then the host directory fsynced) and never
//! changed: `link` never replaces a file, so a record is complete or absent. An open that finds the
//! record, or loses the race to link it, checks that it names the same directory, and fsyncs the
//! host directory too, so a record another process linked and never synced is durable before this
//! open logs anything.
//!
//! The host directory's path is resolved once, by the open of the directory itself (`O_DIRECTORY`
//! and `O_NOFOLLOW`, so its last component is no link). Every later step (the temp file's create,
//! the link, the unlink, the record's read and the fsync) goes through that descriptor (`at`),
//! so a path swapped after the open cannot redirect it. Only the cache directory's real path is
//! resolved by path, and it names a directory outside the host directory.

use std::ffi::c_uint;
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
use crate::inspection;
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
/// The host's lock file (`hold`, design: Hosts).
pub(crate) const HOLD: &str = "hold";
/// Names reserved for the host start so.
const HOST: &str = "host";
/// The most bytes of a record read back.
const MAX_RECORD_BYTES: usize = 64 << 10;
/// The record format's version.
pub(crate) const VERSION: u64 = 1;
/// A record's mode, and its temp file's.
const MODE: c_uint = 0o444;

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
    parse_record(bytes).map(|record| record.dir)
}

/// What a record holds: its `dir`, and its `v` when that is a whole number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Record {
    pub(crate) version: Option<u64>,
    pub(crate) dir: String,
}

/// The record `bytes` hold. Keys other than `v` and `dir` are ignored.
pub(crate) fn parse_record(bytes: &[u8]) -> Result<Record, Malformed> {
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(Malformed::TooLong);
    }
    let text = std::str::from_utf8(bytes).map_err(Malformed::NotUtf8)?;
    let mut version = None;
    let mut dir = None;
    for (key, value) in json::parse_object(text)? {
        match (key.as_str(), value) {
            // The first of each counts, as for any key a reader looks up.
            ("v", Value::Number(n)) if version.is_none() => version = json::whole(n),
            ("dir", Value::Text(text)) if dir.is_none() => dir = Some(text),
            _ => {}
        }
    }
    Ok(Record {
        version,
        dir: dir.ok_or(Malformed::NoDir)?,
    })
}

/// What a name in the host directory is (design: Hosts).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Entry {
    /// `dir-<32 hex>`: a record.
    Record,
    /// `.tmp-<pid>-<16 hex>`: a record being published.
    Tmp,
    /// `hold`, or a name starting `host`: the host's.
    Host,
    /// Anything else: written by nothing this line knows.
    Unrecognized,
}

/// What the name `name` in the host directory is.
pub(crate) fn entry(name: &str) -> Entry {
    let hex = |s: &str, len: usize| {
        s.len() == len
            && s.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    };
    if name.strip_prefix(RECORD).is_some_and(|key| hex(key, 32)) {
        return Entry::Record;
    }
    let tmp = name.strip_prefix(TMP).and_then(|rest| rest.split_once('-'));
    if tmp.is_some_and(|(pid, suffix)| {
        !pid.is_empty() && pid.bytes().all(|b| b.is_ascii_digit()) && hex(suffix, 16)
    }) {
        return Entry::Tmp;
    }
    if name == HOLD || name.starts_with(HOST) {
        return Entry::Host;
    }
    Entry::Unrecognized
}

/// The writer's PID that the temp file's name `name` (`.tmp-<pid>-<16 hex>`) carries, as its own
/// PID namespace numbers it. `None` for another name, or a number that is not a PID `kill` can
/// probe ([`inspection::process_id`]): 0, or one above `pid_t`'s maximum.
pub(crate) fn tmp_pid(name: &str) -> Option<u32> {
    if entry(name) != Entry::Tmp {
        return None;
    }
    let (pid, _suffix) = name.strip_prefix(TMP)?.split_once('-')?;
    let pid: u32 = pid.parse().ok()?;
    inspection::process_id(pid).map(|_probed| pid)
}

/// Removes the file `name` from the open host directory `dir`, through its descriptor and never
/// through a link (`unlinkat`): a host's prune of a record or a temp file.
pub(crate) fn unlink(dir: &File, name: &str) -> std::io::Result<()> {
    at::unlink(dir, name)
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
    let dir = open_dir(host).map_err(io(host, "open", host))?;
    let real = fs::canonicalize(cache).map_err(io(host, "resolve", cache))?;
    let cache = utf8(host, real)?;
    let name = record_name(&cache);
    match read(&dir, &name) {
        Ok(existing) => check(host, &name, &cache, existing)?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => publish(&dir, host, &name, &cache)?,
        Err(e) => return Err(io(host, "read", &host.join(&name))(e)),
    }
    dir.sync_all().map_err(io(host, "fsync", host))?;
    abort_at(AbortStep::HostDirSynced);
    Ok(())
}

/// Opens the host directory `host`: a directory, not a link to one and not another file, as the
/// kernel says at the open.
pub(crate) fn open_dir(host: &Path) -> std::io::Result<File> {
    File::options()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(host)
}

/// Opens the host's lock file in the host directory `host` read-only: through the directory's
/// descriptor, never through a link, and never blocking, so a FIFO in its place opens at once
/// (`O_NONBLOCK`, which a regular file ignores) and the caller refuses it.
pub(crate) fn open_hold(host: &Path) -> std::io::Result<File> {
    let dir = open_dir(host)?;
    at::open(
        &dir,
        HOLD,
        libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        0,
    )
}

/// What a record's name holds.
pub(crate) enum Existing {
    /// A regular file's bytes, at most one past [`MAX_RECORD_BYTES`].
    File(Vec<u8>),
    /// Anything else, unread.
    NotAFile,
}

/// What the record's name `name` in the host directory `dir` holds. Never through a link, and never
/// blocking: a FIFO opens at once (`O_NONBLOCK`, which a regular file ignores) and only a regular
/// file is read.
pub(crate) fn read(dir: &File, name: &str) -> std::io::Result<Existing> {
    let flags = libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK;
    let file = at::open(dir, name, flags, 0)?;
    if !file.metadata()?.is_file() {
        return Ok(Existing::NotAFile);
    }
    let limit = u64::try_from(MAX_RECORD_BYTES.saturating_add(1)).unwrap_or(u64::MAX);
    let mut bytes = Vec::new();
    file.take(limit).read_to_end(&mut bytes)?;
    Ok(Existing::File(bytes))
}

/// What the record's name `name` holds, `existing`, is the record of `cache`, or the open fails.
fn check(host: &Path, name: &str, cache: &str, existing: Existing) -> Result<(), HostDirError> {
    let conflict = |found| HostDirError::Conflict {
        host: host.to_owned(),
        record: host.join(name),
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

/// Publishes the record of `cache` as `name` in the host directory `dir`: a temp file of mode 0444
/// written and fsynced, then linked into place. When another open linked it first, its record is
/// checked instead.
fn publish(dir: &File, host: &Path, name: &str, cache: &str) -> Result<(), HostDirError> {
    let random = random16().map_err(io(host, "read", Path::new(URANDOM)))?;
    let suffix: String = random.iter().take(8).map(|b| format!("{b:02x}")).collect();
    let tmp_name = format!("{TMP}{}-{suffix}", std::process::id());
    let tmp = host.join(&tmp_name);
    let flags = libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW;
    let mut file = at::open(dir, &tmp_name, flags, MODE).map_err(io(host, "create", &tmp))?;
    let written = fill(&mut file, host, &tmp, cache);
    drop(file);
    if let Err(e) = written {
        discard(dir, host, &tmp_name);
        return Err(e);
    }
    abort_at(AbortStep::HostTmpSynced);
    match at::link(dir, &tmp_name, name) {
        Ok(()) => {
            abort_at(AbortStep::HostLinked);
            at::unlink(dir, &tmp_name).map_err(io(host, "unlink", &tmp))
        }
        // An open of another stream in the same directory linked it first.
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            discard(dir, host, &tmp_name);
            let existing = read(dir, name).map_err(io(host, "read", &host.join(name)))?;
            check(host, name, cache, existing)
        }
        Err(e) => {
            discard(dir, host, &tmp_name);
            Err(io(host, "link", &host.join(name))(e))
        }
    }
}

/// Writes the record of `cache` into the temp file `tmp`, mode 0444 whatever the umask, and fsyncs
/// it.
fn fill(file: &mut File, host: &Path, tmp: &Path, cache: &str) -> Result<(), HostDirError> {
    file.set_permissions(Permissions::from_mode(MODE))
        .map_err(io(host, "chmod", tmp))?;
    file.write_all(record_line(cache).as_bytes())
        .map_err(io(host, "write", tmp))?;
    file.sync_all().map_err(io(host, "fsync", tmp))
}

/// Removes the temp file `tmp_name` of a publish that failed. One left behind is ignored by readers,
/// and a host in the writer's PID namespace deletes it once that PID is gone.
fn discard(dir: &File, host: &Path, tmp_name: &str) {
    if let Err(e) = at::unlink(dir, tmp_name) {
        log::emit(Level::Warn, "host_dir_leftover", &[
            ("path", &host.join(tmp_name).display()),
            ("error", &e),
        ]);
    }
}

mod at;
#[cfg(test)]
mod tests;
