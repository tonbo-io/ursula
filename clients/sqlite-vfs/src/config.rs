//! Settings and test hooks read from the environment (`URSULA_VFS_*`), once per process.

use std::path::Path;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::PoisonError;
use std::time::Duration;
use std::time::Instant;

use crate::capture::DEFAULT_MAX_COMMIT_BYTES;
use crate::capture::MAX_COMMIT_BYTES;
use crate::capture::MIN_COMMIT_BYTES;
use crate::log;
use crate::log::Level;

/// `URSULA_VFS_IDLE_MS`: how long a connection is kept after its last request, for the next one
/// to reuse, in milliseconds (90 s by default). 0 never reuses one. A value that is not a number
/// of milliseconds keeps the default, logged once (`setting_adjusted`).
pub(crate) fn idle_age() -> Duration {
    const DEFAULT_MS: u64 = 90_000;
    static V: OnceLock<Duration> = OnceLock::new();
    *V.get_or_init(|| {
        let Ok(value) = std::env::var("URSULA_VFS_IDLE_MS") else {
            return Duration::from_millis(DEFAULT_MS);
        };
        let ms = value.trim().parse().unwrap_or_else(|_not_a_number| {
            adjusted("URSULA_VFS_IDLE_MS", &value, &DEFAULT_MS);
            DEFAULT_MS
        });
        Duration::from_millis(ms)
    })
}

fn adjusted(setting: &'static str, value: &str, used: &dyn std::fmt::Display) {
    log::emit(Level::Warn, "setting_adjusted", &[
        ("setting", &setting),
        ("value", &value),
        ("used", used),
    ]);
}

/// `URSULA_VFS_PANIC_IN`: what panics on purpose at every call, for tests of `guard::contain`: a
/// file method by its name (`x_write`, `x_close`, ...), a SQL function (`ursula_status`, ...) or
/// `sqlite3_extension_init`. Unset outside tests. Unit tests set it per thread instead
/// (`set_panic_in_for_tests`).
pub(crate) fn panic_in() -> Option<&'static str> {
    #[cfg(test)]
    if let Some(entry) = PANIC_IN_FOR_TESTS.get() {
        return Some(entry);
    }
    static V: OnceLock<Option<String>> = OnceLock::new();
    V.get_or_init(|| std::env::var("URSULA_VFS_PANIC_IN").ok())
        .as_deref()
}

#[cfg(test)]
thread_local! {
    static PANIC_IN_FOR_TESTS: std::cell::Cell<Option<&'static str>> =
        const { std::cell::Cell::new(None) };
}

/// `panic_in()` on this thread, for unit tests: the environment is read once per process.
#[cfg(test)]
pub(crate) fn set_panic_in_for_tests(entry: Option<&'static str>) {
    PANIC_IN_FOR_TESTS.set(entry);
}

/// A step of the commit path, of a checkpoint, of a re-key, of a trim or cut of the log, of a
/// generation's copy or of its publish that `URSULA_VFS_ABORT_AT` can stop the process at (design:
/// Capture, Fsyncs, Layout, Materialize, History).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Step {
    /// A write transaction's first WAL write, admitted.
    Admission,
    /// The commit point, before the capture reads the frames back.
    Capture,
    /// The commit frame encoded, before the log write.
    Encode,
    /// The COMMIT record written, before the driver hears of it.
    Logged,
    /// The commit frame's page forwarded to the WAL.
    Forward,
    /// The commit confirmed by `SQLITE_FCNTL_COMMIT_PHASETWO`.
    PhaseTwo,
    /// A PART run: before a part after the first goes out (each resend counts), so the run's
    /// earlier parts may have landed and the rest never do.
    PartSent,
    /// F1: a checkpoint starts, before the log fdatasync.
    CheckpointStart,
    /// F1: the log fdatasynced, before the first db page.
    CheckpointSynced,
    /// F1: the first db page of a checkpoint written.
    CheckpointPage,
    /// F2: before the db fsync.
    BaseStart,
    /// F2: the db fsynced, before the BASE record.
    BaseSynced,
    /// F2: the BASE record written.
    Based,
    /// A re-key: the log rolled to the segment naming the new token, before its CLAIM record.
    RekeyRolled,
    /// A re-key: the CLAIM record fsynced in the new segment, before the claim goes out.
    RekeyClaimed,
    /// A publish that salvages: the manifest fsynced into the superseded generation, before the
    /// new one is renamed into place.
    SalvageMarked,
    /// A publish: the new generation renamed into place and the root fsynced.
    PublishRenamed,
    /// A publish that salvages: the superseded generation moved into `salvage/`, before its magic
    /// is restored.
    SalvageMoved,
    /// A copy of a held generation: its db file copied, before its log.
    CopyDb,
    /// A copy of a held generation: its log copied, before the copy is published.
    CopyLog,
    /// F3: a trim unlinked a segment and fsynced the directory, before the next one goes.
    TrimUnlinked,
    /// An open cutting the log after its valid prefix: the later segments removed and the
    /// directory fsynced, before the current segment is truncated.
    CutRemoved,
}

impl Step {
    fn name(self) -> &'static str {
        match self {
            Step::Admission => "admission",
            Step::Capture => "capture",
            Step::Encode => "encode",
            Step::Logged => "logged",
            Step::Forward => "forward",
            Step::PhaseTwo => "phasetwo",
            Step::PartSent => "part_sent",
            Step::CheckpointStart => "checkpoint_start",
            Step::CheckpointSynced => "checkpoint_synced",
            Step::CheckpointPage => "checkpoint_page",
            Step::BaseStart => "base_start",
            Step::BaseSynced => "base_synced",
            Step::Based => "based",
            Step::RekeyRolled => "rekey_rolled",
            Step::RekeyClaimed => "rekey_claimed",
            Step::SalvageMarked => "salvage_marked",
            Step::PublishRenamed => "publish_renamed",
            Step::SalvageMoved => "salvage_moved",
            Step::CopyDb => "copy_db",
            Step::CopyLog => "copy_log",
            Step::TrimUnlinked => "trim_unlinked",
            Step::CutRemoved => "cut_removed",
        }
    }
}

/// Test hook `URSULA_VFS_ABORT_AT=<step>[:<n>]`: kills the process with SIGKILL the `n`-th time (1
/// by default) it reaches `step` ([`Step`], by its name), as a crash there would stop it (design:
/// Verification). SIGKILL rather than `abort()`: nothing of the process runs after it, and no crash
/// report or core is written.
pub(crate) fn abort_at(step: Step) {
    static V: OnceLock<Option<(String, u64)>> = OnceLock::new();
    static SEEN: Mutex<u64> = Mutex::new(0);
    let Some((name, n)) = V.get_or_init(|| {
        let value = std::env::var("URSULA_VFS_ABORT_AT").ok()?;
        let (name, n) = value.split_once(':').unwrap_or((value.as_str(), "1"));
        Some((name.to_owned(), n.parse().unwrap_or(1)))
    }) else {
        return;
    };
    if name != step.name() {
        return;
    }
    let mut seen = SEEN.lock().unwrap_or_else(PoisonError::into_inner);
    *seen = seen.saturating_add(1);
    if *seen == *n {
        log::emit(Level::Error, "test_hook_abort", &[
            ("hook", &"URSULA_VFS_ABORT_AT"),
            ("step", &step.name()),
            ("n", n),
        ]);
        kill_self();
    }
}

/// SIGKILLs this process. `abort()` if that fails, which it cannot for a process signalling itself.
#[expect(unsafe_code, reason = "raising a signal has no safe std API")]
fn kill_self() -> ! {
    // SAFETY: `raise` takes a signal number and touches no memory of ours.
    unsafe { libc::raise(libc::SIGKILL) };
    std::process::abort()
}

/// `URSULA_VFS_MAX_REQUEST_BYTES`: a request limit below the server's, for a layer in front of it
/// that refuses larger bodies (design: Appends). Unset, or not a number of bytes (logged once as
/// `setting_adjusted`): the server's alone.
pub(crate) fn max_request_bytes() -> Option<u64> {
    static V: OnceLock<Option<u64>> = OnceLock::new();
    *V.get_or_init(|| {
        let value = std::env::var("URSULA_VFS_MAX_REQUEST_BYTES").ok()?;
        match value.trim().parse::<u64>() {
            Ok(bytes) if bytes > 0 => Some(bytes),
            _ => {
                adjusted(
                    "URSULA_VFS_MAX_REQUEST_BYTES",
                    &value,
                    &"the server's limit",
                );
                None
            }
        }
    })
}

/// `URSULA_VFS_MAX_COMMIT_BYTES`: the most bytes of pages one commit may hold, 128 MiB by default
/// (design: Capture). A value outside 4096 ..= 1 GiB is clamped to the nearest bound, and 0 or a
/// value that is not a number of bytes keeps the default, each logged once (`setting_adjusted`).
pub(crate) fn max_commit_bytes() -> u64 {
    static V: OnceLock<u64> = OnceLock::new();
    *V.get_or_init(|| {
        let Ok(value) = std::env::var("URSULA_VFS_MAX_COMMIT_BYTES") else {
            return DEFAULT_MAX_COMMIT_BYTES;
        };
        let (bytes, changed) = commit_limit(&value);
        if changed {
            adjusted("URSULA_VFS_MAX_COMMIT_BYTES", &value, &bytes);
        }
        bytes
    })
}

/// The commit limit `value` sets, and whether it differs from what `value` says.
fn commit_limit(value: &str) -> (u64, bool) {
    match value.trim().parse::<u64>() {
        Ok(0) => (DEFAULT_MAX_COMMIT_BYTES, true),
        Err(_not_a_number) => (DEFAULT_MAX_COMMIT_BYTES, true),
        Ok(bytes) => {
            let used = bytes.clamp(MIN_COMMIT_BYTES, MAX_COMMIT_BYTES);
            (used, used != bytes)
        }
    }
}

/// Test hook `URSULA_VFS_SNAPSHOT_DUE_BYTES`: a snapshot is due once more frame bytes than this
/// are acknowledged since the latest one, instead of past the db file's size or 8 MiB (design:
/// Snapshots). Unset, or not a number of bytes: the design's threshold.
pub(crate) fn snapshot_due_bytes() -> Option<u64> {
    static V: OnceLock<Option<u64>> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("URSULA_VFS_SNAPSHOT_DUE_BYTES")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
    })
}

/// `URSULA_VFS_MIN_UPLOAD_KBPS`: the slowest upload, in KiB/s, an attempt's body time limit allows
/// for (256 by default, at least 1): a body of `n` bytes may take `5 s + n / rate` to send (design:
/// Transport, #566).
pub(crate) fn min_upload_kbps() -> u64 {
    static V: OnceLock<u64> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("URSULA_VFS_MIN_UPLOAD_KBPS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(256)
            .max(1)
    })
}

/// `URSULA_VFS_CA_FILE`: a PEM bundle of the certificate authorities trusted for `https://` stream
/// URLs, instead of the bundled Mozilla roots.
pub(crate) fn ca_file() -> Option<&'static str> {
    static V: OnceLock<Option<String>> = OnceLock::new();
    V.get_or_init(|| non_empty("URSULA_VFS_CA_FILE")).as_deref()
}

/// `URSULA_VFS_TOKEN_FILE`: a file holding the bearer token every request carries (see `auth`).
pub(crate) fn token_file() -> Option<&'static str> {
    static V: OnceLock<Option<String>> = OnceLock::new();
    V.get_or_init(|| non_empty("URSULA_VFS_TOKEN_FILE"))
        .as_deref()
}

/// `URSULA_VFS_FAIL_SYNC=<suffix>` test hook: `xSync` of a file outside the extension's databases
/// whose path ends with `suffix` fails, as a disk refusing a write-back would.
pub(crate) fn fail_sync() -> Option<&'static str> {
    static V: OnceLock<Option<String>> = OnceLock::new();
    V.get_or_init(|| non_empty("URSULA_VFS_FAIL_SYNC"))
        .as_deref()
}

fn non_empty(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// Test hook `URSULA_VFS_SNAPSHOT_HOLD=<dir>`: a snapshot attempt that copied the database creates
/// `<dir>/held` and keeps its private connection open until `<dir>/release` exists (30 s at most),
/// and creates `<dir>/dropped` once it closed it. A test closes and reopens the database while the
/// connection is open. Unset outside tests.
#[derive(Debug)]
pub(crate) struct SnapshotHold(Option<PathBuf>);

impl SnapshotHold {
    /// How long a hold waits for its release at most.
    const MAX: Duration = Duration::from_secs(30);

    /// Holds until the release, when the hook is set.
    pub(crate) fn begin() -> Self {
        static V: OnceLock<Option<PathBuf>> = OnceLock::new();
        let Some(dir) = V
            .get_or_init(|| non_empty("URSULA_VFS_SNAPSHOT_HOLD").map(PathBuf::from))
            .clone()
        else {
            return Self(None);
        };
        hook_file(&dir.join("held"));
        let release = dir.join("release");
        let until = Instant::now().checked_add(Self::MAX);
        while !release.exists() && until.is_some_and(|u| Instant::now() < u) {
            std::thread::sleep(Duration::from_millis(10));
        }
        Self(Some(dir))
    }
}

impl Drop for SnapshotHold {
    fn drop(&mut self) {
        if let Some(dir) = &self.0 {
            hook_file(&dir.join("dropped"));
        }
    }
}

/// Creates a test hook's signal file, logging a failure.
fn hook_file(path: &Path) {
    if let Err(e) = std::fs::write(path, b"") {
        log::emit(Level::Warn, "test_hook_failed", &[
            ("path", &path.display()),
            ("error", &e),
        ]);
    }
}

#[cfg(test)]
mod tests {
    use super::commit_limit;
    use crate::capture::DEFAULT_MAX_COMMIT_BYTES;

    // A commit limit in range is used as is. One outside 4096 ..= 1 GiB is clamped to the nearest
    // bound, and 0 or a value that is not a number keeps the default, each reported as adjusted.
    #[test]
    fn the_commit_limit_is_clamped_to_one_page_through_one_gib() {
        assert_eq!(commit_limit("1048576"), (1 << 20, false));
        assert_eq!(commit_limit(" 4096 "), (4096, false));
        assert_eq!(commit_limit("1073741824"), (1 << 30, false));
        assert_eq!(commit_limit("100"), (4096, true));
        assert_eq!(commit_limit("1073741825"), (1 << 30, true));
        assert_eq!(commit_limit("18446744073709551615"), (1 << 30, true));
        assert_eq!(commit_limit("0"), (DEFAULT_MAX_COMMIT_BYTES, true));
        assert_eq!(commit_limit("64MiB"), (DEFAULT_MAX_COMMIT_BYTES, true));
        assert_eq!(commit_limit("-1"), (DEFAULT_MAX_COMMIT_BYTES, true));
    }
}
