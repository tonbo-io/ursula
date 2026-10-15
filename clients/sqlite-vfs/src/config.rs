//! Settings and test hooks read from the environment (`URSULA_VFS_*`), once per process.

use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::PoisonError;
use std::time::Duration;

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

/// A step of the commit path or of a checkpoint that `URSULA_VFS_ABORT_AT` can stop the process at
/// (design §4.1, §4.3).
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
            Step::CheckpointStart => "checkpoint_start",
            Step::CheckpointSynced => "checkpoint_synced",
            Step::CheckpointPage => "checkpoint_page",
            Step::BaseStart => "base_start",
            Step::BaseSynced => "base_synced",
            Step::Based => "based",
        }
    }
}

/// Test hook `URSULA_VFS_ABORT_AT=<step>[:<n>]`: aborts the process the `n`-th time (1 by default)
/// it reaches `step` ([`Step`], by its name), as a crash there would stop it.
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
        std::process::abort();
    }
}

/// `URSULA_VFS_MAX_REQUEST_BYTES`: a request limit below the server's, for a layer in front of it
/// that refuses larger bodies (design §4.6). Unset, or not a number of bytes (logged once as
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

/// Test hook `URSULA_VFS_SNAPSHOT_DUE_BYTES`: a snapshot is due once more frame bytes than this
/// are acknowledged since the latest one, instead of past the db file's size or 8 MiB (design
/// §4.11). Unset, or not a number of bytes: the design's threshold.
pub(crate) fn snapshot_due_bytes() -> Option<u64> {
    static V: OnceLock<Option<u64>> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("URSULA_VFS_SNAPSHOT_DUE_BYTES")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
    })
}

/// `URSULA_VFS_MIN_UPLOAD_KBPS`: the slowest upload, in KiB/s, an attempt's body time limit allows
/// for (256 by default, at least 1): a body of `n` bytes may take `5 s + n / rate` to send (design
/// §4.12, #566).
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
