//! Settings and test hooks read from the environment (`URSULA_VFS_*`), once per process.

use std::sync::OnceLock;
use std::time::Duration;

use crate::log;
use crate::log::Level;

pub(crate) fn abort_after_ack() -> Option<u64> {
    static V: OnceLock<Option<u64>> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("URSULA_VFS_ABORT_AFTER_ACK")
            .ok()
            .and_then(|v| v.parse().ok())
    })
}

pub(crate) fn retry_budget() -> Duration {
    static V: OnceLock<Duration> = OnceLock::new();
    *V.get_or_init(|| {
        Duration::from_millis(
            std::env::var("URSULA_VFS_RETRY_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(30_000),
        )
    })
}

/// `URSULA_VFS_IDLE_MS`: how long a connection is kept after its last request, for the next one
/// to reuse, in milliseconds (90 s by default, see `client::agent`). 0 never reuses one. A value
/// that is not a number of milliseconds keeps the default, logged once (`setting_adjusted`).
pub(crate) fn idle_age() -> Duration {
    const DEFAULT_MS: u64 = 90_000;
    static V: OnceLock<Duration> = OnceLock::new();
    *V.get_or_init(|| {
        let Ok(value) = std::env::var("URSULA_VFS_IDLE_MS") else {
            return Duration::from_millis(DEFAULT_MS);
        };
        let ms = value.trim().parse().unwrap_or_else(|_not_a_number| {
            log::emit(Level::Warn, "setting_adjusted", &[
                ("setting", &"URSULA_VFS_IDLE_MS"),
                ("value", &value),
                ("used", &DEFAULT_MS),
            ]);
            DEFAULT_MS
        });
        Duration::from_millis(ms)
    })
}

/// `URSULA_VFS_PANIC_IN`: what panics on purpose at every call, for tests of `guard::contain`: a
/// file method by its name (`x_write`, `x_close`, ...), a SQL function (`ursula_status`, ...),
/// `sqlite3_extension_init`, or the attach step `attach_files`. Unset outside tests. Unit tests set
/// it per thread instead (`set_panic_in_for_tests`).
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

/// The largest commit frame the extension sends by default: the server refuses a request body over
/// 32 MiB, and so does a gateway by default (`--max-request-body-bytes`).
pub(crate) const DEFAULT_MAX_FRAME_BYTES: usize = 32 << 20;

/// The smallest frame limit `URSULA_VFS_MAX_FRAME_BYTES` may set: well above a claim frame (about
/// 50 bytes), which every attach of an owner sends, and a commit of a few pages.
pub(crate) const MIN_FRAME_BYTES: usize = 64 << 10;

/// The largest commit frame the extension sends (`client::append`): `URSULA_VFS_MAX_FRAME_BYTES` in
/// bytes, or [`DEFAULT_MAX_FRAME_BYTES`]. Lower it to a smaller limit in front of the server. A
/// value outside [`MIN_FRAME_BYTES`] to [`DEFAULT_MAX_FRAME_BYTES`] is taken to the nearer bound,
/// and one that is not a number of bytes (`16MiB`, say) is ignored, both logged once
/// (`setting_adjusted`): 0 would refuse every claim, and a frame above the server's limit would be
/// sent and refused in a way that can poison.
pub(crate) fn max_frame_bytes() -> usize {
    static V: OnceLock<usize> = OnceLock::new();
    *V.get_or_init(|| {
        let Ok(value) = std::env::var("URSULA_VFS_MAX_FRAME_BYTES") else {
            return DEFAULT_MAX_FRAME_BYTES;
        };
        match frame_limit_from(&value) {
            Setting::Taken(limit) => limit,
            Setting::Adjusted(used) => {
                log::emit(Level::Warn, "setting_adjusted", &[
                    ("setting", &"URSULA_VFS_MAX_FRAME_BYTES"),
                    ("value", &value),
                    ("used", &used),
                ]);
                used
            }
        }
    })
}

/// What a setting's value sets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Setting<T> {
    /// The value, as given.
    Taken(T),
    /// Something else, because the value is out of range or not a number: logged once as
    /// `setting_adjusted`.
    Adjusted(T),
}

/// The frame limit `URSULA_VFS_MAX_FRAME_BYTES=value` sets (see [`max_frame_bytes`]).
fn frame_limit_from(value: &str) -> Setting<usize> {
    match value.trim().parse::<usize>() {
        Ok(bytes) if (MIN_FRAME_BYTES..=DEFAULT_MAX_FRAME_BYTES).contains(&bytes) => {
            Setting::Taken(bytes)
        }
        Ok(bytes) => Setting::Adjusted(bytes.clamp(MIN_FRAME_BYTES, DEFAULT_MAX_FRAME_BYTES)),
        Err(_not_a_number) => Setting::Adjusted(DEFAULT_MAX_FRAME_BYTES),
    }
}

/// Test hook `URSULA_VFS_SNAPSHOT_RETRY_MS`: the wait after a first failed snapshot attempt (100 ms
/// by default, at least 1 ms), which doubles with each further failure
/// (`snapshotter::SnapshotFailing`). Tests set it high to see the next attempt held back.
pub(crate) fn snapshot_retry() -> Duration {
    static V: OnceLock<Duration> = OnceLock::new();
    *V.get_or_init(|| {
        Duration::from_millis(
            std::env::var("URSULA_VFS_SNAPSHOT_RETRY_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100),
        )
    })
}

pub(crate) fn snapshot_min_bytes() -> u64 {
    static V: OnceLock<u64> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("URSULA_VFS_SNAPSHOT_MIN_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8 << 20)
    })
}

pub(crate) fn fail_post_ack() -> Option<u64> {
    static V: OnceLock<Option<u64>> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("URSULA_VFS_FAIL_POST_ACK")
            .ok()
            .and_then(|v| v.parse().ok())
    })
}

pub(crate) fn abort_in_replay() -> Option<u64> {
    static V: OnceLock<Option<u64>> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("URSULA_VFS_ABORT_IN_REPLAY")
            .ok()
            .and_then(|v| v.parse().ok())
    })
}

pub(crate) fn first_claim_epoch() -> Option<u64> {
    static V: OnceLock<Option<u64>> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("URSULA_VFS_FIRST_CLAIM_EPOCH")
            .ok()
            .and_then(|v| v.parse().ok())
    })
}

/// `URSULA_VFS_MIN_UPLOAD_KBPS`: the slowest upload, in KiB/s, an attempt's body time limit allows
/// for (256 by default, at least 1): a body of `n` bytes may take `5 s + n / rate` to send (design
/// §4.12, #566).
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the client side is unwired until the 0.8 driver (PR5) sends through it"
    )
)]
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

fn non_empty(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

#[cfg(test)]
mod tests {
    use super::DEFAULT_MAX_FRAME_BYTES;
    use super::MIN_FRAME_BYTES;
    use super::Setting;
    use super::frame_limit_from;

    // The frame limit is a number of bytes from 64 KiB to 32 MiB, both included: below, an owner's
    // claim could be refused, and above, a frame the server cannot take would be sent. A value
    // outside is taken to the nearer bound, and one that is not a number keeps 32 MiB, both
    // adjusted (logged as `setting_adjusted`).
    #[test]
    fn the_frame_limit_stays_between_64_kib_and_32_mib() {
        assert_eq!(
            (MIN_FRAME_BYTES, DEFAULT_MAX_FRAME_BYTES),
            (65_536, 33_554_432)
        );
        assert_eq!(frame_limit_from("1048576"), Setting::Taken(1_048_576));
        assert_eq!(frame_limit_from(" 524288 "), Setting::Taken(524_288));
        assert_eq!(frame_limit_from("65536"), Setting::Taken(65_536));
        assert_eq!(frame_limit_from("33554432"), Setting::Taken(33_554_432));
        assert_eq!(frame_limit_from("65535"), Setting::Adjusted(65_536));
        assert_eq!(frame_limit_from("33554433"), Setting::Adjusted(33_554_432));
        assert_eq!(frame_limit_from("0"), Setting::Adjusted(65_536));
        assert_eq!(frame_limit_from("16MiB"), Setting::Adjusted(33_554_432));
        assert_eq!(frame_limit_from(""), Setting::Adjusted(33_554_432));
    }
}
