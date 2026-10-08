//! Settings and test hooks read from the environment (`URSULA_VFS_*`), once per process.

use std::sync::OnceLock;
use std::time::Duration;

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
