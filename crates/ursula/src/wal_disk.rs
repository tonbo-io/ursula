//! Process-local Raft WAL disk-pressure state shared by admission, readiness,
//! metrics, and the leadership gate.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WalDiskTransition {
    NoChange,
    EnterPressure,
    LeavePressure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WalDiskSnapshot {
    pub available_bytes: u64,
    pub min_available_bytes: u64,
    pub resume_available_bytes: u64,
    pub pressure: bool,
    pub stat_errors: u64,
}

#[derive(Debug)]
struct WalDiskMonitorInner {
    available_bytes: AtomicU64,
    min_available_bytes: u64,
    resume_available_bytes: u64,
    pressure: AtomicBool,
    stat_errors: AtomicU64,
}

/// Hysteretic free-space guard for the configured Raft WAL filesystem.
#[derive(Debug, Clone)]
pub(crate) struct WalDiskMonitor {
    inner: Arc<WalDiskMonitorInner>,
}

impl Default for WalDiskMonitor {
    fn default() -> Self {
        Self::new(0, 0)
    }
}

impl WalDiskMonitor {
    pub(crate) fn new(min_available_bytes: u64, resume_available_bytes: u64) -> Self {
        Self {
            inner: Arc::new(WalDiskMonitorInner {
                available_bytes: AtomicU64::new(0),
                min_available_bytes,
                resume_available_bytes,
                pressure: AtomicBool::new(false),
                stat_errors: AtomicU64::new(0),
            }),
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.inner.min_available_bytes > 0
    }

    pub(crate) fn is_pressured(&self) -> bool {
        self.inner.pressure.load(Ordering::Acquire)
    }

    pub(crate) fn observe_available(&self, available_bytes: u64) -> WalDiskTransition {
        self.inner
            .available_bytes
            .store(available_bytes, Ordering::Release);
        if !self.enabled() {
            return WalDiskTransition::NoChange;
        }

        let pressured = self.is_pressured();
        let next = if pressured {
            available_bytes < self.inner.resume_available_bytes
        } else {
            available_bytes < self.inner.min_available_bytes
        };
        if next == pressured {
            return WalDiskTransition::NoChange;
        }
        self.inner.pressure.store(next, Ordering::Release);
        if next {
            WalDiskTransition::EnterPressure
        } else {
            WalDiskTransition::LeavePressure
        }
    }

    /// Fail closed when the filesystem cannot be inspected. The error counter
    /// distinguishes a stat failure from a genuine zero-free-byte sample.
    pub(crate) fn observe_error(&self) -> WalDiskTransition {
        self.inner.stat_errors.fetch_add(1, Ordering::Relaxed);
        if !self.enabled() || self.inner.pressure.swap(true, Ordering::AcqRel) {
            WalDiskTransition::NoChange
        } else {
            WalDiskTransition::EnterPressure
        }
    }

    pub(crate) fn snapshot(&self) -> WalDiskSnapshot {
        WalDiskSnapshot {
            available_bytes: self.inner.available_bytes.load(Ordering::Acquire),
            min_available_bytes: self.inner.min_available_bytes,
            resume_available_bytes: self.inner.resume_available_bytes,
            pressure: self.is_pressured(),
            stat_errors: self.inner.stat_errors.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disk_pressure_uses_hysteresis() {
        let monitor = WalDiskMonitor::new(100, 200);
        assert_eq!(monitor.observe_available(150), WalDiskTransition::NoChange);
        assert_eq!(
            monitor.observe_available(99),
            WalDiskTransition::EnterPressure
        );
        assert!(monitor.is_pressured());
        assert_eq!(monitor.observe_available(150), WalDiskTransition::NoChange);
        assert_eq!(
            monitor.observe_available(200),
            WalDiskTransition::LeavePressure
        );
        assert!(!monitor.is_pressured());
    }

    #[test]
    fn disk_stat_error_fails_closed_and_counts() {
        let monitor = WalDiskMonitor::new(100, 200);
        assert_eq!(monitor.observe_error(), WalDiskTransition::EnterPressure);
        assert_eq!(monitor.observe_error(), WalDiskTransition::NoChange);
        assert_eq!(monitor.snapshot().stat_errors, 2);
    }

    #[test]
    fn zero_minimum_disables_pressure() {
        let monitor = WalDiskMonitor::default();
        assert_eq!(monitor.observe_error(), WalDiskTransition::NoChange);
        assert!(!monitor.is_pressured());
    }
}
