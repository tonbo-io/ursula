//! Orthogonal write-path admission controls.
//!
//! Each admission gates accept on one independent "slow downstream":
//! - cold path full (hot_bytes_per_group, existing [`crate::request::ColdWriteAdmission`])
//! - raft replication lagging ([`RaftUncommittedAdmission`], uncommitted_bytes_per_group)
//! - forward queue piling on a remote peer (inflight_forward_bytes_per_peer, lives in `ursula::lib`)
//! - process memory near OOM (rss vs soft_cap, lives in `ursula::lib`)
//!
//! Each admission can be independently configured (`None` = disabled).
//! Call sites consult the relevant subset; errors surface as HTTP 503.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use ursula_shard::RaftGroupId;

/// Per-group admission that rejects new writes when the raft layer has not yet
/// committed enough previously-submitted bytes. Independently configurable from
/// the cold-side admission; intended to catch "replication lag" scenarios where
/// hot bytes have not yet grown because nothing is committing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RaftUncommittedAdmission {
    pub max_uncommitted_bytes_per_group: Option<u64>,
}

impl RaftUncommittedAdmission {
    pub fn is_enabled(self) -> bool {
        self.max_uncommitted_bytes_per_group.is_some()
    }
}

/// Lock-free per-group counters for in-flight (submitted but not-yet-applied)
/// raft payload bytes. Shared between the core worker (increment on submit) and
/// the group actor (decrement on apply completion).
#[derive(Debug)]
pub(crate) struct RaftUncommittedBytesTracker {
    per_group: Vec<AtomicU64>,
}

impl RaftUncommittedBytesTracker {
    pub(crate) fn new(group_count: usize) -> Self {
        Self {
            per_group: (0..group_count).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    pub(crate) fn load(&self, group_id: RaftGroupId) -> u64 {
        self.slot(group_id).load(Ordering::Relaxed)
    }

    pub(crate) fn add(&self, group_id: RaftGroupId, bytes: u64) {
        self.slot(group_id).fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn sub(&self, group_id: RaftGroupId, bytes: u64) {
        // Use saturating subtract to avoid wrap if a request is double-credited
        // (defense-in-depth; the call sites pair add/sub one-for-one).
        let slot = self.slot(group_id);
        let mut current = slot.load(Ordering::Relaxed);
        loop {
            let next = current.saturating_sub(bytes);
            match slot.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    fn slot(&self, group_id: RaftGroupId) -> &AtomicU64 {
        let index = usize::try_from(group_id.0).expect("u32 fits usize");
        &self.per_group[index]
    }
}

pub(crate) type SharedRaftUncommittedBytes = Arc<RaftUncommittedBytesTracker>;

/// Guard that decrements the uncommitted bytes counter on drop. Pair an `add`
/// at submit-time with a guard that lives until the apply (or apply-failure)
/// completes so we never leak credit on early errors.
pub(crate) struct UncommittedBytesGuard {
    tracker: SharedRaftUncommittedBytes,
    group_id: RaftGroupId,
    bytes: u64,
    armed: bool,
}

impl UncommittedBytesGuard {
    pub(crate) fn new(
        tracker: SharedRaftUncommittedBytes,
        group_id: RaftGroupId,
        bytes: u64,
    ) -> Self {
        tracker.add(group_id, bytes);
        Self {
            tracker,
            group_id,
            bytes,
            armed: true,
        }
    }

    /// Disarm without releasing; useful when something else has taken over
    /// the credit (we currently always release on drop, so this is reserved
    /// for future use).
    #[allow(dead_code)]
    pub(crate) fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for UncommittedBytesGuard {
    fn drop(&mut self) {
        if self.armed {
            self.tracker.sub(self.group_id, self.bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_admission_reports_disabled() {
        let admission = RaftUncommittedAdmission::default();
        assert!(!admission.is_enabled());
    }

    #[test]
    fn enabled_admission_reports_enabled() {
        let admission = RaftUncommittedAdmission {
            max_uncommitted_bytes_per_group: Some(1024),
        };
        assert!(admission.is_enabled());
    }

    #[test]
    fn tracker_add_load_sub_round_trips() {
        let tracker = RaftUncommittedBytesTracker::new(2);
        tracker.add(RaftGroupId(0), 32);
        tracker.add(RaftGroupId(0), 8);
        tracker.add(RaftGroupId(1), 4);
        assert_eq!(tracker.load(RaftGroupId(0)), 40);
        assert_eq!(tracker.load(RaftGroupId(1)), 4);
        tracker.sub(RaftGroupId(0), 16);
        assert_eq!(tracker.load(RaftGroupId(0)), 24);
    }

    #[test]
    fn tracker_sub_saturates_at_zero() {
        let tracker = RaftUncommittedBytesTracker::new(1);
        tracker.add(RaftGroupId(0), 4);
        tracker.sub(RaftGroupId(0), 10);
        assert_eq!(tracker.load(RaftGroupId(0)), 0);
    }

    #[test]
    fn guard_releases_on_drop() {
        let tracker = Arc::new(RaftUncommittedBytesTracker::new(1));
        {
            let _guard = UncommittedBytesGuard::new(tracker.clone(), RaftGroupId(0), 32);
            assert_eq!(tracker.load(RaftGroupId(0)), 32);
        }
        assert_eq!(tracker.load(RaftGroupId(0)), 0);
    }
}
