//! Process-wide resource admission of the keyed engine.
//!
//! Single flight bounds the work of one namespace; nothing else bounded the
//! whole process when many namespaces ingest or compact at once (a pod
//! restart, a burst of first reads). One [`Admission`] per engine bounds
//! that:
//!
//! - a **byte budget** reserved before a source page is read (the page's P7
//!   `max_bytes`, plus the bytes the ingest has already folded and still
//!   holds) and before a compaction runs (its input plus output buffers),
//!   released when the work finishes. A reservation larger than the budget
//!   is clamped to the budget, so a single huge record or compaction still
//!   progresses, alone. Waiting never happens while holding a reservation
//!   (an ingest that cannot grow its reservation publishes what it has, or
//!   releases it and waits for the whole amount), so reservations cannot
//!   deadlock;
//! - **slots** for concurrent ingests and concurrent compactions;
//! - a bounded **admission queue** of namespaces whose worker was admitted
//!   but has not started ingesting yet. When it is full, a read that needs
//!   ingestion answers 503 with `Retry-After`; compaction waits instead.
//!
//! The byte semaphore counts 1 KiB granules (FIFO, so a large reservation
//! is not starved by small ones); the budget is rounded down to a granule.
//! Gauges and counters for the metrics snapshot live in [`AdmissionMetrics`].

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use serde::Serialize;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

/// Unit of the byte semaphore.
const GRANULE: u64 = 1024;

/// Admission limits (from [`super::KeyedEngineConfig`]).
#[derive(Clone, Copy, Debug)]
pub(crate) struct AdmissionLimits {
    pub(crate) budget_bytes: u64,
    pub(crate) ingests: usize,
    pub(crate) compactions: usize,
    pub(crate) queue: usize,
}

#[derive(Debug, Default)]
struct Counters {
    in_use: AtomicU64,
    peak: AtomicU64,
    queued: AtomicUsize,
    rejections: AtomicU64,
    ingests: AtomicUsize,
    compactions: AtomicUsize,
    waits: AtomicU64,
    truncations: AtomicU64,
}

/// The admission controller of one engine (one indexer process).
#[derive(Debug)]
pub(crate) struct Admission {
    budget_bytes: u64,
    granules: u32,
    bytes: Arc<Semaphore>,
    ingests: Arc<Semaphore>,
    compactions: Arc<Semaphore>,
    max_queue: usize,
    counters: Arc<Counters>,
}

/// Bytes reserved from the budget; returned on drop.
#[derive(Debug)]
pub(crate) struct Reservation {
    permit: Option<OwnedSemaphorePermit>,
    bytes: u64,
    counters: Arc<Counters>,
}

impl Reservation {
    /// Bytes held (after clamping).
    #[cfg(test)]
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        // Before the permit is released (fields drop after this), so the
        // gauge never exceeds the budget.
        self.counters.in_use.fetch_sub(self.bytes, Ordering::SeqCst);
    }
}

/// A place in the admission queue; leaves the queue on drop.
#[derive(Debug)]
pub(crate) struct QueueTicket {
    counters: Arc<Counters>,
}

impl Drop for QueueTicket {
    fn drop(&mut self) {
        self.counters.queued.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Clone, Copy, Debug)]
enum SlotKind {
    Ingest,
    Compaction,
}

/// An ingest or compaction slot; released on drop.
#[derive(Debug)]
pub(crate) struct Slot {
    _permit: Option<OwnedSemaphorePermit>,
    kind: SlotKind,
    counters: Arc<Counters>,
}

impl Drop for Slot {
    fn drop(&mut self) {
        let running = match self.kind {
            SlotKind::Ingest => &self.counters.ingests,
            SlotKind::Compaction => &self.counters.compactions,
        };
        running.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Admission gauges and counters (U24).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct AdmissionMetrics {
    /// The process-wide byte budget (rounded down to 1 KiB).
    pub budget_bytes: u64,
    /// Bytes reserved now.
    pub in_use_bytes: u64,
    /// The most bytes ever reserved at once (never above the budget).
    pub peak_in_use_bytes: u64,
    /// Namespaces admitted but not yet ingesting.
    pub queue_depth: usize,
    /// The admission queue's bound.
    pub queue_capacity: usize,
    /// Reads answered 503 because the admission queue was full.
    pub rejections: u64,
    /// Ingests running now.
    pub ingests_running: usize,
    /// Compactions running now.
    pub compactions_running: usize,
    /// Reservations that waited for budget.
    pub budget_waits: u64,
    /// Ingests that published early because the budget was taken.
    pub budget_truncations: u64,
}

impl Admission {
    pub(crate) fn new(limits: AdmissionLimits) -> Self {
        let granules = u32::try_from(limits.budget_bytes / GRANULE)
            .unwrap_or(u32::MAX)
            .clamp(1, u32::try_from(Semaphore::MAX_PERMITS).unwrap_or(u32::MAX));
        Self {
            budget_bytes: u64::from(granules).saturating_mul(GRANULE),
            granules,
            bytes: Arc::new(Semaphore::new(granules as usize)),
            ingests: Arc::new(Semaphore::new(limits.ingests.max(1))),
            compactions: Arc::new(Semaphore::new(limits.compactions.max(1))),
            max_queue: limits.queue,
            counters: Arc::new(Counters::default()),
        }
    }

    /// `bytes` clamped to the budget, and its granules.
    fn clamp(&self, bytes: u64) -> (u64, u32) {
        let bytes = bytes.clamp(1, self.budget_bytes);
        let granules = u32::try_from(bytes.div_ceil(GRANULE))
            .unwrap_or(u32::MAX)
            .min(self.granules);
        (bytes, granules)
    }

    fn account(&self, bytes: u64) {
        let in_use = self
            .counters
            .in_use
            .fetch_add(bytes, Ordering::SeqCst)
            .saturating_add(bytes);
        self.counters.peak.fetch_max(in_use, Ordering::SeqCst);
    }

    /// Reserves `bytes` (clamped to the budget), waiting for them.
    pub(crate) async fn reserve(&self, bytes: u64) -> Reservation {
        let (bytes, granules) = self.clamp(bytes);
        let permit = match Arc::clone(&self.bytes).try_acquire_many_owned(granules) {
            Ok(permit) => Some(permit),
            Err(_) => {
                self.counters.waits.fetch_add(1, Ordering::Relaxed);
                // The semaphore is never closed.
                Arc::clone(&self.bytes)
                    .acquire_many_owned(granules)
                    .await
                    .ok()
            }
        };
        let bytes = if permit.is_some() { bytes } else { 0 };
        self.account(bytes);
        Reservation {
            permit,
            bytes,
            counters: Arc::clone(&self.counters),
        }
    }

    /// Grows `reservation` to `total` bytes (clamped) without waiting.
    /// Returns whether it now holds that much.
    pub(crate) fn try_grow(&self, reservation: &mut Reservation, total: u64) -> bool {
        let (total, granules) = self.clamp(total);
        if total <= reservation.bytes {
            return true;
        }
        let held = reservation
            .permit
            .as_ref()
            .map_or(0, |permit| permit.num_permits());
        let more = u32::try_from((granules as usize).saturating_sub(held)).unwrap_or(u32::MAX);
        if more > 0 {
            let Ok(extra) = Arc::clone(&self.bytes).try_acquire_many_owned(more) else {
                return false;
            };
            match reservation.permit.as_mut() {
                Some(permit) => permit.merge(extra),
                None => reservation.permit = Some(extra),
            }
        }
        let added = total.saturating_sub(reservation.bytes);
        reservation.bytes = total;
        self.account(added);
        true
    }

    /// Enters the admission queue, or counts a rejection when it is full.
    pub(crate) fn try_enqueue(&self) -> Option<QueueTicket> {
        let entered = self
            .counters
            .queued
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |queued| {
                (queued < self.max_queue).then_some(queued.saturating_add(1))
            })
            .is_ok();
        if !entered {
            self.counters.rejections.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        Some(QueueTicket {
            counters: Arc::clone(&self.counters),
        })
    }

    async fn slot(&self, kind: SlotKind) -> Slot {
        let (semaphore, running) = match kind {
            SlotKind::Ingest => (&self.ingests, &self.counters.ingests),
            SlotKind::Compaction => (&self.compactions, &self.counters.compactions),
        };
        // The semaphore is never closed.
        let permit = Arc::clone(semaphore).acquire_owned().await.ok();
        running.fetch_add(1, Ordering::SeqCst);
        Slot {
            _permit: permit,
            kind,
            counters: Arc::clone(&self.counters),
        }
    }

    /// Waits for an ingest slot.
    pub(crate) async fn ingest_slot(&self) -> Slot {
        self.slot(SlotKind::Ingest).await
    }

    /// Waits for a compaction slot.
    pub(crate) async fn compaction_slot(&self) -> Slot {
        self.slot(SlotKind::Compaction).await
    }

    /// Counts an ingest that published early for want of budget.
    pub(crate) fn truncated(&self) {
        self.counters.truncations.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn metrics(&self) -> AdmissionMetrics {
        let counters = &self.counters;
        AdmissionMetrics {
            budget_bytes: self.budget_bytes,
            in_use_bytes: counters.in_use.load(Ordering::SeqCst),
            peak_in_use_bytes: counters.peak.load(Ordering::SeqCst),
            queue_depth: counters.queued.load(Ordering::SeqCst),
            queue_capacity: self.max_queue,
            rejections: counters.rejections.load(Ordering::Relaxed),
            ingests_running: counters.ingests.load(Ordering::SeqCst),
            compactions_running: counters.compactions.load(Ordering::SeqCst),
            budget_waits: counters.waits.load(Ordering::Relaxed),
            budget_truncations: counters.truncations.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admission(budget_bytes: u64) -> Admission {
        Admission::new(AdmissionLimits {
            budget_bytes,
            ingests: 2,
            compactions: 1,
            queue: 2,
        })
    }

    #[tokio::test]
    async fn oversized_reservations_are_clamped_and_run_alone() {
        let admission = admission(64 * 1024);
        let big = admission.reserve(1 << 30).await;
        assert_eq!(big.bytes(), 64 * 1024);
        let mut small = None;
        tokio::select! {
            biased;
            reservation = admission.reserve(1) => small = Some(reservation),
            () = tokio::task::yield_now() => {}
        }
        assert!(small.is_none(), "the budget is fully held");
        drop(big);
        let small = admission.reserve(1).await;
        assert_eq!(small.bytes(), 1);
        assert_eq!(admission.metrics().peak_in_use_bytes, 64 * 1024);
    }

    #[tokio::test]
    async fn growth_never_exceeds_the_budget() {
        let admission = admission(10 * 1024);
        let mut first = admission.reserve(4 * 1024).await;
        let other = admission.reserve(4 * 1024).await;
        assert!(admission.try_grow(&mut first, 6 * 1024));
        assert!(!admission.try_grow(&mut first, 7 * 1024));
        assert_eq!(first.bytes(), 6 * 1024);
        drop(other);
        assert!(admission.try_grow(&mut first, 1 << 40));
        assert_eq!(first.bytes(), 10 * 1024);
        assert_eq!(admission.metrics().in_use_bytes, 10 * 1024);
        drop(first);
        let metrics = admission.metrics();
        assert_eq!(metrics.in_use_bytes, 0);
        assert!(metrics.peak_in_use_bytes <= metrics.budget_bytes);
    }

    #[test]
    fn the_queue_is_bounded() {
        let admission = admission(1024);
        let first = admission.try_enqueue();
        let second = admission.try_enqueue();
        assert!(first.is_some() && second.is_some());
        assert!(admission.try_enqueue().is_none());
        assert_eq!(admission.metrics().rejections, 1);
        drop(first);
        assert!(admission.try_enqueue().is_some());
    }
}
