//! Cold-tier garbage-collection queue.
//!
//! When a stream's cold objects become unreferenced (stream deleted, prefix
//! compacted) their reclamation is deferred to a background worker on the
//! leader. This queue stamps each batch with a monotonically increasing
//! sequence number so draining can be confirmed by a replicated `AckColdGc`.

use std::collections::VecDeque;

use super::ColdGcEntry;
use super::ColdGcTarget;

#[derive(Debug, Clone, Default)]
pub(super) struct ColdGcQueue {
    pending: VecDeque<ColdGcEntry>,
    next_seq: u64,
}

impl ColdGcQueue {
    /// Rebuild the queue from a persisted snapshot.
    pub(super) fn from_parts(pending: Vec<ColdGcEntry>, next_seq: u64) -> Self {
        Self {
            pending: pending.into_iter().collect(),
            next_seq,
        }
    }

    /// Append a reclamation target, stamping it with the next sequence number.
    pub(super) fn enqueue(&mut self, target: ColdGcTarget) {
        let seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);
        self.pending.push_back(ColdGcEntry { seq, target });
    }

    /// Drain every entry with `seq <= up_to_seq`; returns how many were removed.
    pub(super) fn ack(&mut self, up_to_seq: u64) -> u64 {
        let before = self.pending.len();
        while self
            .pending
            .front()
            .is_some_and(|entry| entry.seq <= up_to_seq)
        {
            self.pending.pop_front();
        }
        u64::try_from(before - self.pending.len()).expect("removed fits u64")
    }

    /// A bounded view of the front of the queue for the leader's GC worker.
    pub(super) fn batch(&self, max: usize) -> Vec<ColdGcEntry> {
        self.pending.iter().take(max).cloned().collect()
    }

    pub(super) fn len(&self) -> usize {
        self.pending.len()
    }

    /// Persist-side view of every pending entry, in queue order.
    pub(super) fn entries(&self) -> impl Iterator<Item = &ColdGcEntry> {
        self.pending.iter()
    }

    pub(super) fn next_seq(&self) -> u64 {
        self.next_seq
    }
}
