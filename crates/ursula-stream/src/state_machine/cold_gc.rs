//! Cold-tier garbage-collection queue.
//!
//! When a stream's cold objects become unreferenced (stream deleted, prefix
//! compacted) their reclamation is deferred to a background worker on the
//! leader. This queue stamps each batch with a monotonically increasing
//! sequence number so draining can be confirmed by a replicated `AckColdGc`.
#![expect(
    clippy::arithmetic_side_effects,
    reason = "pre-existing arithmetic debt; see Known debt in AGENTS.md"
)]

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
    pub(super) fn enqueue(&mut self, bucket_id: String, target: ColdGcTarget) {
        self.enqueue_after(bucket_id, target, 0);
    }

    /// Append the removal of one stream incarnation, scoped to its cold
    /// generation (F14g).
    pub(super) fn enqueue_stream(
        &mut self,
        bucket_id: String,
        stream_id: ursula_shard::BucketStreamId,
        cold_generation: u64,
    ) {
        self.push(
            bucket_id,
            ColdGcTarget::Stream(stream_id),
            0,
            Some(cold_generation),
        );
    }

    /// Append a reclamation target that must remain readable until the given
    /// wall-clock timestamp. Cold-object compaction uses this grace period so
    /// a lagging replica can apply the replacement and invalidate its cached
    /// cold-index page before the old objects disappear.
    pub(super) fn enqueue_after(
        &mut self,
        bucket_id: String,
        target: ColdGcTarget,
        not_before_ms: u64,
    ) {
        self.push(bucket_id, target, not_before_ms, None);
    }

    fn push(
        &mut self,
        bucket_id: String,
        target: ColdGcTarget,
        not_before_ms: u64,
        cold_generation: Option<u64>,
    ) {
        let seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);
        self.pending.push_back(ColdGcEntry {
            seq,
            bucket_id,
            not_before_ms,
            target,
            cold_generation,
            defer_attempts: 0,
        });
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

    /// Moves the entry `seq` to the tail under the next sequence number, due
    /// no earlier than `not_before_ms` (F14b). Acks pop a prefix by sequence
    /// number, so the entry must be restamped: under its old number a later
    /// ack would pop it without its objects having been reclaimed. Returns
    /// the new number, or `None` when no entry has `seq`.
    pub(super) fn defer(&mut self, seq: u64, not_before_ms: u64) -> Option<u64> {
        let index = self.pending.iter().position(|entry| entry.seq == seq)?;
        let mut entry = self.pending.remove(index)?;
        let new_seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);
        entry.seq = new_seq;
        entry.not_before_ms = entry.not_before_ms.max(not_before_ms);
        entry.defer_attempts = entry.defer_attempts.saturating_add(1);
        self.pending.push_back(entry);
        Some(new_seq)
    }

    /// A bounded view of the front of the queue for the leader's GC worker.
    pub(super) fn batch(&self, max: usize) -> Vec<ColdGcEntry> {
        self.pending.iter().take(max).cloned().collect()
    }

    pub(super) fn len(&self) -> usize {
        self.pending.len()
    }

    pub(super) fn len_for_bucket(&self, bucket_id: &str) -> usize {
        self.pending
            .iter()
            .filter(|entry| entry.bucket_id == bucket_id)
            .count()
    }

    /// Persist-side view of every pending entry, in queue order.
    pub(super) fn entries(&self) -> impl Iterator<Item = &ColdGcEntry> {
        self.pending.iter()
    }

    pub(super) fn next_seq(&self) -> u64 {
        self.next_seq
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paths_entry(seq: u64, path: &str) -> ColdGcEntry {
        ColdGcEntry {
            seq,
            bucket_id: "bucket".to_owned(),
            not_before_ms: 0,
            target: ColdGcTarget::Paths(vec![path.to_owned()]),
            cold_generation: None,
            defer_attempts: 0,
        }
    }

    #[test]
    fn defer_moves_entry_to_tail_with_new_seq_and_backoff() {
        let mut queue = ColdGcQueue::from_parts(vec![paths_entry(3, "a"), paths_entry(4, "b")], 5);
        assert_eq!(queue.defer(3, 1_000), Some(5));
        let order = queue
            .entries()
            .map(|entry| (entry.seq, entry.not_before_ms))
            .collect::<Vec<_>>();
        assert_eq!(order, vec![(4, 0), (5, 1_000)]);
        assert_eq!(
            queue.batch(2)[1].defer_attempts,
            1,
            "deferral counts an attempt"
        );
        assert_eq!(queue.defer(5, 2_000), Some(6));
        assert_eq!(queue.batch(2)[1].defer_attempts, 2);
        assert_eq!(queue.next_seq(), 7);
        let mut queue = ColdGcQueue::from_parts(vec![paths_entry(3, "a"), paths_entry(4, "b")], 5);
        assert_eq!(queue.defer(3, 1_000), Some(5));
        assert_eq!(queue.next_seq(), 6);
        // Acking the entry behind it must not pop the deferred one.
        assert_eq!(queue.ack(4), 1);
        assert_eq!(queue.len(), 1);
        // Replay of the same deferral is a no-op.
        assert_eq!(queue.defer(3, 1_000), None);
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn defer_never_shortens_an_existing_grace() {
        let mut queue = ColdGcQueue::from_parts(
            vec![ColdGcEntry {
                not_before_ms: 9_000,
                ..paths_entry(0, "a")
            }],
            1,
        );
        assert_eq!(queue.defer(0, 1_000), Some(1));
        assert_eq!(queue.batch(1)[0].not_before_ms, 9_000);
    }
}
