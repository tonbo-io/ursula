//! Producer-state bounds (bounded-stream-state F3) and `TidyStream`.
//!
//! - **One receipt per producer.** A record keeps its newest write's receipt
//!   only (#581 section 2.7); a repeat of an older write answers
//!   `receipt-evicted`.
//! - **Epoch floor.** Retiring a record, by idle expiry, the producer cap or
//!   `TidyStream`, raises the stream's `retired_epoch_watermark` to at least
//!   its epoch (#581 section 2.3), so a tokened write never reopens an epoch
//!   the stream forgot.
//! - **Idle expiry.** A producer whose newest write is at least
//!   [`PRODUCER_IDLE_EXPIRY_MS`] old (by the persisted `last_seen_ms` and the
//!   command's `now_ms`) is treated as absent at its own next write and
//!   removed then; `TidyStream` removes idle producers in bulk.
//! - **Producer cap.** A stream holds at most [`MAX_PRODUCERS_PER_STREAM`]
//!   producers. A new producer beyond it evicts the least recently seen
//!   producers idle for at least an hour, at the enforcement point, at most
//!   [`PRODUCER_CAP_EVICT_BUDGET`] per command (`TidyStream` drains a larger
//!   excess); when none is idle that long the write fails with
//!   `ProducerLimit` (`429`).
//! - **`TidyStream`.** Converges one stream in bounded steps: idle-producer
//!   expiry and producer-cap eviction.

use super::BucketStreamId;
use super::ProducerState;
use super::StreamErrorCode;
use super::StreamResponse;
use super::StreamSlot;
use super::StreamStateMachine;

/// A producer idle this long is treated as absent at its next write (F3).
pub const PRODUCER_IDLE_EXPIRY_MS: u64 = 7 * 24 * 60 * 60 * 1_000;

/// Producers a stream holds at most (F3 producer cap).
pub const MAX_PRODUCERS_PER_STREAM: usize = 4_096;

/// A producer idle this long may be evicted to admit a new producer once the
/// stream holds [`MAX_PRODUCERS_PER_STREAM`] (F3 producer cap).
pub const PRODUCER_CAP_EVICT_IDLE_MS: u64 = 60 * 60 * 1_000;

/// Producers one command may evict for the producer cap; `TidyStream`
/// drains the rest of the excess (F3 bounded catch-up).
pub const PRODUCER_CAP_EVICT_BUDGET: usize = 1_024;

/// Idle producers one `TidyStream` may expire.
pub const TIDY_PRODUCER_BUDGET: usize = 4_096;

/// Whether a producer last written at `last_seen_ms` is idle at `now_ms`.
pub(super) fn producer_is_idle(state: &ProducerState, now_ms: u64) -> bool {
    now_ms.saturating_sub(state.last_seen_ms) >= PRODUCER_IDLE_EXPIRY_MS
}

/// Whether the producer cap may evict `state` at `now_ms`: idle for at least
/// [`PRODUCER_CAP_EVICT_IDLE_MS`].
fn producer_cap_evictable(state: &ProducerState, now_ms: u64) -> bool {
    now_ms.saturating_sub(state.last_seen_ms) >= PRODUCER_CAP_EVICT_IDLE_MS
}

impl StreamSlot {
    /// Retires one producer record, raising the stream's epoch floor to at
    /// least its epoch.
    pub(super) fn retire_producer(&mut self, producer_id: &str) -> Option<ProducerState> {
        let state = self.producers.remove(producer_id)?;
        self.retired_epoch_watermark = Some(
            self.retired_epoch_watermark
                .map_or(state.epoch, |floor| floor.max(state.epoch)),
        );
        Some(state)
    }

    /// Whether a write by `producer_id` at `now_ms` fits the producer cap: the
    /// producer exists already, the stream is below the cap, or enough
    /// producers are idle for an hour to make room after the command.
    pub(super) fn producer_cap_admits(&self, producer_id: &str, now_ms: u64) -> bool {
        if self.producers.len() < MAX_PRODUCERS_PER_STREAM
            || self.producers.contains_key(producer_id)
        {
            return true;
        }
        let active = self
            .producers
            .values()
            .filter(|state| !producer_cap_evictable(state, now_ms))
            .count();
        active < MAX_PRODUCERS_PER_STREAM
    }

    /// Evicts least recently seen producers idle for an hour, oldest
    /// `(last_seen_ms, producer_id)` first, until the stream holds at most
    /// [`MAX_PRODUCERS_PER_STREAM`] or `budget` producers were evicted.
    /// One O(P) pass collects the candidates and selects the oldest excess,
    /// so a stream far over the cap costs O(P) per command, and `TidyStream`
    /// drains what the budget leaves.
    pub(super) fn evict_producers_over_cap(&mut self, now_ms: u64, budget: usize) {
        let excess = self
            .producers
            .len()
            .saturating_sub(MAX_PRODUCERS_PER_STREAM)
            .min(budget);
        if excess == 0 {
            return;
        }
        let mut candidates = self
            .producers
            .iter()
            .filter(|(_, state)| producer_cap_evictable(state, now_ms))
            .map(|(producer_id, state)| (state.last_seen_ms, producer_id.clone()))
            .collect::<Vec<_>>();
        if candidates.len() > excess {
            // Keys are unique, so the selected set is the same on every
            // replica whatever the map's iteration order.
            candidates.select_nth_unstable(excess);
            candidates.truncate(excess);
        }
        for (_, victim) in candidates {
            self.retire_producer(&victim);
        }
    }

    /// Whether the stream is over the producer cap with an evictable
    /// producer at `now_ms`.
    fn producer_cap_over(&self, now_ms: u64) -> bool {
        self.producers.len() > MAX_PRODUCERS_PER_STREAM
            && self
                .producers
                .values()
                .any(|state| producer_cap_evictable(state, now_ms))
    }
}

impl StreamStateMachine {
    /// F3 enforcement point, run once per command after it fully applied:
    /// the producer cap.
    pub(super) fn enforce_producer_cap(&mut self, stream_id: &BucketStreamId, now_ms: u64) {
        if let Some(slot) = self.stream_slot_mut(stream_id) {
            slot.evict_producers_over_cap(now_ms, PRODUCER_CAP_EVICT_BUDGET);
        }
    }

    /// Lazy idle expiry at the producer's own write: an idle producer is
    /// removed before the write is evaluated, so it is treated as absent.
    pub(super) fn expire_idle_producer(
        &mut self,
        stream_id: &BucketStreamId,
        producer_id: &str,
        now_ms: u64,
    ) {
        let Some(slot) = self.stream_slot_mut(stream_id) else {
            return;
        };
        if slot
            .producers
            .get(producer_id)
            .is_some_and(|state| producer_is_idle(state, now_ms))
        {
            slot.retire_producer(producer_id);
        }
    }

    /// Whether `TidyStream` would change this stream at `now_ms`.
    pub fn stream_has_tidy_debt(&self, stream_id: &BucketStreamId, now_ms: u64) -> bool {
        let Some(slot) = self.stream_slot(stream_id) else {
            return false;
        };
        slot.producer_cap_over(now_ms)
            || slot
                .producers
                .values()
                .any(|state| producer_is_idle(state, now_ms))
    }

    /// Up to `limit` streams with `TidyStream` debt at `now_ms`, in stream-id
    /// order. Read-only; a leader-side driver proposes `TidyStream` for them.
    pub fn tidy_candidates(&self, now_ms: u64, limit: usize) -> Vec<BucketStreamId> {
        if limit == 0 {
            return Vec::new();
        }
        let mut candidates = self
            .registry
            .slots()
            .map(|slot| &slot.metadata.stream_id)
            .filter(|stream_id| self.stream_has_tidy_debt(stream_id, now_ms))
            .cloned()
            .collect::<Vec<_>>();
        candidates.sort_by(super::compare_stream_ids);
        candidates.truncate(limit);
        candidates
    }

    /// Applies [`crate::StreamCommand::TidyStream`].
    pub(super) fn tidy_stream(
        &mut self,
        stream_id: &BucketStreamId,
        now_ms: u64,
    ) -> StreamResponse {
        if let Err(response) = self.validate_stream_scope(stream_id) {
            return response;
        }
        if self.stream_slot(stream_id).is_none() {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        }
        let Some(slot) = self.stream_slot_mut(stream_id) else {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        };
        // Idle-producer expiry in producer-id order so every replica does
        // the same bounded work.
        let mut producer_ids = slot
            .producers
            .iter()
            .filter(|(_, state)| producer_is_idle(state, now_ms))
            .map(|(producer_id, _)| producer_id.clone())
            .collect::<Vec<_>>();
        producer_ids.sort();
        producer_ids.truncate(TIDY_PRODUCER_BUDGET);
        for producer_id in producer_ids {
            slot.retire_producer(&producer_id);
        }
        slot.evict_producers_over_cap(now_ms, PRODUCER_CAP_EVICT_BUDGET);
        if slot.producers.capacity() > slot.producers.len().saturating_mul(2).saturating_add(8) {
            slot.producers.shrink_to_fit();
        }
        StreamResponse::StreamTidied {
            debt_remaining: self.stream_has_tidy_debt(stream_id, now_ms),
        }
    }
}

impl StreamSlot {
    /// Seal point `p(s)`: the first hot byte, or the tail when nothing is
    /// hot. Every retained byte below it is cold (F18).
    pub(super) fn seal_point(&self) -> u64 {
        self.hot_buffer
            .first_start_offset()
            .unwrap_or(self.metadata.tail_offset)
    }
}
