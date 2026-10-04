//! Producer-state bounds (bounded-stream-state F3) and `TidyStream` (F0), both
//! at feature level 1.
//!
//! - **Receipt window.** A stream keeps at most [`RECEIPT_WINDOW_ITEMS`]
//!   receipt items (one per append; legacy receipts may hold more) beyond each
//!   producer's newest receipt, which is never evicted. Eviction takes the
//!   oldest evictable receipt in commit order: every producer's receipts are
//!   in commit order, so the oldest evictable one is the front of some
//!   producer that holds at least two. [`ReceiptWindow`] is derived state:
//!   restore rebuilds it from the receipts, so a replica that installed a
//!   snapshot evicts exactly what a replica that replayed the log evicts.
//! - **One enforcement point.** The window is enforced once per command
//!   after the whole command applied, at most [`RECEIPT_TRIM_BUDGET`]
//!   receipts per command, so a legacy producer with a million receipts
//!   drains over several commands without stalling apply.
//! - **Idle expiry.** A producer whose newest write is at least
//!   [`PRODUCER_IDLE_EXPIRY_MS`] old (by the persisted `last_seen_ms` and the
//!   command's `now_ms`) is treated as absent at its own next write and
//!   removed then; `TidyStream` removes idle producers in bulk.
//! - **Producer cap.** A stream holds at most [`MAX_PRODUCERS_PER_STREAM`]
//!   producers. A new producer beyond it evicts the least recently seen
//!   producers idle for at least an hour, at the enforcement point, at most
//!   [`PRODUCER_CAP_EVICT_BUDGET`] per command (`TidyStream` drains a larger
//!   excess, such as a level-0 stream's after the raise); when none is idle
//!   that long the write fails with `ProducerLimit` (`429`).
//! - **`TidyStream`.** Converges one stream in bounded steps: message-record
//!   collapse below the seal point (F4a), idle-producer stamping and expiry,
//!   producer-cap eviction, receipt trimming and dropping the level-0 `last_items` copy.

use std::collections::BTreeSet;
use std::collections::HashMap;

use super::BucketStreamId;
use super::ProducerState;
use super::StreamErrorCode;
use super::StreamResponse;
use super::StreamSlot;
use super::StreamStateMachine;

/// Receipt items a stream keeps beyond each producer's newest receipt (F3).
pub const RECEIPT_WINDOW_ITEMS: u64 = 1_024;

/// A producer idle this long is treated as absent at its next write (F3).
pub const PRODUCER_IDLE_EXPIRY_MS: u64 = 7 * 24 * 60 * 60 * 1_000;

/// Receipts one command may evict from one stream (F3 bounded catch-up).
pub const RECEIPT_TRIM_BUDGET: usize = 65_536;

/// Producers a stream holds at most (F3 producer cap).
pub const MAX_PRODUCERS_PER_STREAM: usize = 4_096;

/// A producer idle this long may be evicted to admit a new producer once the
/// stream holds [`MAX_PRODUCERS_PER_STREAM`] (F3 producer cap).
pub const PRODUCER_CAP_EVICT_IDLE_MS: u64 = 60 * 60 * 1_000;

/// Producers one command may evict for the producer cap; `TidyStream`
/// drains the rest of the excess (F3 bounded catch-up).
pub const PRODUCER_CAP_EVICT_BUDGET: usize = 1_024;

/// Producers one `TidyStream` may stamp, expire or strip of `last_items`.
pub const TIDY_PRODUCER_BUDGET: usize = 4_096;

/// Items a receipt counts against the window: one per append, at least one.
pub(super) fn receipt_items(receipt: &crate::model::ProducerReceipt) -> u64 {
    u64::try_from(receipt.items.len().max(1)).unwrap_or(u64::MAX)
}

/// Whether a producer last written at `last_seen_ms` is idle at `now_ms`.
/// Producers never stamped (written below level 1) are not idle until a
/// `TidyStream` stamps them.
pub(super) fn producer_is_idle(state: &ProducerState, now_ms: u64) -> bool {
    state
        .last_seen_ms
        .is_some_and(|seen| now_ms.saturating_sub(seen) >= PRODUCER_IDLE_EXPIRY_MS)
}

/// Whether the producer cap may evict `state` at `now_ms`: idle for at least
/// [`PRODUCER_CAP_EVICT_IDLE_MS`]. Unstamped producers (written below level 1)
/// are never evicted by the cap.
fn producer_cap_evictable(state: &ProducerState, now_ms: u64) -> bool {
    state
        .last_seen_ms
        .is_some_and(|seen| now_ms.saturating_sub(seen) >= PRODUCER_CAP_EVICT_IDLE_MS)
}

/// Derived per-stream receipt window: the total receipt items held and, for
/// every producer with at least two receipts, its front receipt keyed by
/// `(start_offset, producer_id)` in eviction order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct ReceiptWindow {
    items: u64,
    evictable: BTreeSet<(u64, String)>,
}

impl ReceiptWindow {
    pub(super) fn rebuild(producers: &HashMap<String, ProducerState>) -> Self {
        let mut window = Self::default();
        for (producer_id, state) in producers {
            window.add_producer(producer_id, state);
        }
        window
    }

    #[cfg(test)]
    pub(super) fn items(&self) -> u64 {
        self.items
    }

    fn front_key(producer_id: &str, state: &ProducerState) -> Option<(u64, String)> {
        (state.receipts.len() >= 2)
            .then(|| state.receipts.front())
            .flatten()
            .map(|front| (front.start_offset, producer_id.to_owned()))
    }

    /// Accounts for every receipt of `state`.
    pub(super) fn add_producer(&mut self, producer_id: &str, state: &ProducerState) {
        let items = state.receipts.iter().map(receipt_items).sum::<u64>();
        self.items = self.items.saturating_add(items);
        if let Some(key) = Self::front_key(producer_id, state) {
            self.evictable.insert(key);
        }
    }

    /// Accounts for `receipt` about to be pushed onto `state`'s receipts.
    pub(super) fn push_receipt(
        &mut self,
        producer_id: &str,
        state: &ProducerState,
        receipt: &crate::model::ProducerReceipt,
    ) {
        self.items = self.items.saturating_add(receipt_items(receipt));
        if state.receipts.len() == 1
            && let Some(front) = state.receipts.front()
        {
            self.evictable
                .insert((front.start_offset, producer_id.to_owned()));
        }
    }

    /// Removes every receipt of `state` from the window.
    pub(super) fn remove_producer(&mut self, producer_id: &str, state: &ProducerState) {
        let items = state.receipts.iter().map(receipt_items).sum::<u64>();
        self.items = self.items.saturating_sub(items);
        if let Some(key) = Self::front_key(producer_id, state) {
            self.evictable.remove(&key);
        }
    }
}

impl StreamSlot {
    /// Evicts the oldest evictable receipts until the stream holds at most
    /// [`RECEIPT_WINDOW_ITEMS`] items or nothing more is evictable, at most
    /// `budget` receipts. Returns whether evictable excess remains.
    pub(super) fn trim_receipt_window(&mut self, budget: usize) -> bool {
        let mut evicted = 0usize;
        while self.receipt_window.items > RECEIPT_WINDOW_ITEMS {
            if evicted >= budget {
                return true;
            }
            let Some((_, producer_id)) = self.receipt_window.evictable.pop_first() else {
                return false;
            };
            let Some(state) = self.producers.get_mut(&producer_id) else {
                continue;
            };
            if state.receipts.len() < 2 {
                continue;
            }
            if let Some(receipt) = state.receipts.pop_front() {
                self.receipt_window.items = self
                    .receipt_window
                    .items
                    .saturating_sub(receipt_items(&receipt));
                evicted = evicted.saturating_add(1);
            }
            if let Some(key) = ReceiptWindow::front_key(&producer_id, state) {
                self.receipt_window.evictable.insert(key);
            }
        }
        false
    }

    /// Removes one producer and its receipts from the window.
    pub(super) fn remove_producer(&mut self, producer_id: &str) -> Option<ProducerState> {
        let state = self.producers.remove(producer_id)?;
        self.receipt_window.remove_producer(producer_id, &state);
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
    /// so a stream far over the cap (raised from level 0) costs O(P) per
    /// command, and `TidyStream` drains what the budget leaves.
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
            self.remove_producer(&victim);
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

    /// Whether the window has evictable excess.
    fn receipt_window_over(&self) -> bool {
        self.receipt_window.items > RECEIPT_WINDOW_ITEMS
            && !self.receipt_window.evictable.is_empty()
    }
}

impl StreamStateMachine {
    /// Whether F3 producer bounds apply (feature level 1).
    pub(super) fn producer_bounds_enabled(&self) -> bool {
        self.feature_level >= crate::feature::FEATURE_LEVEL_KEYED_STREAMS
    }

    /// F3 enforcement point, run once per command after it fully applied:
    /// the producer cap, then the receipt window.
    pub(super) fn enforce_producer_window(&mut self, stream_id: &BucketStreamId, now_ms: u64) {
        if !self.producer_bounds_enabled() {
            return;
        }
        if let Some(slot) = self.stream_slot_mut(stream_id) {
            slot.evict_producers_over_cap(now_ms, PRODUCER_CAP_EVICT_BUDGET);
            slot.trim_receipt_window(RECEIPT_TRIM_BUDGET);
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
        if !self.producer_bounds_enabled() {
            return;
        }
        let Some(slot) = self.stream_slot_mut(stream_id) else {
            return;
        };
        if slot
            .producers
            .get(producer_id)
            .is_some_and(|state| producer_is_idle(state, now_ms))
        {
            slot.remove_producer(producer_id);
        }
    }

    /// Whether `TidyStream` would change this stream at `now_ms`.
    pub fn stream_has_tidy_debt(&self, stream_id: &BucketStreamId, now_ms: u64) -> bool {
        if !self.producer_bounds_enabled() {
            return false;
        }
        let Some(slot) = self.stream_slot(stream_id) else {
            return false;
        };
        let seal_point = slot.seal_point();
        let collapsible = if self.message_records_removed() {
            // F4b: legacy records left after the raise are debt; tidy
            // converts them.
            !slot.message_records.is_empty()
        } else {
            slot.message_records
                .get(1)
                .is_some_and(|record| record.end_offset <= seal_point)
        };
        collapsible
            || self.stream_has_seal_debt(stream_id)
            || slot.receipt_window_over()
            || slot.producer_cap_over(now_ms)
            || slot.producers.values().any(|state| {
                state.last_seen_ms.is_none()
                    || !state.last_items.is_empty()
                    || producer_is_idle(state, now_ms)
            })
    }

    /// Up to `limit` streams with `TidyStream` debt at `now_ms`, in stream-id
    /// order. Read-only; a leader-side driver proposes `TidyStream` for them.
    pub fn tidy_candidates(&self, now_ms: u64, limit: usize) -> Vec<BucketStreamId> {
        if !self.producer_bounds_enabled() || limit == 0 {
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
        if let Err(response) =
            self.require_feature_level(crate::feature::FEATURE_LEVEL_KEYED_STREAMS, "stream tidy")
        {
            return response;
        }
        if let Err(response) = self.validate_stream_scope(stream_id) {
            return response;
        }
        if self.stream_slot(stream_id).is_none() {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        }
        self.collapse_sealed_message_records(stream_id);
        // F1 (level 2): legacy and idle streams seal here, at most
        // `SEAL_BUDGET_RECORDS` per command.
        self.seal_record_index(stream_id);
        let Some(slot) = self.stream_slot_mut(stream_id) else {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        };
        // Producer hygiene in producer-id order so every replica does the
        // same bounded work.
        let mut producer_ids = slot
            .producers
            .iter()
            .filter(|(_, state)| {
                state.last_seen_ms.is_none()
                    || !state.last_items.is_empty()
                    || producer_is_idle(state, now_ms)
            })
            .map(|(producer_id, _)| producer_id.clone())
            .collect::<Vec<_>>();
        producer_ids.sort();
        producer_ids.truncate(TIDY_PRODUCER_BUDGET);
        for producer_id in producer_ids {
            let idle = slot
                .producers
                .get(&producer_id)
                .is_some_and(|state| producer_is_idle(state, now_ms));
            if idle {
                slot.remove_producer(&producer_id);
                continue;
            }
            if let Some(state) = slot.producers.get_mut(&producer_id) {
                // Producers written before level 1 count their idle period
                // from the first tidy after the raise.
                state.last_seen_ms.get_or_insert(now_ms);
                state.last_items = Vec::new();
            }
        }
        slot.evict_producers_over_cap(now_ms, PRODUCER_CAP_EVICT_BUDGET);
        slot.trim_receipt_window(RECEIPT_TRIM_BUDGET);
        if slot.producers.capacity() > slot.producers.len().saturating_mul(2).saturating_add(8) {
            slot.producers.shrink_to_fit();
        }
        StreamResponse::StreamTidied {
            debt_remaining: self.stream_has_tidy_debt(stream_id, now_ms),
        }
    }

    /// F4a: collapses every message record that ends at or below the seal
    /// point into one `[retained, p)` record. Feature level 1.
    /// From level 4 (F4b) there is nothing to collapse: the call converts
    /// any legacy records instead.
    pub(super) fn collapse_sealed_message_records(&mut self, stream_id: &BucketStreamId) {
        if self.message_records_removed() {
            self.migrate_message_records(stream_id);
            return;
        }
        if !self.producer_bounds_enabled() {
            return;
        }
        let Some(slot) = self.stream_slot(stream_id) else {
            return;
        };
        let seal_point = slot.seal_point();
        let retained_offset = slot.retained_offset;
        if slot
            .message_records
            .get(1)
            .is_some_and(|record| record.end_offset <= seal_point)
        {
            self.compact_message_records_before(stream_id, retained_offset, seal_point);
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
