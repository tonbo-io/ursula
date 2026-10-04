//! Stream registry: the id-indexed slot store plus its TTL expiry index.
//!
//! `keys` and `slots` form a bijection — every id in `keys` points at a live
//! slot, and slots are reachable only through it. Keeping the fields private
//! to this type is what guarantees they stay in sync: callers can only mutate
//! them through `insert` / `remove` / `refresh_ttl`.
//!
//! The TTL index keeps at most one *armed* heap entry per stream (F8): `armed`
//! records, per live slot, the expiry its armed entry was pushed at. A refresh
//! pushes only when the stream has no armed entry or its expiry moved earlier;
//! an armed entry that pops before the stream's current expiry is re-pushed at
//! that expiry. Every entry `pop_expired` returns is therefore at its stream's
//! true expiry with every smaller key already processed, so the pop order is
//! identical to pushing one entry per refresh. Unarmed (stale) entries are
//! discarded lazily, and the heap is rebuilt from `armed` once it holds more
//! than twice as many entries. Slots are boxed so a vacant `SlotMap` slot
//! costs a pointer, and the keys map shrinks after deletes (F7).

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;

use slotmap::SecondaryMap;
use slotmap::SlotMap;

use super::BucketStreamId;
use super::StreamKey;
use super::StreamMetadata;
use super::StreamSlot;
use super::TtlEntry;
use super::TtlIndex;
use super::stream_expiry_at_ms;
use super::stream_is_expired;

type SlotEntry = Box<StreamSlot>;

#[derive(Debug, Clone, Default)]
pub(super) struct StreamRegistry {
    keys: HashMap<BucketStreamId, StreamKey>,
    slots: SlotMap<StreamKey, SlotEntry>,
    ttl: TtlIndex,
    /// Expiry of each live stream's armed TTL heap entry. Node-local, never
    /// replicated; derived again from metadata when a registry is rebuilt.
    armed: SecondaryMap<StreamKey, u64>,
}

impl StreamRegistry {
    pub(super) fn key(&self, stream_id: &BucketStreamId) -> Option<StreamKey> {
        self.keys.get(stream_id).copied()
    }

    pub(super) fn slot(&self, stream_id: &BucketStreamId) -> Option<&StreamSlot> {
        let key = self.key(stream_id)?;
        self.slots.get(key).map(|slot| &**slot)
    }

    pub(super) fn slot_mut(&mut self, stream_id: &BucketStreamId) -> Option<&mut StreamSlot> {
        let key = self.key(stream_id)?;
        self.slots.get_mut(key).map(|slot| &mut **slot)
    }

    pub(super) fn metadata(&self, stream_id: &BucketStreamId) -> Option<&StreamMetadata> {
        self.slot(stream_id).map(|slot| &slot.metadata)
    }

    pub(super) fn metadata_mut(
        &mut self,
        stream_id: &BucketStreamId,
    ) -> Option<&mut StreamMetadata> {
        self.slot_mut(stream_id).map(|slot| &mut slot.metadata)
    }

    pub(super) fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Stream ids of every live slot, in arbitrary order.
    pub(super) fn stream_ids(&self) -> impl Iterator<Item = &BucketStreamId> {
        self.keys.keys()
    }

    /// Every live slot, in arbitrary order.
    pub(super) fn slots(&self) -> impl Iterator<Item = &StreamSlot> {
        self.slots.values().map(|slot| &**slot)
    }

    pub(super) fn contains_key(&self, stream_id: &BucketStreamId) -> bool {
        self.keys.contains_key(stream_id)
    }

    /// Insert a fresh slot, returning its key, or `None` if the id already exists.
    pub(super) fn insert(&mut self, slot: StreamSlot) -> Option<StreamKey> {
        let stream_id = slot.metadata.stream_id.clone();
        if self.keys.contains_key(&stream_id) {
            return None;
        }
        let key = self.slots.insert(Box::new(slot));
        self.keys.insert(stream_id.clone(), key);
        self.arm_ttl_entry(&stream_id, key);
        Some(key)
    }

    /// Remove a stream, returning its slot if it existed. Its TTL heap entry,
    /// if any, is left for `pop_expired` (or the next rebuild) to discard.
    pub(super) fn remove(&mut self, stream_id: &BucketStreamId) -> Option<StreamSlot> {
        let key = self.keys.remove(stream_id)?;
        self.armed.remove(key);
        let slot = self.slots.remove(key).map(|slot| *slot);
        self.shrink_keys_if_sparse();
        self.rebuild_ttl_if_bloated();
        slot
    }

    /// Entries in the TTL min-heap, stale ones included (bounded-state gauge).
    pub(super) fn ttl_heap_len(&self) -> usize {
        self.ttl.entries.len()
    }

    /// Re-stamp a stream's TTL entry after its expiry may have changed.
    pub(super) fn refresh_ttl(&mut self, stream_id: &BucketStreamId) {
        if let Some(key) = self.key(stream_id) {
            self.arm_ttl_entry(stream_id, key);
            self.rebuild_ttl_if_bloated();
        }
    }

    /// Pop the next genuinely-expired stream, discarding stale heap entries along
    /// the way. Returns `None` when the heap is empty or its front is not yet due.
    pub(super) fn pop_expired(&mut self, now_ms: u64) -> Option<BucketStreamId> {
        loop {
            let Reverse(entry) = self.ttl.entries.peek().cloned()?;
            if entry.expires_at_ms > now_ms {
                return None;
            }
            self.ttl.entries.pop();
            if self.key(&entry.stream_id) != Some(entry.key) {
                continue;
            }
            if self.armed.get(entry.key).copied() != Some(entry.expires_at_ms) {
                continue;
            }
            let current = self
                .slots
                .get(entry.key)
                .and_then(|slot| stream_expiry_at_ms(&slot.metadata));
            match current {
                Some(expires_at_ms) if expires_at_ms > entry.expires_at_ms => {
                    // Renewed since it was armed: re-arm at the true expiry,
                    // which sorts after every key processed so far.
                    self.armed.insert(entry.key, expires_at_ms);
                    self.ttl.entries.push(Reverse(TtlEntry {
                        expires_at_ms,
                        ..entry
                    }));
                    continue;
                }
                Some(expires_at_ms) if expires_at_ms == entry.expires_at_ms => {
                    self.armed.remove(entry.key);
                    let expired = self
                        .slots
                        .get(entry.key)
                        .is_some_and(|slot| stream_is_expired(&slot.metadata, now_ms));
                    if !expired {
                        continue;
                    }
                    return Some(entry.stream_id);
                }
                _ => {
                    self.armed.remove(entry.key);
                    continue;
                }
            }
        }
    }

    fn arm_ttl_entry(&mut self, stream_id: &BucketStreamId, key: StreamKey) {
        let Some(slot) = self.slots.get(key) else {
            return;
        };
        let Some(expires_at_ms) = stream_expiry_at_ms(&slot.metadata) else {
            return;
        };
        if self
            .armed
            .get(key)
            .is_some_and(|armed_at| *armed_at <= expires_at_ms)
        {
            return;
        }
        self.armed.insert(key, expires_at_ms);
        self.ttl.entries.push(Reverse(TtlEntry {
            expires_at_ms,
            stream_id: stream_id.clone(),
            key,
        }));
    }

    /// Drop every unarmed entry once they outnumber the armed ones. The kept
    /// entries are exactly those `pop_expired` would act on, so pop order is
    /// unchanged; each rebuild is paid for by the removals that created as
    /// many stale entries as there are armed ones.
    fn rebuild_ttl_if_bloated(&mut self) {
        if self.ttl.entries.len() <= self.armed.len().saturating_mul(2) {
            return;
        }
        let mut entries = Vec::with_capacity(self.armed.len());
        for (key, expires_at_ms) in &self.armed {
            if let Some(slot) = self.slots.get(key) {
                entries.push(Reverse(TtlEntry {
                    expires_at_ms: *expires_at_ms,
                    stream_id: slot.metadata.stream_id.clone(),
                    key,
                }));
            }
        }
        self.ttl.entries = BinaryHeap::from(entries);
    }

    fn shrink_keys_if_sparse(&mut self) {
        let capacity = self.keys.capacity();
        if capacity > 64 && self.keys.len() < capacity / 4 {
            self.keys.shrink_to(self.keys.len().saturating_mul(2));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BinaryHeap;

    use super::*;
    use crate::model::StreamStatus;
    use crate::state_machine::HotBuffer;
    use crate::state_machine::StreamColdState;

    fn slot(
        name: &str,
        ttl_seconds: Option<u64>,
        expires_at_ms: Option<u64>,
        now: u64,
    ) -> StreamSlot {
        StreamSlot {
            metadata: StreamMetadata {
                stream_id: BucketStreamId::new("bucket", name),
                content_type: "application/octet-stream".to_owned(),
                status: StreamStatus::Open,
                tail_offset: 0,
                last_stream_seq: None,
                stream_ttl_seconds: ttl_seconds,
                stream_expires_at_ms: expires_at_ms,
                created_at_ms: now,
                last_ttl_touch_at_ms: now,
            },
            hot_buffer: HotBuffer::default(),
            cold: StreamColdState::default(),
            message_records: Vec::new(),
            record_index: None,
            retained_offset: 0,
            visible_snapshot: None,
            producers: HashMap::new(),
            receipt_window: Default::default(),
            append_count: 0,
        }
    }

    /// The pre-F8 TTL index: one heap entry per refresh, validated lazily.
    #[derive(Default)]
    struct ReferenceTtl {
        entries: BinaryHeap<Reverse<TtlEntry>>,
    }

    impl ReferenceTtl {
        fn push(&mut self, registry: &StreamRegistry, stream_id: &BucketStreamId) {
            let Some(key) = registry.key(stream_id) else {
                return;
            };
            let Some(expires_at_ms) = registry
                .slots
                .get(key)
                .and_then(|slot| stream_expiry_at_ms(&slot.metadata))
            else {
                return;
            };
            self.entries.push(Reverse(TtlEntry {
                expires_at_ms,
                stream_id: stream_id.clone(),
                key,
            }));
        }

        fn pop_expired(
            &mut self,
            registry: &StreamRegistry,
            now_ms: u64,
        ) -> Option<BucketStreamId> {
            loop {
                let Reverse(entry) = self.entries.peek().cloned()?;
                if entry.expires_at_ms > now_ms {
                    return None;
                }
                self.entries.pop();
                if registry.key(&entry.stream_id) != Some(entry.key) {
                    continue;
                }
                let Some(slot) = registry.slots.get(entry.key) else {
                    continue;
                };
                if stream_expiry_at_ms(&slot.metadata) != Some(entry.expires_at_ms) {
                    continue;
                }
                if !stream_is_expired(&slot.metadata, now_ms) {
                    continue;
                }
                return Some(entry.stream_id);
            }
        }
    }

    struct Lcg(u64);

    impl Lcg {
        fn next(&mut self, bound: u64) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (self.0 >> 33) % bound
        }
    }

    fn live_ttl_streams(registry: &StreamRegistry) -> usize {
        registry
            .slots()
            .filter(|slot| stream_expiry_at_ms(&slot.metadata).is_some())
            .count()
    }

    #[test]
    fn ttl_index_pops_match_reference_under_random_workloads() {
        for seed in 0..64u64 {
            let mut rng = Lcg(seed);
            let mut registry = StreamRegistry::default();
            let mut reference = ReferenceTtl::default();
            let mut now = 1_000u64;
            for _ in 0..2_000 {
                now += rng.next(400);
                let name = format!("s{}", rng.next(12));
                let stream_id = BucketStreamId::new("bucket", &name);
                match rng.next(10) {
                    0 | 1 => {
                        let (ttl, at) = match rng.next(3) {
                            0 => (Some(1 + rng.next(5)), None),
                            1 => (None, Some(now + rng.next(5_000))),
                            _ => (None, None),
                        };
                        if registry.insert(slot(&name, ttl, at, now)).is_some() {
                            reference.push(&registry, &stream_id);
                        }
                    }
                    2 => {
                        registry.remove(&stream_id);
                    }
                    3..=6 => {
                        // Sliding renewal, as appends and accesses do.
                        if let Some(metadata) = registry.metadata_mut(&stream_id) {
                            metadata.last_ttl_touch_at_ms = now;
                        }
                        registry.refresh_ttl(&stream_id);
                        reference.push(&registry, &stream_id);
                    }
                    7 => {
                        // Expiry moves earlier, which no command does today; the
                        // index must still match the reference.
                        if let Some(metadata) = registry.metadata_mut(&stream_id)
                            && let Some(at) = metadata.stream_expires_at_ms
                        {
                            metadata.stream_expires_at_ms =
                                Some(at.saturating_sub(rng.next(3_000)));
                        }
                        registry.refresh_ttl(&stream_id);
                        reference.push(&registry, &stream_id);
                    }
                    _ => loop {
                        let actual = registry.pop_expired(now);
                        let expected = reference.pop_expired(&registry, now);
                        assert_eq!(actual, expected, "seed {seed} at {now}");
                        let Some(expired) = actual else {
                            break;
                        };
                        registry.remove(&expired);
                    },
                }
                assert!(
                    registry.ttl.entries.len() <= 2 * live_ttl_streams(&registry) + 1,
                    "seed {seed}: {} heap entries for {} TTL streams",
                    registry.ttl.entries.len(),
                    live_ttl_streams(&registry)
                );
            }
        }
    }

    #[test]
    fn ttl_heap_does_not_grow_per_refresh() {
        // Measured before F8: 1M appends to one TTL stream left 1M heap
        // entries (104 MB), surviving the stream's delete.
        let mut registry = StreamRegistry::default();
        let stream_id = BucketStreamId::new("bucket", "ttl");
        registry.insert(slot("ttl", Some(3_600), None, 0));
        for now in 0..10_000u64 {
            if let Some(metadata) = registry.metadata_mut(&stream_id) {
                metadata.last_ttl_touch_at_ms = now;
            }
            registry.refresh_ttl(&stream_id);
        }
        assert!(registry.ttl.entries.len() <= 2);
        registry.remove(&stream_id);
        assert!(registry.ttl.entries.len() <= 1);
        assert_eq!(registry.pop_expired(u64::MAX), None);
        assert!(registry.ttl.entries.is_empty());
    }

    #[test]
    fn removed_slots_and_keys_release_capacity() {
        let mut registry = StreamRegistry::default();
        for index in 0..10_000 {
            registry.insert(slot(&format!("s{index}"), None, None, 0));
        }
        for index in 10..10_000 {
            registry.remove(&BucketStreamId::new("bucket", format!("s{index}")));
        }
        assert_eq!(registry.keys.len(), 10);
        assert!(
            registry.keys.capacity() <= 4 * 64,
            "keys capacity {} after deletes",
            registry.keys.capacity()
        );
        // A vacant slot holds a pointer, not a whole `StreamSlot`.
        assert!(std::mem::size_of::<SlotEntry>() <= 16);
    }
}
