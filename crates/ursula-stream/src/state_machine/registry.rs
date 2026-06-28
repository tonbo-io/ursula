//! Stream registry: the keyed slot store plus its TTL expiry index.
//!
//! `keys` and `slots` form a bijection — every id in `keys` points at a live
//! slot, and slots are reachable only through it. The `ttl` min-heap may hold
//! stale entries (a stream deleted/recreated leaves its old entry behind); they
//! are validated and discarded lazily on [`StreamRegistry::pop_expired`]. Keeping
//! the three fields private to this type is what guarantees they stay in sync:
//! callers can only mutate them through `insert` / `remove` / `refresh_ttl`.

use std::cmp::Reverse;
use std::collections::HashMap;

use slotmap::SlotMap;

use super::BucketStreamId;
use super::StreamKey;
use super::StreamMetadata;
use super::StreamSlot;
use super::TtlEntry;
use super::TtlIndex;
use super::stream_expiry_at_ms;
use super::stream_is_expired;

#[derive(Debug, Clone, Default)]
pub(super) struct StreamRegistry {
    keys: HashMap<BucketStreamId, StreamKey>,
    slots: SlotMap<StreamKey, StreamSlot>,
    ttl: TtlIndex,
}

impl StreamRegistry {
    pub(super) fn key(&self, stream_id: &BucketStreamId) -> Option<StreamKey> {
        self.keys.get(stream_id).copied()
    }

    pub(super) fn slot(&self, stream_id: &BucketStreamId) -> Option<&StreamSlot> {
        let key = self.key(stream_id)?;
        self.slots.get(key)
    }

    pub(super) fn slot_mut(&mut self, stream_id: &BucketStreamId) -> Option<&mut StreamSlot> {
        let key = self.key(stream_id)?;
        self.slots.get_mut(key)
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

    /// Stream ids of every live slot, in arbitrary order.
    pub(super) fn stream_ids(&self) -> impl Iterator<Item = &BucketStreamId> {
        self.keys.keys()
    }

    /// Every live slot, in arbitrary order.
    pub(super) fn slots(&self) -> impl Iterator<Item = &StreamSlot> {
        self.slots.values()
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
        let key = self.slots.insert(slot);
        self.keys.insert(stream_id.clone(), key);
        self.push_ttl_entry(&stream_id, key);
        Some(key)
    }

    /// Remove a stream, returning its slot if it existed. Stale TTL entries are
    /// left for `pop_expired` to discard.
    pub(super) fn remove(&mut self, stream_id: &BucketStreamId) -> Option<StreamSlot> {
        let key = self.keys.remove(stream_id)?;
        self.slots.remove(key)
    }

    /// Re-stamp a stream's TTL entry after its expiry may have changed.
    pub(super) fn refresh_ttl(&mut self, stream_id: &BucketStreamId) {
        if let Some(key) = self.key(stream_id) {
            self.push_ttl_entry(stream_id, key);
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
            let Some(slot) = self.slots.get(entry.key) else {
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

    fn push_ttl_entry(&mut self, stream_id: &BucketStreamId, key: StreamKey) {
        let Some(slot) = self.slots.get(key) else {
            return;
        };
        let Some(expires_at_ms) = stream_expiry_at_ms(&slot.metadata) else {
            return;
        };
        self.ttl.entries.push(Reverse(TtlEntry {
            expires_at_ms,
            stream_id: stream_id.clone(),
            key,
        }));
    }
}
