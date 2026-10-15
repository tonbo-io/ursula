//! One stream of the model: its bytes, configuration, snapshot, retention, producer records, epoch
//! floor and what its current history inherited (#581 §2.3, §3, §4.8).

use std::collections::BTreeMap;
use std::sync::Arc;

use ursula_headers::Epoch;
use ursula_headers::Incarnation;
use ursula_headers::Inherited;
use ursula_headers::Offset;
use ursula_headers::SnapshotRef;

use super::producer::IDLE_RETIREMENT_MS;
use super::producer::ProducerRecord;

/// What the stream's current history began from, in history ids (#581 §4.8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InheritedFrom {
    pub(crate) history: u64,
    pub(crate) offset: Offset,
}

/// The latest published snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Snapshot {
    pub(crate) snapshot: SnapshotRef,
    pub(crate) content_type: String,
    pub(crate) body: Arc<[u8]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Stream {
    /// Unique per group (C7): the token derives from it and the history id.
    pub(crate) created_at_ms: u64,
    pub(crate) content_type: String,
    /// Every byte from offset 0: offsets are byte positions. Retention hides a prefix from reads.
    pub(crate) bytes: Vec<u8>,
    pub(crate) closed: bool,
    pub(crate) retained: Offset,
    pub(crate) snapshot: Option<Snapshot>,
    /// By `Producer-Id`.
    pub(crate) producers: BTreeMap<String, ProducerRecord>,
    /// `retired_epoch_watermark`, the epoch floor: the highest epoch of any record retired.
    pub(crate) watermark: Option<Epoch>,
    /// The group's history id when the stream was created or last written.
    pub(crate) history: u64,
    pub(crate) inherited: Option<InheritedFrom>,
}

impl Stream {
    pub(crate) fn new(created_at_ms: u64, content_type: String, history: u64) -> Self {
        Self {
            created_at_ms,
            content_type,
            bytes: Vec::new(),
            closed: false,
            retained: Offset::ZERO,
            snapshot: None,
            producers: BTreeMap::new(),
            watermark: None,
            history,
            inherited: None,
        }
    }

    pub(crate) fn tail(&self) -> Offset {
        Offset::from_u64(u64::try_from(self.bytes.len()).unwrap_or(u64::MAX))
    }

    /// The stream's token in history `history` (#581 §4.2).
    pub(crate) fn token_in(&self, history: u64) -> Incarnation {
        super::incarnation_token(self.created_at_ms, history)
    }

    /// What HEAD reports about the history the current one began from (#581 §4.8).
    pub(crate) fn inherited(&self, group_history: u64) -> Option<Inherited> {
        if self.history != group_history {
            return Some(Inherited {
                previous_incarnation: self.token_in(self.history),
                offset: self.tail(),
            });
        }
        self.inherited.map(|from| Inherited {
            previous_incarnation: self.token_in(from.history),
            offset: from.offset,
        })
    }

    /// Before a write changes the bytes or the closure: the first such write since a history event
    /// records what the current history inherited (no write since, so the tail is the tail at the
    /// event).
    fn written_in(&mut self, group_history: u64) {
        if self.history != group_history {
            self.inherited = Some(InheritedFrom {
                history: self.history,
                offset: self.tail(),
            });
            self.history = group_history;
        }
    }

    /// Appends `bytes`, returning the range they landed at.
    pub(crate) fn append(&mut self, bytes: &[u8], group_history: u64) -> (Offset, Offset) {
        self.written_in(group_history);
        let start = self.tail();
        self.bytes.extend_from_slice(bytes);
        (start, self.tail())
    }

    pub(crate) fn close(&mut self, group_history: u64) {
        self.written_in(group_history);
        self.closed = true;
    }

    /// Retires producer `id`'s record when it is idle at `now_ms`, raising the floor to its epoch.
    pub(crate) fn retire_idle(&mut self, id: &str, now_ms: u64) {
        let idle = self.producers.get(id).is_some_and(|r| idle(r, now_ms));
        if idle && let Some(record) = self.producers.remove(id) {
            let epoch = record.epoch.epoch();
            self.watermark = Some(self.watermark.map_or(epoch, |w| w.max(epoch)));
        }
    }
}

/// Whether `record` is idle at `now_ms`, so retired at its producer's next write.
fn idle(record: &ProducerRecord, now_ms: u64) -> bool {
    now_ms.saturating_sub(record.last_seen_ms) >= IDLE_RETIREMENT_MS
}
