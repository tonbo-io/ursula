//! History events and the token every response renders (#581 section 4).
//!
//! - [`StreamStateMachine::new_history`] applies `NewHistory` in O(1): it
//!   records the event and raises the C7 counter. Every token of the group
//!   changes because [`StreamStateMachine::stream_incarnation`] derives it
//!   from the group's history id, and every producer record is
//!   history-fenced because the record keeps the id it was claimed in.
//! - [`StreamSlot::inherit_history`] runs before a write changes a stream's
//!   bytes or closure: the first such write after a history event records
//!   what the stream's current history began from (W5).
//! - [`StreamStateMachine::stream_inherited`] is what `HEAD` reports.

use std::num::NonZeroU64;

use super::BucketStreamId;
use super::HistoryEvent;
use super::InheritedHistory;
use super::StreamMetadata;
use super::StreamResponse;
use super::StreamSlot;
use super::StreamStateMachine;
use crate::history::InheritedReport;
use crate::history::incarnation_token;

impl StreamStateMachine {
    /// The group's latest history event; `None` before its genesis.
    pub fn history(&self) -> Option<HistoryEvent> {
        self.history
    }

    /// The group's history id; 0 before its genesis, when the group holds
    /// no stream in production (the leader admits nothing but the genesis
    /// event first).
    pub fn history_id(&self) -> u64 {
        self.history.map_or(0, |history| history.id.get())
    }

    /// Applies [`crate::StreamCommand::NewHistory`] of the entry at `index`.
    pub(super) fn new_history(
        &mut self,
        index: u64,
        history: NonZeroU64,
        now_ms: u64,
    ) -> StreamResponse {
        self.history = Some(HistoryEvent {
            id: history,
            event_index: index,
            imported: self.history.and_then(|previous| previous.imported),
        });
        // A lost suffix also lost the C7 counter: without this a create
        // after the loss could reuse the `created_at_ms`, and with it the
        // cold generation and cold-index paths, of a lost incarnation.
        self.last_created_at_ms = self.last_created_at_ms.max(now_ms);
        StreamResponse::HistoryStarted { history }
    }

    /// The token of a stream incarnation in the group's current history.
    pub(super) fn token_of(&self, stream: &StreamMetadata) -> u64 {
        incarnation_token(stream.created_at_ms, self.history_id())
    }

    /// The stream's public `Stream-Incarnation` token, or `None` when there
    /// is no stream. Every response that renders the token, and every
    /// precondition that compares one, reads it here, from the state that
    /// serves or applies it.
    pub fn stream_incarnation(&self, stream_id: &BucketStreamId) -> Option<u64> {
        self.stream_metadata(stream_id)
            .map(|stream| self.token_of(stream))
    }

    /// What the stream's current history began from, as `HEAD` reports it
    /// (W5): the stream's token in the earlier history and the offset below
    /// which its bytes are unchanged. `None` when the stream has not been
    /// through a history event since it was created.
    pub fn stream_inherited(&self, stream_id: &BucketStreamId) -> Option<InheritedReport> {
        let slot = self.stream_slot(stream_id)?;
        let created_at_ms = slot.metadata.created_at_ms;
        let (history, offset) = if slot.history != self.history_id() {
            // No write since the event: the whole stream is inherited.
            (slot.history, slot.metadata.tail_offset)
        } else {
            let inherited = slot.inherited?;
            (inherited.history, inherited.offset)
        };
        Some(InheritedReport {
            previous_incarnation: incarnation_token(created_at_ms, history),
            offset,
        })
    }

    /// Records, before a write changes the stream's bytes or closure, what
    /// its current history began from (W5).
    pub(super) fn inherit_history(&mut self, stream_id: &BucketStreamId) {
        let group = self.history_id();
        if let Some(slot) = self.stream_slot_mut(stream_id) {
            slot.inherit_history(group);
        }
    }
}

impl StreamSlot {
    /// The first write after a history event: no byte changed since the
    /// event, so the tail now is the tail at the event.
    pub(super) fn inherit_history(&mut self, group: u64) {
        if self.history != group {
            self.inherited = Some(InheritedHistory {
                history: self.history,
                offset: self.metadata.tail_offset,
            });
            self.history = group;
        }
    }
}
