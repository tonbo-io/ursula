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
use crate::usage::VersionCounters;

/// Whether a stream whose bytes last changed in history `stream_history`
/// was written since its group's latest history event, `group_history`. A
/// stream not written since keeps naming the history it was last written
/// in, through any number of events: its whole tail is inherited from that
/// history (W5). The one rule `HEAD` and the first write after an event
/// follow, and an import keeps each stream's history so that it holds there
/// too (#581 section 4.8, extensions section 2.4).
pub(super) fn written_since_event(stream_history: u64, group_history: u64) -> bool {
    stream_history == group_history
}

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
        previous: u64,
        kept: u64,
        leader: crate::HistoryLeader,
        now_ms: u64,
    ) -> StreamResponse {
        // The proposer applied the whole log before it proposed, so another
        // event in between means two proposers raced: this one replaces a
        // history it never saw, and its `previous` would misname it.
        if previous != self.history_id() {
            return StreamResponse::error(
                crate::StreamErrorCode::HistoryMismatch,
                format!(
                    "history event names {previous:#x} as the history it replaces, but the \
                     group's history is {:#x}",
                    self.history_id()
                ),
            );
        }
        if previous == 0 {
            self.usage.record_genesis(history);
        }
        self.history = Some(HistoryEvent {
            id: history,
            event_index: index,
            previous,
            kept,
            leader,
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
        let (history, offset) = if !written_since_event(slot.history, self.history_id()) {
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
    /// The first write after a history event: no byte changed and nothing
    /// counted since the event, so the tail and the usage counters now are
    /// those at the event. The counters become the version's history
    /// baseline before the write counts (F20).
    pub(super) fn inherit_history(&mut self, group: u64) {
        if !written_since_event(self.history, group) {
            self.inherited = Some(InheritedHistory {
                history: self.history,
                offset: self.metadata.tail_offset,
            });
            self.usage.history_baseline = self.usage.counters;
            self.history = group;
        }
    }

    /// The version's counters when the group's history `group` began for
    /// it: its recorded baseline when it was created, restored or written
    /// in `group`, else its counters, which nothing changed since `group`
    /// began.
    pub(super) fn history_baseline_in(&self, group: u64) -> VersionCounters {
        if written_since_event(self.history, group) {
            self.usage.history_baseline
        } else {
            self.usage.counters
        }
    }
}
