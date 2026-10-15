//! `synced()` tickets (design §8.2). A ticket resolves when (1) every record up to its target is
//! acknowledged and (2) a round trip that started after the call found no foreign frame past
//! this owner's acknowledged offset: an append applied by that attempt (`200`, its receipt
//! contiguous with the expected tail), a claim applied by that attempt whose reconcile completed,
//! or an empty leader read at the tail. A `204` repeat proves nothing about what landed after
//! the earlier attempt applied it.
//! Attempt ids grow, so "started after the call" is "an attempt id at or above the next id at the
//! call". Concurrent callers share one probe, and under write load appends are the probe.

use super::Action;
use super::RequestId;
use crate::ids::Position;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Ticket {
    ticket: u64,
    target: Position,
    after: RequestId,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct Tickets {
    waiting: Vec<Ticket>,
    /// The latest attempt that proved the tail.
    proof: Option<RequestId>,
}

impl Tickets {
    pub(crate) fn add(&mut self, ticket: u64, target: Position, after: RequestId) {
        self.waiting.push(Ticket {
            ticket,
            target,
            after,
        });
    }

    /// Attempt `id` proved that nothing foreign sits past the acknowledged offset.
    pub(crate) fn prove(&mut self, id: RequestId) {
        self.proof = Some(self.proof.map_or(id, |proof| proof.max(id)));
    }

    fn proven(&self, ticket: &Ticket) -> bool {
        self.proof.is_some_and(|proof| proof >= ticket.after)
    }

    /// Resolves every ticket that both conditions hold for. `first_pending` is the first
    /// position not acknowledged: a ticket is covered when no commit at or below its target
    /// waits, whatever record (a commit, a CLAIM, an ACK, a BASE) sits at the target.
    pub(crate) fn settle(&mut self, first_pending: Option<Position>, actions: &mut Vec<Action>) {
        let proof = self.proof;
        self.waiting.retain(|t| {
            let done = covered(first_pending, t.target) && proof.is_some_and(|p| p >= t.after);
            if done {
                actions.push(Action::Synced { ticket: t.ticket });
            }
            !done
        });
    }

    /// Whether a probe is needed: a covered ticket waits for a round trip.
    pub(crate) fn wants_probe(&self, first_pending: Option<Position>) -> bool {
        self.waiting
            .iter()
            .any(|t| covered(first_pending, t.target) && !self.proven(t))
    }

    /// A takeover discarded every record from `from` on: tickets that wait for one never resolve.
    pub(crate) fn abandon_from(&mut self, from: Position, actions: &mut Vec<Action>) {
        self.waiting.retain(|t| {
            let lost = t.target >= from;
            if lost {
                actions.push(Action::Abandoned { ticket: t.ticket });
            }
            !lost
        });
    }
}

/// Every commit at or below `target` is acknowledged.
fn covered(first_pending: Option<Position>, target: Position) -> bool {
    first_pending.is_none_or(|first| first > target)
}
