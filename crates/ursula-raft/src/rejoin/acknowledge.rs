//! Whether a replica acknowledges its group's leader again since it opened
//! in this run: the restart half of serving readiness (design 10).
//!
//! A replica that restarted may know its group's leader from its persisted
//! vote before that leader counts it: until the replica accepted an append
//! that took its log to the leader's commit index, a quorum that needs it
//! does not form. A node that turned ready in that gap would let a rolling
//! restart stop the group's next voter while the group has no live quorum.
//!
//! [`Acknowledgements`] records, per replica and run, when a committed
//! leader's append last reached it ([`Acknowledgements::contacted`], before
//! any screen, so a refused append counts as contact) and whether OpenRaft
//! accepted one that reached the leader's commit index
//! ([`Acknowledgements::answered`]). Once acknowledged, the replica stays
//! acknowledged for the run: readiness reflects this node's own catch-up,
//! never a leader lost later on other nodes. A replica that leads with a
//! live quorum holds every committed entry and is acknowledged too.
//!
//! The wait is bounded: a replica that no leader's append reached within
//! [`LEADER_CONTACT_GRACE`] of its open, nor within
//! [`super::LIVE_LEADER_QUORUM_ACK`] of now, belongs to a group with no
//! leader that could count it, which is other nodes' state, and reports
//! `no_leader_contact`.

use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use openraft::raft::AppendEntriesResponse;
use ursula_proto::admin::LeaderAcknowledgement;

use crate::rt::time::Instant;
use crate::types::UrsulaAppendEntriesRequest;
use crate::types::UrsulaRaftTypeConfig;

/// How long after its open a replica that no leader's append reached keeps
/// its node unready: several election timeouts, and the time a leader takes
/// to dial a peer that restarted again.
pub const LEADER_CONTACT_GRACE: Duration = Duration::from_secs(10);

/// One replica's acknowledgement of its group's leaders in this run.
#[derive(Debug)]
pub(crate) struct Acknowledgements {
    opened: Instant,
    /// When a committed leader's append last reached the replica, kept
    /// until it acknowledged one.
    contact: Mutex<Option<Instant>>,
    acknowledged: AtomicBool,
}

/// What an inbound append proves once OpenRaft accepts it: the index the
/// follower then matches the leader up to, and the leader's commit index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LeaderAppend {
    matched: Option<u64>,
    commit: Option<u64>,
}

impl LeaderAppend {
    /// The proof `request` carries, `None` for an append without a
    /// committed vote, which no leader sends.
    fn of(request: &UrsulaAppendEntriesRequest) -> Option<Self> {
        request.vote.is_committed().then(|| Self {
            matched: request
                .entries
                .last()
                .map(|entry| entry.log_id.index)
                .or_else(|| request.prev_log_id.map(|id| id.index)),
            commit: request.leader_commit.map(|id| id.index),
        })
    }

    /// Whether `response` acknowledges the leader: the follower accepted
    /// the append, and its log matches the leader's up to its commit index.
    fn acknowledged_by(self, response: &AppendEntriesResponse<UrsulaRaftTypeConfig>) -> bool {
        let matched = match response {
            AppendEntriesResponse::Success => self.matched,
            AppendEntriesResponse::PartialSuccess(matched) => matched.map(|id| id.index),
            AppendEntriesResponse::Conflict | AppendEntriesResponse::HigherVote(_) => return false,
        };
        matched >= self.commit
    }
}

impl Acknowledgements {
    /// A replica opened now that acknowledged no leader yet.
    pub(crate) fn new() -> Self {
        Self {
            opened: Instant::now(),
            contact: Mutex::new(None),
            acknowledged: AtomicBool::new(false),
        }
    }

    /// Whether the replica acknowledged a leader in this run.
    pub(crate) fn acknowledged(&self) -> bool {
        self.acknowledged.load(Ordering::Acquire)
    }

    /// Records that `request` reached the replica, and returns what it
    /// proves once OpenRaft accepts it ([`Self::answered`]). Once the
    /// replica acknowledged a leader in this run nothing is recorded.
    pub(crate) fn contacted(&self, request: &UrsulaAppendEntriesRequest) -> Option<LeaderAppend> {
        if self.acknowledged() {
            return None;
        }
        let append = LeaderAppend::of(request)?;
        *self.contact.lock().unwrap_or_else(PoisonError::into_inner) = Some(Instant::now());
        Some(append)
    }

    /// Records OpenRaft's `response` to the append `append` describes.
    pub(crate) fn answered(
        &self,
        append: LeaderAppend,
        response: &AppendEntriesResponse<UrsulaRaftTypeConfig>,
    ) {
        if append.acknowledged_by(response) {
            self.acknowledged.store(true, Ordering::Release);
        }
    }

    /// The replica's acknowledgement now; `leads_live` when it leads with
    /// a live quorum ([`super::leads_live`]).
    pub(crate) fn status(&self, leads_live: bool) -> LeaderAcknowledgement {
        let contact = *self.contact.lock().unwrap_or_else(PoisonError::into_inner);
        let now = Instant::now();
        status_of(
            self.acknowledged() || leads_live,
            now.saturating_duration_since(self.opened),
            contact.map(|contact| now.saturating_duration_since(contact)),
        )
    }
}

/// The acknowledgement of a replica that `acknowledged` its leader or not,
/// opened `opened_for` ago and last contacted by a leader `contact_age` ago.
fn status_of(
    acknowledged: bool,
    opened_for: Duration,
    contact_age: Option<Duration>,
) -> LeaderAcknowledgement {
    if acknowledged {
        LeaderAcknowledgement::Acknowledged
    } else if opened_for < LEADER_CONTACT_GRACE
        || contact_age.is_some_and(|age| age < super::LIVE_LEADER_QUORUM_ACK)
    {
        LeaderAcknowledgement::Awaiting
    } else {
        LeaderAcknowledgement::NoLeaderContact
    }
}

#[cfg(test)]
mod tests {
    use openraft::alias::LogIdOf;
    use openraft::vote::RaftLeaderId;

    use super::*;
    use crate::types::UrsulaVote;

    fn log_id(term: u64, index: u64) -> LogIdOf<UrsulaRaftTypeConfig> {
        type LeaderId = <UrsulaRaftTypeConfig as openraft::RaftTypeConfig>::LeaderId;
        openraft::LogId::new(LeaderId::new(term, 1), index)
    }

    fn append(prev: Option<u64>, last: Option<u64>, commit: Option<u64>) -> LeaderAppend {
        LeaderAppend {
            matched: last.or(prev),
            commit,
        }
    }

    #[test]
    fn an_accepted_append_acknowledges_only_once_the_log_reaches_the_commit_index() {
        let success = AppendEntriesResponse::Success;
        assert!(append(Some(4), Some(9), Some(9)).acknowledged_by(&success));
        assert!(append(Some(9), None, Some(7)).acknowledged_by(&success));
        assert!(!append(Some(2), Some(5), Some(9)).acknowledged_by(&success));
        assert!(append(None, None, None).acknowledged_by(&success));
        let partial = AppendEntriesResponse::PartialSuccess(Some(log_id(1, 6)));
        assert!(!append(Some(2), Some(12), Some(9)).acknowledged_by(&partial));
        let partial = AppendEntriesResponse::PartialSuccess(Some(log_id(1, 9)));
        assert!(append(Some(2), Some(12), Some(9)).acknowledged_by(&partial));
        for refused in [
            AppendEntriesResponse::Conflict,
            AppendEntriesResponse::HigherVote(UrsulaVote::new_committed(3, 2)),
        ] {
            assert!(!append(Some(9), Some(9), Some(9)).acknowledged_by(&refused));
        }
    }

    #[test]
    fn the_wait_ends_with_an_acknowledgement_or_without_leader_contact() {
        let young = Duration::from_secs(1);
        let old = LEADER_CONTACT_GRACE;
        let recent = Some(Duration::from_millis(200));
        let stale = Some(super::super::LIVE_LEADER_QUORUM_ACK);
        assert_eq!(
            status_of(true, young, None),
            LeaderAcknowledgement::Acknowledged
        );
        assert_eq!(
            status_of(false, young, None),
            LeaderAcknowledgement::Awaiting
        );
        assert_eq!(
            status_of(false, old, recent),
            LeaderAcknowledgement::Awaiting
        );
        assert_eq!(
            status_of(false, old, None),
            LeaderAcknowledgement::NoLeaderContact
        );
        assert_eq!(
            status_of(false, old, stale),
            LeaderAcknowledgement::NoLeaderContact
        );
    }
}
