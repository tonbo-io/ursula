//! One replica's view of its group's leadership, taken from one borrow of
//! its Raft metrics. The front door routes writes by it, the engine routes
//! writes and names the leader's term in its forward hints by it, and both
//! fence cold uploads by it (F14j), so all decide by one policy.
//!
//! The node fence takes part: OpenRaft keeps a leader whose disk lease
//! lapsed in its leader state, and the fence blocks every message that
//! could depose it, so a fenced replica leads nothing here, and every
//! leader-gated job stops at this one site.
//!
//! So does OpenRaft's leader lease: a leader that no quorum acknowledged
//! within it stays in OpenRaft's leader state until a message with a higher
//! term reaches it, which a leader cut off from its peers never gets. Such
//! a leader leads nothing here either ([`LeaderRole::Lapsed`]): a write
//! routed to it is refused before its body, and a write it proposed earlier
//! stops waiting for an answer only its quorum could give ([`until_lapsed`]).

use std::time::Duration;

use openraft::RaftMetrics;
use openraft::ServerState;
use openraft::rt::Instant as _;
use openraft::rt::WatchReceiver;
use openraft::type_config::alias::WatchReceiverOf;
use ursula_runtime::CommittedLeader;
use ursula_runtime::LeadershipReason;
use ursula_runtime::NodeFence;
use ursula_runtime::UploadFence;
use ursula_runtime::WriteRoute;
use ursula_runtime::node_fence::fence_now;

use crate::types::UrsulaRaftTypeConfig;

/// This replica's role in its group, as its Raft metrics show it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaderRole {
    /// This replica is the leader (`ServerState::Leader`).
    Leads,
    /// Another node leads the group.
    Follows { leader_id: u64 },
    /// No leader is known, or this replica is between roles.
    Unknown,
    /// OpenRaft keeps this replica in its leader state, but it cannot
    /// commit: its node fence does not admit it, or no quorum acknowledged
    /// it within its leader lease. OpenRaft answers a write it proposed
    /// only once the write commits or a newer leader's log replaces it, so
    /// a lapsed leader cut off from its peers would hold the write for as
    /// long as the cut lasts.
    Lapsed,
}

/// A replica's view of its group's leadership. It can lag an election: a
/// write routed by a stale view either fails after its body (a replica that
/// thinks it leads) or reaches a leader that answers it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupLeadership {
    /// This replica's node.
    pub node_id: u64,
    /// The term of this view.
    pub term: u64,
    pub role: LeaderRole,
}

impl GroupLeadership {
    /// The view `metrics` show behind `fence`, for a group whose OpenRaft
    /// leader lease is `lease` ([`leader_lease`]). Only `ServerState::Leader`
    /// leads: a node that names itself leader in any other state is between
    /// roles. While the fence does not admit, a leader has lapsed and any
    /// other replica's role is unknown. After the fence lifted, a leader
    /// leads again only once OpenRaft confirmed its leadership
    /// ([`fence_lets_lead`]). A leader that no quorum acknowledged within
    /// `lease` has lapsed ([`lease_holds`]).
    pub fn of(
        metrics: &RaftMetrics<UrsulaRaftTypeConfig>,
        fence: &NodeFence,
        lease: Duration,
    ) -> Self {
        let now = fence_now();
        let leader = metrics.state == ServerState::Leader;
        let role = if !fence.admits(now) && fence_rules_leadership() {
            if leader {
                LeaderRole::Lapsed
            } else {
                LeaderRole::Unknown
            }
        } else if leader {
            let own_quorum = own_quorum(metrics);
            let acked_age = metrics
                .last_quorum_acked
                .as_ref()
                .map(|acked| acked.elapsed());
            let since_lift = fence
                .lifted_at()
                .map(|lifted| now.saturating_duration_since(lifted));
            let confirmed = fence_lets_lead(own_quorum, acked_age, since_lift)
                && lease_holds(own_quorum, acked_age, lease);
            if confirmed || !fence_rules_leadership() {
                LeaderRole::Leads
            } else {
                LeaderRole::Lapsed
            }
        } else {
            match metrics.current_leader {
                Some(leader_id) if leader_id != metrics.id => LeaderRole::Follows { leader_id },
                _ => LeaderRole::Unknown,
            }
        };
        Self {
            node_id: metrics.id,
            term: metrics.current_term,
            role,
        }
    }

    /// The view of the replica that runs `raft`, behind `fence`, with its
    /// group's lease taken from the Raft's own config.
    pub(crate) fn of_raft<SM: openraft::storage::RaftStateMachine<UrsulaRaftTypeConfig>>(
        raft: &openraft::Raft<UrsulaRaftTypeConfig, SM>,
        fence: &NodeFence,
    ) -> Self {
        Self::of(
            &raft.metrics().borrow_watched(),
            fence,
            leader_lease(raft.config()),
        )
    }

    /// Where this view sends a write: here when this replica leads (with
    /// no early answer yet, and the fence of the write's cold upload), to
    /// the leader it knows, or nowhere.
    pub fn write_route(self) -> WriteRoute {
        match self.role {
            LeaderRole::Leads => WriteRoute::Leader {
                early_answer: None,
                fence: UploadFence::Leader(self.committed_leader()),
            },
            LeaderRole::Follows { leader_id } => WriteRoute::Follower {
                leader_id,
                term: self.term,
            },
            LeaderRole::Unknown | LeaderRole::Lapsed => WriteRoute::NoLeader,
        }
    }

    /// Why this view refuses what only a leader may do: a lapsed leader
    /// could not confirm its leadership with a quorum, and any other replica
    /// does not lead.
    pub fn refusal_reason(&self) -> LeadershipReason {
        match self.role {
            LeaderRole::Lapsed => LeadershipReason::QuorumUnconfirmed,
            LeaderRole::Leads | LeaderRole::Follows { .. } | LeaderRole::Unknown => {
                LeadershipReason::Follower
            }
        }
    }

    /// This replica leads the group.
    pub fn leads(&self) -> bool {
        matches!(self.role, LeaderRole::Leads)
    }

    /// The fence of a cold upload this replica plans now (F14j): its
    /// committed leader id, `(term, node)`, while it leads. A leader's vote
    /// is committed for its own term and node, which is the leader id
    /// OpenRaft checks `with_leader` against. A view that lags OpenRaft's
    /// names a leadership that already ended, so a lag only ever gets the
    /// upload refused, never appended under another leadership.
    pub fn upload_fence(&self) -> Option<UploadFence> {
        self.leads()
            .then(|| UploadFence::Leader(self.committed_leader()))
    }

    /// This replica as the committed leader of this view's term.
    fn committed_leader(&self) -> CommittedLeader {
        CommittedLeader::new(self.term, self.node_id)
    }

    /// The term in which this view knows `leader` leads, if it names it.
    pub fn term_of(&self, leader: u64) -> Option<u64> {
        match self.role {
            LeaderRole::Follows { leader_id, .. } if leader_id == leader => Some(self.term),
            LeaderRole::Leads if self.node_id == leader => Some(self.term),
            _ => None,
        }
    }
}

/// OpenRaft's leader lease of a group that runs `config`: OpenRaft takes it
/// from `election_timeout_max`, and refuses a write while no quorum
/// acknowledged its leader within it. Its followers campaign within the same
/// bound once the leader goes silent.
pub(crate) fn leader_lease(config: &openraft::Config) -> Duration {
    Duration::from_millis(config.election_timeout_max)
}

/// Whether a leader keeps its lease: it is its own quorum, or a quorum
/// acknowledged it less than `lease` ago (`acked_age`, the age of OpenRaft's
/// `last_quorum_acked`). A leader no follower answered yet (`acked_age` is
/// `None`) was just elected and keeps it: OpenRaft refuses its writes until
/// the first answer, and a write waits for that answer before it proposes
/// ([`crate::forward::await_first_quorum_ack`]). Unlike OpenRaft, which
/// counts such a lease as ended, this keeps a leader cut off before its
/// first answer leading: its leader-gated jobs keep running, and a write
/// there is refused by OpenRaft (`LeaseExpired`) once its wait for that
/// answer ends, `election_timeout_min` later.
fn lease_holds(own_quorum: bool, acked_age: Option<Duration>, lease: Duration) -> bool {
    own_quorum || acked_age.is_none_or(|age| age < lease)
}

/// Resolves once this replica's leadership, as `metrics` show it behind
/// `fence` for a group whose lease is `lease`, has lapsed
/// ([`LeaderRole::Lapsed`]). A write OpenRaft took while this replica led
/// waits on it beside its answer: a lapsed leader cut off from its peers gets
/// no answer for it, and the write may still commit through its quorum or a
/// newer leader, so its outcome is unknown. Any other role keeps the wait:
/// a replica that stepped down hears of the write's fate from the newer
/// leader.
///
/// A lease ends by time, not by a metrics change, so the wait looks again
/// when the lease of the last quorum acknowledgement would end, which a
/// later acknowledgement moves on: a write that commits within its lease
/// never wakes it. A leader that is its own quorum, or that no follower
/// answered yet, has no such end, and looks again one full lease later. No
/// look comes sooner than [`MIN_LOOK`], so the wait never spins on a lease
/// that ends as it looks. A fence that closes while a quorum still
/// acknowledges is seen by the next look, at most one lease later.
pub(crate) async fn until_lapsed(
    metrics: WatchReceiverOf<UrsulaRaftTypeConfig, RaftMetrics<UrsulaRaftTypeConfig>>,
    fence: &NodeFence,
    lease: Duration,
) {
    loop {
        let look_again = {
            let current = metrics.borrow_watched();
            if GroupLeadership::of(&current, fence, lease).role == LeaderRole::Lapsed {
                return;
            }
            match current.last_quorum_acked.as_ref() {
                Some(acked) if !own_quorum(&current) => lease.saturating_sub(acked.elapsed()),
                _ => lease,
            }
        };
        crate::rt::time::sleep(look_again.max(MIN_LOOK)).await;
    }
}

/// The shortest wait between two looks of [`until_lapsed`].
const MIN_LOOK: Duration = Duration::from_millis(1);

/// Whether the replica whose metrics are `metrics` is the only voter of its
/// effective membership, joint configs included, and so its own quorum.
/// OpenRaft's leader then has no follower to acknowledge it: its
/// `last_quorum_acked` stays at the time it was elected, and only
/// `is_self_quorum` tells it leads.
pub(crate) fn own_quorum(metrics: &RaftMetrics<UrsulaRaftTypeConfig>) -> bool {
    metrics
        .membership_config
        .membership()
        .voter_ids()
        .all(|voter| voter == metrics.id)
}

/// Whether the node fence and the leader lease take part in the leader
/// predicate, read only where they decide the role. A negative control
/// turns them off (`Defence::FencedLeadership`), and the predicate then
/// reads OpenRaft's server state alone.
fn fence_rules_leadership() -> bool {
    use ursula_stream::sim_controls::Defence;
    if ursula_stream::sim_controls::off(Defence::FencedLeadership) {
        ursula_stream::sim_controls::note_fired(Defence::FencedLeadership);
        return false;
    }
    true
}

/// Whether a leader behind an admitting fence leads: the fence never
/// lifted (`since_lift` is `None`), or OpenRaft confirmed this leadership
/// after the lift as a ReadIndex read does, by a quorum acknowledgement of
/// an RPC sent after it (`acked_age` is the age of `last_quorum_acked`, that
/// RPC's send time), or at once as its own quorum. A leader deposed while it
/// was fenced acts on no frozen state before it learns it.
fn fence_lets_lead(
    own_quorum: bool,
    acked_age: Option<Duration>,
    since_lift: Option<Duration>,
) -> bool {
    let Some(since_lift) = since_lift else {
        return true;
    };
    own_quorum || acked_age.is_some_and(|age| age < since_lift)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use openraft::RaftMetrics;
    use openraft::ServerState;
    use ursula_runtime::NodeFence;
    use ursula_runtime::node_fence::ProbeOutcome;
    use ursula_runtime::node_fence::fence_now;

    use super::GroupLeadership;
    use super::LeaderRole;
    use super::fence_lets_lead;
    use super::lease_holds;
    use crate::types::UrsulaRaftTypeConfig as C;

    const LEASE: Duration = Duration::from_secs(3);

    /// The one leader predicate behind the fence: a fenced leader leads
    /// nothing, and a leader whose fence never lifted leads.
    #[test]
    fn a_fenced_leader_leads_nothing() {
        let fence = NodeFence::leased(Duration::from_secs(5));
        let mut metrics = RaftMetrics::<C>::new_initial(1);
        metrics.state = ServerState::Leader;
        metrics.current_leader = Some(1);
        let fenced = GroupLeadership::of(&metrics, &fence, LEASE);
        assert_eq!(fenced.role, LeaderRole::Lapsed);
        assert_eq!(fenced.upload_fence(), None);
        let start = fence_now();
        fence.record_probe(start, ProbeOutcome::Synced, start);
        assert!(GroupLeadership::of(&metrics, &fence, LEASE).leads());
    }

    /// A leader leads while a quorum acknowledged it within its lease, or
    /// while no follower answered it yet, and lapses once the last
    /// acknowledgement is older than the lease. Its own quorum never lapses.
    #[test]
    fn a_leader_lapses_once_its_last_quorum_acknowledgement_is_older_than_its_lease() {
        let secs = Duration::from_secs;
        assert!(lease_holds(false, None, LEASE), "just elected");
        assert!(lease_holds(false, Some(secs(1)), LEASE), "acknowledged");
        assert!(!lease_holds(false, Some(LEASE), LEASE), "at the lease");
        assert!(!lease_holds(false, Some(secs(60)), LEASE), "cut off");
        assert!(lease_holds(true, Some(secs(60)), LEASE), "its own quorum");
    }

    /// After a lift a leader leads again only once a quorum acknowledged an
    /// RPC it sent after the lift, or at once as its own quorum.
    #[test]
    fn a_lifted_leader_leads_only_once_confirmed_after_the_lift() {
        let secs = Duration::from_secs;
        assert!(fence_lets_lead(false, None, None), "never lifted");
        assert!(
            !fence_lets_lead(false, None, Some(secs(1))),
            "never acknowledged"
        );
        assert!(
            !fence_lets_lead(false, Some(secs(3)), Some(secs(1))),
            "acknowledged before the lift"
        );
        assert!(
            !fence_lets_lead(false, Some(secs(1)), Some(secs(1))),
            "acknowledged at the lift"
        );
        assert!(
            fence_lets_lead(false, Some(secs(1)), Some(secs(2))),
            "acknowledged after the lift"
        );
        assert!(fence_lets_lead(true, None, Some(secs(1))), "its own quorum");
    }
}
