//! One replica's view of its group's leadership, taken from one borrow of
//! its Raft metrics. The front door routes writes by it, the engine routes
//! writes and names the leader's term in its forward hints by it, and both
//! fence cold uploads by it (F14j), so all decide by one policy.
//!
//! The node fence takes part: OpenRaft keeps a leader whose disk lease
//! lapsed in its leader state, and the fence blocks every message that
//! could depose it, so a fenced replica leads nothing here, and every
//! leader-gated job stops at this one site.

use std::time::Duration;

use openraft::RaftMetrics;
use openraft::ServerState;
use openraft::rt::Instant as _;
use ursula_runtime::CommittedLeader;
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
    /// The view `metrics` show behind `fence`. Only `ServerState::Leader`
    /// leads: a node that names itself leader in any other state is between
    /// roles. While the fence does not admit, the role is unknown, and after
    /// it lifted a leader leads again only once OpenRaft confirmed its
    /// leadership ([`fence_lets_lead`]).
    pub fn of(metrics: &RaftMetrics<UrsulaRaftTypeConfig>, fence: &NodeFence) -> Self {
        let now = fence_now();
        let role = if !fence.admits(now) && fence_rules_leadership() {
            LeaderRole::Unknown
        } else if metrics.state == ServerState::Leader {
            let own_quorum = metrics
                .membership_config
                .membership()
                .voter_ids()
                .all(|voter| voter == metrics.id);
            let acked_age = metrics
                .last_quorum_acked
                .as_ref()
                .map(|acked| acked.elapsed());
            let since_lift = fence
                .lifted_at()
                .map(|lifted| now.saturating_duration_since(lifted));
            if fence_lets_lead(own_quorum, acked_age, since_lift) || !fence_rules_leadership() {
                LeaderRole::Leads
            } else {
                LeaderRole::Unknown
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
            LeaderRole::Unknown => WriteRoute::NoLeader,
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

/// Whether the node fence takes part in the leader predicate, read only
/// where it decides the role. A negative control turns it off
/// (`Defence::FencedLeadership`), and the predicate then reads OpenRaft's
/// server state alone.
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
    use crate::types::UrsulaRaftTypeConfig as C;

    /// The one leader predicate behind the fence: a fenced leader leads
    /// nothing, and a leader whose fence never lifted leads.
    #[test]
    fn a_fenced_leader_leads_nothing() {
        let fence = NodeFence::leased(Duration::from_secs(5));
        let mut metrics = RaftMetrics::<C>::new_initial(1);
        metrics.state = ServerState::Leader;
        metrics.current_leader = Some(1);
        let fenced = GroupLeadership::of(&metrics, &fence);
        assert_eq!(fenced.role, LeaderRole::Unknown);
        assert_eq!(fenced.upload_fence(), None);
        let start = fence_now();
        fence.record_probe(start, ProbeOutcome::Synced, start);
        assert!(GroupLeadership::of(&metrics, &fence).leads());
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
