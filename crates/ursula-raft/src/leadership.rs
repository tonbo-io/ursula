//! One replica's view of its group's leadership, taken from one borrow of
//! its Raft metrics. The front door routes writes by it, the engine routes
//! writes and names the leader's term in its forward hints by it, and both
//! fence cold uploads by it (F14j), so all decide by one policy.

use openraft::RaftMetrics;
use openraft::ServerState;
use ursula_runtime::CommittedLeader;
use ursula_runtime::UploadFence;
use ursula_runtime::WriteRoute;

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
    /// The view `metrics` show. Only `ServerState::Leader` leads: a node
    /// that names itself leader in any other state is between roles.
    pub fn of(metrics: &RaftMetrics<UrsulaRaftTypeConfig>) -> Self {
        let role = if metrics.state == ServerState::Leader {
            LeaderRole::Leads
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
