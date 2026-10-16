//! One replica's view of its group's leadership, taken from one borrow of
//! its Raft metrics. The front door routes writes by it and the engine
//! routes writes and names the leader's term in its forward hints by it,
//! so both decide by one policy.

use openraft::RaftMetrics;
use openraft::ServerState;
use ursula_runtime::WriteRoute;

use crate::types::UrsulaRaftTypeConfig;

/// This replica's role in its group, as its Raft metrics show it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaderRole {
    /// This replica is the leader (`ServerState::Leader`).
    Leads,
    /// Another node leads the group.
    Follows {
        leader_id: u64,
        /// The leader's address in the group's membership.
        leader_address: Option<String>,
    },
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
                Some(leader_id) if leader_id != metrics.id => LeaderRole::Follows {
                    leader_id,
                    leader_address: metrics
                        .membership_config
                        .membership()
                        .get_node(&leader_id)
                        .map(|node| node.addr.clone()),
                },
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
    /// no early answer yet), to the leader it knows, or nowhere.
    pub fn write_route(self) -> WriteRoute {
        match self.role {
            LeaderRole::Leads => WriteRoute::Leader { early_answer: None },
            LeaderRole::Follows {
                leader_id,
                leader_address,
            } => WriteRoute::Follower {
                leader_id,
                leader_address,
                term: self.term,
            },
            LeaderRole::Unknown => WriteRoute::NoLeader,
        }
    }

    /// This replica leads the group.
    pub fn leads(&self) -> bool {
        matches!(self.role, LeaderRole::Leads)
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
