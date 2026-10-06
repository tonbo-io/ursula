//! Applied data-group configuration observed after a fresh quorum read.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;
use ursula_shard::RaftGroupId;

use crate::MembershipLogId;
use crate::VerifiedGroupMembership;

/// Includes both constituent voter sets during joint consensus. A configuration
/// observation never makes a joint configuration eligible for placement publish.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedGroupConfiguration {
    pub raft_group_id: RaftGroupId,
    pub leader_id: u64,
    pub leader_term: u64,
    pub applied_log_id: MembershipLogId,
    pub membership_log_id: MembershipLogId,
    pub voter_sets: Vec<BTreeSet<u64>>,
    pub learners: BTreeSet<u64>,
    pub nodes: BTreeMap<u64, String>,
}

impl CommittedGroupConfiguration {
    pub fn validate(&self) -> Result<(), String> {
        let voters: BTreeSet<_> = self.voter_sets.iter().flatten().copied().collect();
        if self.voter_sets.is_empty()
            || self.voter_sets.len() > 2
            || self.voter_sets.iter().any(BTreeSet::is_empty)
            || voters.contains(&0)
            || self.learners.contains(&0)
            || !voters.contains(&self.leader_id)
            || !voters.is_disjoint(&self.learners)
            || self.membership_log_id.node_id == 0
            || self.applied_log_id.node_id == 0
            || self.leader_term < self.applied_log_id.term
            || self.applied_log_id.index < self.membership_log_id.index
            || self.applied_log_id.term < self.membership_log_id.term
            || (self.applied_log_id.index == self.membership_log_id.index
                && self.applied_log_id != self.membership_log_id)
            || self.nodes.keys().copied().collect::<BTreeSet<_>>()
                != voters.union(&self.learners).copied().collect()
            || self.nodes.values().any(|origin| origin.is_empty())
        {
            return Err("inconsistent committed data configuration observation".to_owned());
        }
        Ok(())
    }

    /// Only a uniform configuration can be converted to the certificate used
    /// by placement publication. Learners are retained for caller validation.
    pub fn uniform_membership(&self) -> Result<VerifiedGroupMembership, String> {
        self.validate()?;
        let [voters] = self.voter_sets.as_slice() else {
            return Err("joint configuration is not a uniform membership certificate".to_owned());
        };
        Ok(VerifiedGroupMembership {
            voters: voters.clone(),
            learners: self.learners.clone(),
            log_id: self.membership_log_id.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use ursula_shard::RaftGroupId;

    use super::CommittedGroupConfiguration;
    use crate::MembershipLogId;

    fn configuration() -> CommittedGroupConfiguration {
        CommittedGroupConfiguration {
            raft_group_id: RaftGroupId(0),
            leader_id: 1,
            leader_term: 2,
            membership_log_id: MembershipLogId {
                term: 1,
                node_id: 1,
                index: 7,
            },
            applied_log_id: MembershipLogId {
                term: 2,
                node_id: 1,
                index: 8,
            },
            voter_sets: vec![[1, 2, 3].into()],
            learners: [4].into(),
            nodes: (1..=4)
                .map(|id| (id, format!("http://node{id}:4440")))
                .collect::<BTreeMap<_, _>>(),
        }
    }
    #[test]
    fn uniform_configuration_retains_exact_membership_and_learner_identity() {
        let observed = configuration();
        let membership = observed.uniform_membership().unwrap();
        assert_eq!(membership.voters, [1, 2, 3].into());
        assert_eq!(membership.learners, [4].into());
        assert_eq!(membership.log_id, observed.membership_log_id);
    }
    #[test]
    fn joint_configuration_preserves_constituent_quorums_and_cannot_publish_as_uniform() {
        let mut observed = configuration();
        observed.voter_sets.push([1, 3, 4].into());
        observed.learners.clear();
        assert!(observed.validate().is_ok());
        assert!(observed.uniform_membership().is_err());
        assert_eq!(observed.voter_sets.len(), 2);
        assert_eq!(observed.voter_sets[0], [1, 2, 3].into());
        assert_eq!(observed.voter_sets[1], [1, 3, 4].into());
    }
    #[test]
    fn configuration_rejects_conflicting_prefixes_unknown_members_and_invalid_quorum_shapes() {
        let observed = configuration();
        for field in [
            "prefix",
            "term",
            "leader",
            "missing-node",
            "learner",
            "empty",
            "many",
        ] {
            let mut wrong = observed.clone();
            match field {
                "prefix" => {
                    wrong.applied_log_id.index = 7;
                    wrong.applied_log_id.node_id = 2;
                }
                "term" => wrong.applied_log_id.term = 0,
                "leader" => wrong.leader_id = 4,
                "missing-node" => {
                    wrong.nodes.remove(&3);
                }
                "learner" => {
                    wrong.learners.insert(2);
                }
                "empty" => wrong.voter_sets.clear(),
                "many" => {
                    wrong.voter_sets.push([1, 3, 4].into());
                    wrong.voter_sets.push([1, 2, 4].into());
                }
                _ => unreachable!(),
            }
            assert!(wrong.validate().is_err(), "{field}");
        }
    }
}
