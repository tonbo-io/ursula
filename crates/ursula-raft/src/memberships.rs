//! What a live leader answers a recovery barrier with: its committed vote,
//! the index a replica must apply to, and the committed and effective
//! memberships of the leader view the barrier confirmed.
//!
//! The leader sends both memberships whole, never a verdict about the node
//! that asked, so no field of the request can disagree with the sender's
//! admitted identity, and every asker classifies them with the kernel's
//! one classifier, [`ursula_control::membership_answer`]: a replica about
//! its own node, the removed-node sweep about a removed one.

use std::collections::BTreeSet;

use openraft::type_config::alias::StoredMembershipOf;
use serde::Deserialize;
use serde::Serialize;
use ursula_control::MembershipConfig;
use ursula_control::membership_answer;
use ursula_proto::placement::MembershipAnswer;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::VoterSet;

use crate::types::UrsulaRaftTypeConfig;
use crate::types::UrsulaVote;

type Membership = StoredMembershipOf<UrsulaRaftTypeConfig>;

/// The committed and the effective membership of a leader view, as a
/// barrier answer carries them (`RejoinBarrierResponseV1.memberships`,
/// MessagePack, required). Format epoch 6.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaderMemberships {
    pub committed: Membership,
    pub effective: Membership,
}

/// Which membership of a [`LeaderMemberships`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MembershipKind {
    Committed,
    Effective,
}

impl std::fmt::Display for MembershipKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Committed => "committed",
            Self::Effective => "effective",
        })
    }
}

/// A barrier answer whose memberships no live leader holds. It is refused
/// as malformed, never read as an answer that the node is no member.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MalformedMembership {
    /// A membership without a voter config, or with an empty one.
    #[error("the leader's {membership} membership has no voters in a config")]
    EmptyVoterConfig { membership: MembershipKind },
    /// A membership of more than two voter configs: a config is uniform or
    /// joint.
    #[error("the leader's {membership} membership has {configs} voter configs, not one or two")]
    ConfigCount {
        membership: MembershipKind,
        configs: usize,
    },
}

impl LeaderMemberships {
    /// The memberships of the leader view `metrics` shows.
    pub(crate) fn of(metrics: &openraft::RaftMetrics<UrsulaRaftTypeConfig>) -> Self {
        Self {
            committed: Membership::clone(&metrics.committed_membership_config),
            effective: Membership::clone(&metrics.membership_config),
        }
    }

    /// The committed and the effective membership as the kernel's configs,
    /// refusing memberships no live leader holds.
    pub fn configs(&self) -> Result<(MembershipConfig, MembershipConfig), MalformedMembership> {
        Ok((
            membership_config(&self.committed, MembershipKind::Committed)?,
            membership_config(&self.effective, MembershipKind::Effective)?,
        ))
    }

    /// Refuses memberships no live leader holds.
    pub fn check(&self) -> Result<(), MalformedMembership> {
        self.configs().map(|_configs| ())
    }

    /// The leader's answer about `node`, by the kernel's one classifier
    /// ([`membership_answer`]).
    pub fn answer(&self, node: NodeId) -> Result<MembershipAnswer, MalformedMembership> {
        let (committed, effective) = self.configs()?;
        Ok(membership_answer(node, &committed, &effective))
    }
}

/// `membership` as the kernel's config: one voter config is uniform, two are
/// joint, and every listed node outside them is a learner.
pub(crate) fn membership_config(
    membership: &Membership,
    kind: MembershipKind,
) -> Result<MembershipConfig, MalformedMembership> {
    let voter_set = |config: &BTreeSet<u64>| {
        VoterSet::new(config.iter().copied().map(NodeId::new).collect())
            .map_err(|_empty| MalformedMembership::EmptyVoterConfig { membership: kind })
    };
    let configs = membership.get_joint_config();
    let voters: BTreeSet<u64> = configs.iter().flatten().copied().collect();
    let learners: BTreeSet<NodeId> = membership
        .nodes()
        .map(|(node, _)| *node)
        .filter(|node| !voters.contains(node))
        .map(NodeId::new)
        .collect();
    match configs.as_slice() {
        [] => Err(MalformedMembership::EmptyVoterConfig { membership: kind }),
        [voters] => Ok(MembershipConfig::Uniform {
            voters: voter_set(voters)?,
            learners,
        }),
        [old, new] => Ok(MembershipConfig::Joint {
            old: voter_set(old)?,
            new: voter_set(new)?,
            learners,
        }),
        _ => Err(MalformedMembership::ConfigCount {
            membership: kind,
            configs: configs.len(),
        }),
    }
}

/// A leader's answer to a recovery barrier: its committed vote, the index
/// the asking replica must apply to, and its memberships.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarrierAnswer {
    pub vote: UrsulaVote,
    pub index: u64,
    pub memberships: LeaderMemberships,
}

#[cfg(test)]
mod tests {
    use openraft::type_config::alias::LogIdOf;
    use openraft::type_config::alias::StoredMembershipOf;
    use openraft::vote::RaftLeaderId;
    use ursula_proto::placement::MembershipAnswer;
    use ursula_proto::placement::NodeId;

    use super::LeaderMemberships;
    use super::MalformedMembership;
    use super::MembershipKind;
    use crate::types::UrsulaRaftTypeConfig;

    type LeaderId = <UrsulaRaftTypeConfig as openraft::RaftTypeConfig>::LeaderId;

    fn membership(
        index: u64,
        configs: Vec<Vec<u64>>,
        learners: &[u64],
    ) -> StoredMembershipOf<UrsulaRaftTypeConfig> {
        let log_id: LogIdOf<UrsulaRaftTypeConfig> = openraft::LogId {
            leader_id: LeaderId::new(3, 1),
            index,
        };
        StoredMembershipOf::<UrsulaRaftTypeConfig>::new(
            Some(log_id),
            openraft::Membership::new_with_defaults(
                configs
                    .into_iter()
                    .map(|config| config.into_iter().collect())
                    .collect(),
                learners.iter().copied(),
            ),
        )
    }

    /// The outgoing voters of a committed joint config whose uniform entry
    /// is appended are still voters: a replica that asks then is not torn
    /// down.
    #[test]
    fn the_outgoing_voters_of_a_committed_joint_config_are_voters() {
        let memberships = LeaderMemberships {
            committed: membership(10, vec![vec![1, 2, 3], vec![2, 3, 4]], &[]),
            effective: membership(11, vec![vec![2, 3, 4]], &[]),
        };
        memberships.check().expect("a live leader's memberships");
        assert_eq!(
            memberships.answer(NodeId::new(1)),
            Ok(MembershipAnswer::Voter)
        );
        assert_eq!(
            memberships.answer(NodeId::new(4)),
            Ok(MembershipAnswer::Voter)
        );
        assert_eq!(
            memberships.answer(NodeId::new(5)),
            Ok(MembershipAnswer::NotMember)
        );
        let settled = LeaderMemberships {
            committed: membership(11, vec![vec![2, 3, 4]], &[1]),
            effective: membership(11, vec![vec![2, 3, 4]], &[1]),
        };
        assert_eq!(
            settled.answer(NodeId::new(1)),
            Ok(MembershipAnswer::Learner)
        );
    }

    /// A membership without voters is malformed, never an answer that a
    /// node is no member.
    #[test]
    fn memberships_without_voters_are_refused() {
        let live = membership(10, vec![vec![1, 2, 3]], &[]);
        for (committed, effective, kind) in [
            (
                membership(10, Vec::new(), &[]),
                live.clone(),
                MembershipKind::Committed,
            ),
            (
                live.clone(),
                membership(10, vec![vec![1], Vec::new()], &[]),
                MembershipKind::Effective,
            ),
        ] {
            assert_eq!(
                LeaderMemberships {
                    committed,
                    effective
                }
                .check(),
                Err(MalformedMembership::EmptyVoterConfig { membership: kind })
            );
        }
    }

    /// A config is uniform or joint: a membership of three voter configs is
    /// malformed too.
    #[test]
    fn memberships_of_more_than_two_configs_are_refused() {
        let memberships = LeaderMemberships {
            committed: membership(10, vec![vec![1, 2, 3]], &[]),
            effective: membership(11, vec![vec![1], vec![2], vec![3]], &[]),
        };
        assert_eq!(
            memberships.answer(NodeId::new(1)),
            Err(MalformedMembership::ConfigCount {
                membership: MembershipKind::Effective,
                configs: 3,
            })
        );
    }
}
