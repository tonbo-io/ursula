//! The membership asker (design 5.8, S10 of the design): how a held
//! replica that may have left its group learns it.
//!
//! A voter a membership step dropped keeps a membership that lists it,
//! since the leader stops replicating to it, so only its group can tell it.
//! The hosting driver therefore asks, for each held replica that is closed,
//! or whose bootstrapped view gives its node the role `Outgoing`,
//! `Abandoned` or none, the group's leader for a fresh quorum barrier
//! ([`MetaStatusReader::barrier`]), along the group's leader candidates
//! ([`ursula_control::leader_candidates`]). It asks after each change of the
//! replica's vote, and every [`ASK_INTERVAL`] until an append with a vote of
//! at least its last answer's follows that answer. Each ask is one driver
//! action under the per-core budget, begun after the pass's opens and
//! closes.
//!
//! Only a leader that just proved its quorum answers, so a group without
//! one answers nothing and its replicas keep counting. Every answer is
//! classified for this node by the kernel's one classifier
//! ([`crate::LeaderMemberships::answer`]) and reaches the group's left mark
//! ([`crate::LeftMarks`]), as the gate driver's answers do. The latest
//! answer per group goes into the driver's status ([`GroupAnswer`]).

use std::collections::BTreeMap;
use std::time::Duration;

use ursula_control::Asked;
use ursula_proto::placement::MembershipAnswer;
use ursula_proto::placement::NodeId;
use ursula_shard::RaftGroupId;
use ursula_shard::RaftTarget;

use super::ActionEnd;
use crate::LeftMarks;
use crate::meta::status::MetaStatusReader;
use crate::rt::time::Instant;
use crate::types::UrsulaVote;

/// How often a replica that may have left its group asks again while no
/// append with a vote of at least its last answer's followed that answer.
pub const ASK_INTERVAL: Duration = Duration::from_secs(30);

/// A live leader's latest answer about this node's replica of a group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupAnswer {
    /// The leader that answered.
    pub leader: NodeId,
    /// Its committed vote.
    pub vote: UrsulaVote,
    /// What its memberships make this node, by the kernel's one classifier.
    pub answer: MembershipAnswer,
}

/// An answer, with when its question went out in the group's appends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Answered {
    answer: GroupAnswer,
    asked: Asked,
}

/// What the driver knows of one held replica's questions.
#[derive(Debug, Default)]
struct Asking {
    /// When the last question went out, and the replica's vote then,
    /// `None` while it was closed.
    asked: Option<(Instant, Option<UrsulaVote>)>,
    /// The latest answer a live leader gave.
    last: Option<Answered>,
}

/// The questions of every held replica, by group.
#[derive(Debug, Default)]
pub(super) struct Askers(BTreeMap<RaftGroupId, Asking>);

impl Askers {
    /// Whether the replica of `group`, whose vote is `vote` (`None` while
    /// closed), asks now: it never asked, its vote changed since it last
    /// asked, or it asked [`ASK_INTERVAL`] ago and no append with a vote of
    /// at least its last answer's followed that answer.
    pub(super) fn due(
        &self,
        group: RaftGroupId,
        vote: Option<UrsulaVote>,
        marks: &LeftMarks,
    ) -> bool {
        let Some((at, asked_vote)) = self.0.get(&group).and_then(|asking| asking.asked) else {
            return true;
        };
        if asked_vote != vote {
            return true;
        }
        if at.elapsed() < ASK_INTERVAL {
            return false;
        }
        !self
            .0
            .get(&group)
            .and_then(|asking| asking.last)
            .is_some_and(|last| marks.followed(group, last.asked, &last.answer.vote))
    }

    /// Records that the replica of `group`, at vote `vote`, asks now.
    pub(super) fn started(&mut self, group: RaftGroupId, vote: Option<UrsulaVote>) {
        self.0.entry(group).or_default().asked = Some((Instant::now(), vote));
    }

    /// Records how the ask of `group` ended: a live leader's answer, or
    /// none, which keeps the last answer.
    pub(super) fn ended(&mut self, group: RaftGroupId, answered: Option<Answered>) {
        if let Some(answered) = answered {
            self.0.entry(group).or_default().last = Some(answered);
        }
    }

    /// Forgets every group whose replica the node no longer holds.
    pub(super) fn retain(&mut self, held: impl Fn(RaftGroupId) -> bool) {
        self.0.retain(|group, _| held(*group));
    }

    /// The latest answer of each group.
    pub(super) fn answers(&self) -> BTreeMap<RaftGroupId, GroupAnswer> {
        self.0
            .iter()
            .filter_map(|(group, asking)| Some((*group, asking.last?.answer)))
            .collect()
    }
}

/// Asks `candidates` in order for a fresh quorum barrier of `group`, until
/// one answers as a live leader, and records the answer, classified for
/// `me`, in the group's left mark.
pub(super) async fn ask(
    meta: MetaStatusReader,
    marks: LeftMarks,
    me: NodeId,
    group: RaftGroupId,
    candidates: Vec<NodeId>,
) -> ActionEnd {
    for leader in candidates {
        let asked = marks.ask(group);
        let barrier = meta.barrier(leader, RaftTarget::Data(group));
        let answer = match crate::rt::time::timeout(
            crate::rejoin::RECOVERY_BARRIER_TIMEOUT,
            barrier,
        )
        .await
        {
            Ok(Ok(answer)) => answer,
            Ok(Err(failure)) => {
                tracing::debug!(
                    group = group.0,
                    leader = leader.get(),
                    %failure,
                    "membership asker: no barrier from this candidate"
                );
                continue;
            }
            Err(_elapsed) => {
                tracing::debug!(
                    group = group.0,
                    leader = leader.get(),
                    "membership asker: the candidate's barrier timed out"
                );
                continue;
            }
        };
        match answer.memberships.answer(me) {
            Ok(classified) => {
                marks.answered(group, asked, classified, answer.vote);
                return ActionEnd::Asked(Some(Answered {
                    answer: GroupAnswer {
                        leader,
                        vote: answer.vote,
                        answer: classified,
                    },
                    asked,
                }));
            }
            // Checked on arrival, so this is no live leader's answer.
            Err(error) => tracing::debug!(
                group = group.0,
                leader = leader.get(),
                %error,
                "membership asker: the answer's memberships are malformed"
            ),
        }
    }
    ActionEnd::Asked(None)
}
