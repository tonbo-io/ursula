//! The membership asker (design 5.8, S10 of the design): how a held
//! replica that may have left its group learns it.
//!
//! A voter a membership step dropped keeps a membership that lists it,
//! since the leader stops replicating to it, so only its group can tell it.
//! The hosting driver therefore asks, for each held replica that is closed,
//! or whose bootstrapped view gives its node the role `Outgoing`,
//! `Abandoned` or none (none on a node that knows itself removed,
//! [`ursula_control::Standing::placed_role`]), the group's leader for a
//! fresh quorum barrier ([`MetaStatusReader::barrier`]), along the group's
//! leader candidates ([`ursula_control::leader_candidates`]). It asks after
//! each change of the replica's vote, and again each [`ASK_INTERVAL`]
//! unless an append with a vote of at least its last answer's arrived since
//! the previous check. Each check after the interval, asked or skipped,
//! moves that baseline to now, so a replica its leader stops replicating to
//! asks again within one interval: a voter dropped after a `Voter` answer,
//! whose vote no membership change moves, learns it too. Each ask is one
//! driver action under the per-core budget, begun after the pass's opens
//! and closes.
//!
//! Only a leader that just proved its quorum answers, so a group without
//! one answers nothing and its replicas keep counting. Every answer is
//! classified for this node by the kernel's one classifier
//! ([`crate::LeaderMemberships::answer`]) and reaches the group's left mark
//! ([`crate::LeftMarks`]), as the gate driver's answers do. The latest
//! answer per group goes into the driver's status ([`GroupAnswer`]), and a
//! teardown's first step takes the first answer that came after it started
//! (`hosting::teardown`). Its second step asks for itself, through [`ask`].

use std::collections::BTreeMap;
use std::time::Duration;

use ursula_control::Asked;
use ursula_proto::placement::MembershipAnswer;
use ursula_proto::placement::NodeId;
use ursula_shard::RaftGroupId;
use ursula_shard::RaftTarget;

use crate::LeftMarks;
use crate::meta::status::MetaStatusReader;
use crate::rt::time::Instant;
use crate::types::UrsulaVote;

/// How often a replica that may have left its group asks again, unless an
/// append with a vote of at least its last answer's arrived since the
/// previous check.
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

/// The driver's last check of one held replica: when it last asked, or
/// last found an append following the last answer.
#[derive(Debug, Clone, Copy)]
struct Checked {
    at: Instant,
    /// The replica's vote then, `None` while it was closed.
    vote: Option<UrsulaVote>,
    /// The group's appends then, in the left mark's count.
    appends: Asked,
}

/// What the driver knows of one held replica's questions.
#[derive(Debug, Default)]
struct Asking {
    checked: Option<Checked>,
    /// The latest answer a live leader gave.
    last: Option<GroupAnswer>,
    /// How many answers live leaders gave, so a teardown takes only one
    /// that came after it started.
    answers: u64,
}

/// The questions of every held replica, by group.
#[derive(Debug, Default)]
pub(super) struct Askers(BTreeMap<RaftGroupId, Asking>);

impl Askers {
    /// Whether the replica of `group`, whose vote is `vote` (`None` while
    /// closed), asks now: it was never checked, its vote changed since the
    /// last check, or that check was [`ASK_INTERVAL`] ago and no append
    /// with a vote of at least its last answer's arrived since. When one
    /// did, the check moves to now and the replica does not ask, so the
    /// next interval looks only at the appends after it.
    pub(super) fn due(
        &mut self,
        group: RaftGroupId,
        vote: Option<UrsulaVote>,
        marks: &LeftMarks,
    ) -> bool {
        let Some(asking) = self.0.get_mut(&group) else {
            return true;
        };
        let Some(checked) = asking.checked else {
            return true;
        };
        if checked.vote != vote {
            return true;
        }
        if checked.at.elapsed() < ASK_INTERVAL {
            return false;
        }
        let followed = asking
            .last
            .is_some_and(|last| marks.followed(group, checked.appends, &last.vote));
        if followed {
            asking.checked = Some(Checked::now(group, vote, marks));
        }
        !followed
    }

    /// Records that the replica of `group`, at vote `vote`, asks now.
    pub(super) fn started(
        &mut self,
        group: RaftGroupId,
        vote: Option<UrsulaVote>,
        marks: &LeftMarks,
    ) {
        self.0.entry(group).or_default().checked = Some(Checked::now(group, vote, marks));
    }

    /// Records how the ask of `group` ended: a live leader's answer, or
    /// none, which keeps the last answer.
    pub(super) fn ended(&mut self, group: RaftGroupId, answer: Option<GroupAnswer>) {
        if let Some(answer) = answer {
            let asking = self.0.entry(group).or_default();
            asking.last = Some(answer);
            asking.answers = asking.answers.saturating_add(1);
        }
    }

    /// Makes the replica of `group` ask at its next check, as a replica
    /// never checked does: a teardown starts.
    pub(super) fn rearm(&mut self, group: RaftGroupId) {
        if let Some(asking) = self.0.get_mut(&group) {
            asking.checked = None;
        }
    }

    /// How many answers about `group` live leaders gave.
    pub(super) fn answer_count(&self, group: RaftGroupId) -> u64 {
        self.0.get(&group).map_or(0, |asking| asking.answers)
    }

    /// The latest answer about `group`, if it came after the first `count`.
    pub(super) fn answer_since(&self, group: RaftGroupId, count: u64) -> Option<GroupAnswer> {
        let asking = self.0.get(&group)?;
        if asking.answers > count {
            asking.last
        } else {
            None
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
            .filter_map(|(group, asking)| Some((*group, asking.last?)))
            .collect()
    }
}

impl Checked {
    /// A check of `group`'s replica, at vote `vote`, now.
    fn now(group: RaftGroupId, vote: Option<UrsulaVote>, marks: &LeftMarks) -> Self {
        Self {
            at: Instant::now(),
            vote,
            appends: marks.ask(group),
        }
    }
}

/// Asks `candidates` in order for a fresh quorum barrier of `group`, until
/// one answers as a live leader, and records the answer, classified for
/// `me`, in the group's left mark. `None` when no candidate answered as a
/// live leader.
pub(super) async fn ask(
    meta: MetaStatusReader,
    marks: LeftMarks,
    me: NodeId,
    group: RaftGroupId,
    candidates: Vec<NodeId>,
) -> Option<GroupAnswer> {
    for leader in candidates {
        let asked = marks.ask(group);
        let Some(answer) = live_barrier(&meta, group, leader).await else {
            continue;
        };
        match answer.memberships.answer(me) {
            Ok(classified) => {
                marks.answered(group, asked, classified, answer.vote);
                return Some(GroupAnswer {
                    leader,
                    vote: answer.vote,
                    answer: classified,
                });
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
    None
}

/// `leader`'s answer to a fresh quorum barrier of `group`, asked over the
/// one barrier seam ([`MetaStatusReader::barrier`]) within
/// [`crate::rejoin::RECOVERY_BARRIER_TIMEOUT`]: `None` when it answered
/// nothing as a live leader. This asker, the teardown's asks and the
/// removed-node sweep (`meta::sweep`) ask through it. The rejoin barrier
/// loop (`rejoin::barrier`) still asks a group's memberships on its own.
pub(crate) async fn live_barrier(
    meta: &MetaStatusReader,
    group: RaftGroupId,
    leader: NodeId,
) -> Option<crate::BarrierAnswer> {
    let barrier = meta.barrier(leader, RaftTarget::Data(group));
    match crate::rt::time::timeout(crate::rejoin::RECOVERY_BARRIER_TIMEOUT, barrier).await {
        Ok(Ok(answer)) => Some(answer),
        Ok(Err(failure)) => {
            tracing::debug!(
                group = group.0,
                leader = leader.get(),
                %failure,
                "membership ask: no barrier from this candidate"
            );
            None
        }
        Err(_elapsed) => {
            tracing::debug!(
                group = group.0,
                leader = leader.get(),
                "membership ask: the candidate's barrier timed out"
            );
            None
        }
    }
}
