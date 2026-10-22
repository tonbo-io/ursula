//! The one voter predicate on this node (S10 of the design): whether a data
//! replica counts as a voter, by the kernel's [`counts_as_voter`].
//!
//! A replica counts when a confirmed placement gives its node the role
//! `Voter` ([`crate::SeedRole::placed_role`]) or its node is a voter of a
//! config of its effective membership, and its group has not marked it
//! left. A closed replica counts unless it is marked left. So does an open
//! replica whose log held anything at open while its Raft has not reported
//! a membership yet: until that first report the metrics show none, also
//! when the log lists the node ([`crate::GroupRejoin::counts`]).
//!
//! An open replica that holds nothing and has no confirmed role counts for
//! nothing, so its closed gate holds back no participation. A server node's
//! wiped voter (#729) never runs in that state: the hosting driver creates
//! a replica it does not hold only from a confirmed view, whose role the
//! predicate then reads, and opens only what its journal holds from an
//! unconfirmed one. A replica that kept anything counts as an open replica
//! with a held log does, so its gate keeps the node out of handoffs until
//! it opens (design 5.8).
//!
//! The left mark of each group ([`LeftMarks`]) belongs to the node, not to
//! a replica's engine, so a replica closed and opened again keeps it for
//! the run. Two producers feed it:
//!
//! - every barrier answer a replica's gate driver gets, classified for this
//!   node by the kernel's one classifier ([`crate::LeaderMemberships::answer`]),
//!   may set it;
//! - every inbound append with a committed vote may clear it
//!   ([`crate::GroupRejoin::observe_inbound_append`]).
//!
//! Node participation ([`crate::RaftGroupHandleRegistry::recovery_barriers_ready`]),
//! the egress probe's groups and the hosting driver's plan read the
//! predicate. The meta replica's gate counts for none of them.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;

use ursula_control::LeftMark;
use ursula_control::counts_as_voter;
use ursula_shard::RaftGroupId;

use crate::types::UrsulaVote;

/// One group's left mark, shared by the node's replicas of the group.
pub(crate) type LeftMarkCell = Arc<Mutex<LeftMark<UrsulaVote>>>;

/// The left mark of every data group attached on this node in the run,
/// set or not, created when its replica binds ([`LeftMarks::cell`]).
/// Cloning shares it.
#[derive(Debug, Clone, Default)]
pub struct LeftMarks(Arc<Mutex<BTreeMap<RaftGroupId, LeftMarkCell>>>);

impl LeftMarks {
    /// The mark of `group`, which its replica holds while it runs, so an
    /// append takes only its group's lock.
    pub(crate) fn cell(&self, group: RaftGroupId) -> LeftMarkCell {
        Arc::clone(
            self.0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(group)
                .or_default(),
        )
    }

    /// Whether `group` marked this node left.
    pub fn is_left(&self, group: RaftGroupId) -> bool {
        let cell = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&group)
            .cloned();
        cell.is_some_and(|cell| is_left(&cell))
    }
}

/// Whether the mark in `cell` is set.
pub(crate) fn is_left(cell: &LeftMarkCell) -> bool {
    cell.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .is_left()
}

/// Whether a closed replica of `group` counts as a voter: unless the group
/// marked this node left.
pub(crate) fn closed_counts(marks: &LeftMarks, group: RaftGroupId) -> ursula_control::Counts {
    counts_as_voter(
        None,
        ursula_control::ReplicaMembership::Closed,
        marks.is_left(group),
    )
}
