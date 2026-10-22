//! The meta leader's sweep of removed nodes' snapshot objects (design 8.3).
//!
//! Only an owner deletes its snapshot objects (`snapshot_holds`), and a
//! removed node that is gone never deletes its own. A removed node never
//! regains a role, so once a group stopped counting it, no replica uses its
//! objects of that group again. The meta leader therefore deletes them: the
//! one exception to owner-only deletion
//! ([`ursula_runtime::SnapshotStore::delete_node_object`]).
//!
//! While this node's meta replica leads under a committed vote with a
//! settled history and the node fence admits, a pass runs once a
//! leadership began and again every [`SWEEP_INTERVAL`] while it lasts.
//! A view that shows no removed node needs no pass. A pass:
//!
//! 1. confirms the view by a meta read barrier that this node answered as
//!    the meta leader in the term it leads, so a deposed meta leader with a
//!    view from before a meta plan sweeps nothing;
//! 2. lists, for each node that view shows `Removed`, the groups its
//!    directory holds objects of
//!    ([`ursula_runtime::SnapshotStore::list_node_groups`]);
//! 3. per group, lists the node's objects of it, then asks the group for a
//!    fresh quorum barrier, this node's own replica first and then the
//!    group's leader candidates ([`leader_candidates`]) until a live leader
//!    answers, and deletes exactly the listed objects when that answer
//!    classifies the node `NotMember` by the kernel's one classifier
//!    ([`crate::snapshot_holds::sweep_removed_group`]). An answer deletes
//!    nothing once this node stopped leading meta in that term or its fence
//!    refuses, and the pass then ends.
//!
//! The pass keeps no replicated state. The groups whose objects it left,
//! for want of a live leader's answer, for an answer that still counts the
//! node or for a failed store call, are the report's `sweep_pending` by
//! removed node, published until this node stops leading. A node whose
//! directory did not list keeps the previous pass's entry.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

use ursula_control::CandidatesOf;
use ursula_control::PlacementView;
use ursula_control::leader_candidates;
use ursula_proto::placement::MembershipAnswer;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeLifecycle;
use ursula_runtime::NodeFence;
use ursula_shard::RaftGroupId;

use super::decommission::DECOMMISSION_INTERVAL;
use super::decommission::leading;
use super::status::MetaStatusReader;
use crate::RaftGroupHandleRegistry;
use crate::hosting::asker::live_barrier;
use crate::rt::sync::watch;
use crate::rt::time::Instant;
use crate::snapshot_holds::GroupSweep;
use crate::snapshot_holds::sweep_removed_group;

/// How long a meta leader waits between two passes of the sweep.
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(10 * 60);

/// The groups whose objects the sweep left, by removed node.
pub(crate) type SweepPending = BTreeMap<NodeId, BTreeSet<RaftGroupId>>;

/// What the sweep of node `me` runs with.
pub(crate) struct RemovedSweep {
    pub(crate) me: NodeId,
    pub(crate) reader: MetaStatusReader,
    pub(crate) registry: RaftGroupHandleRegistry,
    pub(crate) fence: NodeFence,
    pub(crate) pending: watch::Sender<SweepPending>,
}

impl RemovedSweep {
    /// Runs passes until `stopped` is true (see the module docs).
    pub(crate) async fn run(self, stopped: impl Fn() -> bool) {
        // The term of the last pass, and when it began.
        let mut last: Option<(u64, Instant)> = None;
        while !stopped() {
            crate::rt::time::sleep(DECOMMISSION_INTERVAL).await;
            let Some(leading) = leading(&self.registry, self.me) else {
                last = None;
                self.pending.send_if_modified(|pending| {
                    let had = !pending.is_empty();
                    pending.clear();
                    had
                });
                continue;
            };
            let due = last.is_none_or(|(term, began)| {
                term != leading.term || began.elapsed() >= SWEEP_INTERVAL
            });
            if !due || !self.fence.admits_now() {
                continue;
            }
            last = Some((leading.term, Instant::now()));
            if let Some(pending) = self.pass(leading.term).await {
                self.pending.send_replace(pending);
            }
        }
    }

    /// One pass in the meta term `term`: what it left, `None` when it
    /// proved no leadership in `term`.
    async fn pass(&self, term: u64) -> Option<SweepPending> {
        if removed_nodes(&self.registry.placement_watch().current(), self.me).is_empty() {
            return Some(SweepPending::new());
        }
        let confirmed = match self.reader.confirm_view().await {
            Ok(confirmed) => confirmed,
            Err(error) => {
                tracing::debug!(%error, "removed-node sweep: no confirmed view; sweeping later");
                return None;
            }
        };
        let proven = confirmed.stamp().applied.get();
        if proven.leader != self.me || proven.term != term {
            return None;
        }
        let view = confirmed.view();
        let peers: BTreeSet<NodeId> = self
            .registry
            .peer_cluster()
            .directory()
            .node_ids()
            .map(NodeId::new)
            .collect();
        let store = self.registry.snapshot_store();
        let mut pending = SweepPending::new();
        for node in removed_nodes(view, self.me) {
            let groups = match store.list_node_groups(node.get()).await {
                Ok(groups) => groups,
                Err(error) => {
                    tracing::warn!(
                        node = node.get(),
                        %error,
                        "removed-node sweep: the node's snapshot directory did not list; sweeping later"
                    );
                    if let Some(left) = self.pending.borrow().get(&node) {
                        pending.insert(node, left.clone());
                    }
                    continue;
                }
            };
            for group in groups {
                // A deposed or fenced leader stops its pass.
                if !self.leads(term) {
                    return None;
                }
                let answer = self.answer(term, node, group, view, &peers);
                match sweep_removed_group(&store, node, group, answer).await {
                    Ok(GroupSweep::Empty) => {}
                    Ok(GroupSweep::Swept { deleted }) => tracing::info!(
                        node = node.get(),
                        group = group.0,
                        deleted,
                        "removed-node sweep: deleted the removed node's snapshot objects of the group"
                    ),
                    Ok(GroupSweep::Kept { answer }) => {
                        tracing::debug!(
                            node = node.get(),
                            group = group.0,
                            ?answer,
                            "removed-node sweep: the group did not answer that the node is no member"
                        );
                        pending.entry(node).or_default().insert(group);
                    }
                    Err(error) => {
                        tracing::warn!(
                            node = node.get(),
                            group = group.0,
                            %error,
                            "removed-node sweep: the group's objects were not swept; sweeping later"
                        );
                        pending.entry(node).or_default().insert(group);
                    }
                }
            }
        }
        Some(pending)
    }

    /// A live leader of `group`'s answer about `removed`, if this node
    /// still leads meta in `term` and its fence admits once it arrived.
    async fn answer(
        &self,
        term: u64,
        removed: NodeId,
        group: RaftGroupId,
        view: &PlacementView,
        peers: &BTreeSet<NodeId>,
    ) -> Option<MembershipAnswer> {
        let answer = self.live_answer(removed, group, view, peers).await?;
        self.leads(term).then_some(answer)
    }

    /// Whether this node still leads meta in `term` and its fence admits.
    fn leads(&self, term: u64) -> bool {
        leading(&self.registry, self.me).is_some_and(|leading| leading.term == term)
            && self.fence.admits_now()
    }

    /// Asks this node's replica of `group`, then its leader candidates in
    /// order, for a fresh quorum barrier, until one answers as a live leader
    /// whose memberships classify `removed`.
    async fn live_answer(
        &self,
        removed: NodeId,
        group: RaftGroupId,
        view: &PlacementView,
        peers: &BTreeSet<NodeId>,
    ) -> Option<MembershipAnswer> {
        // The candidates leave this node out, and it may lead the group:
        // the barrier its own server would answer with.
        if let Ok(answer) = self.registry.confirm_recovery_barrier(group).await
            && let Ok(classified) = answer.memberships.answer(removed)
        {
            return Some(classified);
        }
        let candidates = leader_candidates(self.me, CandidatesOf::Group(group), view, None, peers);
        for leader in candidates.iter() {
            let Some(answer) = live_barrier(&self.reader, group, leader).await else {
                continue;
            };
            match answer.memberships.answer(removed) {
                Ok(classified) => return Some(classified),
                // Checked on arrival, so this is no live leader's answer.
                Err(error) => tracing::debug!(
                    group = group.0,
                    leader = leader.get(),
                    %error,
                    "removed-node sweep: the answer's memberships are malformed"
                ),
            }
        }
        None
    }
}

/// The nodes `view` shows removed, but `me`.
fn removed_nodes(view: &PlacementView, me: NodeId) -> Vec<NodeId> {
    view.cluster().map_or_else(Vec::new, |cluster| {
        cluster
            .nodes()
            .iter()
            .filter(|(node, record)| **node != me && record.lifecycle == NodeLifecycle::Removed)
            .map(|(node, _)| *node)
            .collect()
    })
}
