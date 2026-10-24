//! The meta leader's removal-completion pass (design 8.3, decision 15):
//! it sweeps removed nodes' snapshot objects and marks a removed node id
//! `vacant` once nothing of its life is left.
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
//! leadership began, on the first view that shows a removed node the last
//! pass did not, and again every [`REMOVAL_RETRY`] while a node is
//! removed. A view that shows no removed node needs no pass. A pass:
//!
//! 1. confirms the view by a meta read barrier that this node answered as
//!    the meta leader in the term it leads, so a deposed meta leader with a
//!    view from before a meta plan does nothing;
//! 2. for each node that view shows `Removed` on a root `r`, asks every
//!    data group whose live leader has not confirmed it in this
//!    leadership for a fresh quorum barrier, this node's own replica first
//!    and then the group's leader candidates ([`leader_candidates`]), up to
//!    [`ASKS_AT_ONCE`] groups at a time, and keeps each `NotMember` answer
//!    for the rest of the leadership: the classification is stable while
//!    the node stays removed on `r`, since no placement gives it a role;
//! 3. lists the groups its directory holds objects of
//!    ([`ursula_runtime::SnapshotStore::list_node_groups`]), and per
//!    confirmed group lists the node's objects of it, confirms the view
//!    again and deletes exactly the listed objects when it still shows the
//!    node removed on `r` ([`crate::snapshot_holds::sweep_removed_group`]).
//!    The second barrier comes after the listing, so every object deleted
//!    was written while the node was still removed on `r`, before any new
//!    life of the id bound: a meta leader deposed mid-pass deletes no new
//!    life's object. Then it lists the groups again;
//! 4. reads whether this node's committed or effective meta membership
//!    still lists the node, by the kernel's one classifier;
//! 5. sends the node a genesis probe, which only a process of `r` answers
//!    in a way that counts ([`MetaStatusReader::bound_process_answers`]);
//! 6. once every group confirmed, the directory lists no group, the meta
//!    memberships do not list the node and no process of `r` answers,
//!    reports the removal complete (`ControlCommand::ReportRemovalComplete`),
//!    a compare-and-set on the removed record and `r`, which turns the id
//!    vacant.
//!
//! An answer deletes nothing once this node stopped leading meta in that
//! term or its fence refuses, and the pass then ends.
//!
//! The pass keeps no replicated state. What each removed node's removal
//! still waits for is the report's `removal_pending`
//! ([`ursula_control::PendingRemoval`]), published until this node stops
//! leading.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

use futures_util::StreamExt;
use openraft::rt::WatchReceiver;
use ursula_control::CandidatesOf;
use ursula_control::ControlApplied;
use ursula_control::PendingRemoval;
use ursula_control::PlacementView;
use ursula_control::RemovalDirectory;
use ursula_control::group_ids;
use ursula_control::leader_candidates;
use ursula_proto::placement::MembershipAnswer;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeLifecycle;
use ursula_proto::placement::RootId;
use ursula_runtime::NodeFence;
use ursula_shard::RaftGroupId;

use super::decommission::DECOMMISSION_INTERVAL;
use super::decommission::leading;
use super::status::MetaStatusReader;
use crate::RaftGroupHandleRegistry;
use crate::hosting::asker::live_barrier;
use crate::memberships::LeaderMemberships;
use crate::rt::sync::watch;
use crate::rt::time::Instant;
use crate::snapshot_holds::GroupSweep;
use crate::snapshot_holds::sweep_removed_group;

/// How long a meta leader waits between two passes while a node is
/// removed.
pub const REMOVAL_RETRY: Duration = Duration::from_secs(30);
/// The most groups a pass asks at once.
pub const ASKS_AT_ONCE: usize = 8;

/// What each removed node's removal still waits for.
pub(crate) type RemovalPending = BTreeMap<NodeId, PendingRemoval>;

/// The groups whose live leader confirmed, in the current meta leadership,
/// that a removed life is no member, by removed life.
type Confirmations = BTreeMap<NodeId, (RootId, BTreeSet<RaftGroupId>)>;

/// The last pass of a leadership: its term, when it began and the removed
/// nodes it saw.
struct LastPass {
    term: u64,
    began: Instant,
    nodes: BTreeSet<NodeId>,
}

/// What the pass of node `me` runs with.
pub(crate) struct RemovedSweep {
    pub(crate) me: NodeId,
    pub(crate) reader: MetaStatusReader,
    pub(crate) registry: RaftGroupHandleRegistry,
    pub(crate) fence: NodeFence,
    pub(crate) pending: watch::Sender<RemovalPending>,
}

impl RemovedSweep {
    /// Runs passes until `stopped` is true (see the module docs).
    pub(crate) async fn run(self, stopped: impl Fn() -> bool) {
        let mut last: Option<LastPass> = None;
        let mut confirmations = Confirmations::new();
        while !stopped() {
            crate::rt::time::sleep(DECOMMISSION_INTERVAL).await;
            let Some(leading) = leading(&self.registry, self.me) else {
                last = None;
                confirmations.clear();
                self.clear_pending();
                continue;
            };
            if last.as_ref().is_some_and(|last| last.term != leading.term) {
                last = None;
                confirmations.clear();
            }
            let removed = removed_nodes(&self.registry.placement_watch().current(), self.me);
            if removed.is_empty() {
                self.clear_pending();
                continue;
            }
            confirmations.retain(|node, (root, _)| removed.get(node) == Some(root));
            let nodes: BTreeSet<NodeId> = removed.keys().copied().collect();
            let due = last.as_ref().is_none_or(|last| {
                last.began.elapsed() >= REMOVAL_RETRY || !nodes.is_subset(&last.nodes)
            });
            if !due || !self.fence.admits_now() {
                continue;
            }
            last = Some(LastPass {
                term: leading.term,
                began: Instant::now(),
                nodes,
            });
            if let Some(pending) = self.pass(leading.term, &mut confirmations).await {
                self.pending.send_replace(pending);
            }
        }
    }

    fn clear_pending(&self) {
        self.pending.send_if_modified(|pending| {
            let had = !pending.is_empty();
            pending.clear();
            had
        });
    }

    /// One pass in the meta term `term`: what each removal still waits
    /// for, `None` when it proved no leadership in `term`.
    async fn pass(&self, term: u64, confirmations: &mut Confirmations) -> Option<RemovalPending> {
        let confirmed = match self.reader.confirm_view().await {
            Ok(confirmed) => confirmed,
            Err(error) => {
                tracing::debug!(%error, "removal pass: no confirmed view; trying later");
                return None;
            }
        };
        let proven = confirmed.stamp().applied.get();
        if proven.leader != self.me || proven.term != term {
            return None;
        }
        let view = confirmed.view();
        let Some(cluster) = view.cluster() else {
            return Some(RemovalPending::new());
        };
        let groups: BTreeSet<RaftGroupId> = group_ids(cluster.params().group_count).collect();
        let peers: BTreeSet<NodeId> = self
            .registry
            .peer_cluster()
            .directory()
            .node_ids()
            .map(NodeId::new)
            .collect();
        let mut pending = RemovalPending::new();
        let peers = &peers;
        for (node, root) in removed_nodes(view, self.me) {
            let (_, confirmed) = confirmations
                .entry(node)
                .and_modify(|(life, confirmed)| {
                    if *life != root {
                        *life = root;
                        confirmed.clear();
                    }
                })
                .or_insert_with(|| (root, BTreeSet::new()));
            let asked: Vec<RaftGroupId> = groups.difference(confirmed).copied().collect();
            let answers: Vec<(RaftGroupId, Option<MembershipAnswer>)> =
                futures_util::stream::iter(asked)
                    .map(|group| async move {
                        (group, self.answer(term, node, group, view, peers).await)
                    })
                    .buffer_unordered(ASKS_AT_ONCE)
                    .collect()
                    .await;
            if !self.leads(term) {
                return None;
            }
            for (group, answer) in answers {
                if answer == Some(MembershipAnswer::NotMember) {
                    confirmed.insert(group);
                }
            }
            let directory = self.sweep(term, node, root, confirmed).await?;
            let progress = PendingRemoval {
                root,
                unconfirmed: groups.difference(confirmed).copied().collect(),
                directory,
                meta_member: self.meta_lists(node),
                process_answers: self.reader.bound_process_answers(node).await,
            };
            if !progress.is_complete() {
                pending.insert(node, progress);
                continue;
            }
            if !self.leads(term) {
                return None;
            }
            match self.reader.report_removal_complete(node, root).await {
                Ok(ControlApplied::RemovalCompleted(record)) => tracing::info!(
                    node = node.get(),
                    %root,
                    slot = %record.root,
                    "removal pass: the node's removal completed; its id is vacant and an empty \
                     disk may register under it"
                ),
                Ok(other) => {
                    tracing::warn!(node = node.get(), ?other, "removal pass: unexpected answer");
                    pending.insert(node, progress);
                }
                Err(error) => {
                    tracing::info!(
                        node = node.get(),
                        %error,
                        "removal pass: the removal report was not applied; trying later"
                    );
                    pending.insert(node, progress);
                }
            }
        }
        Some(pending)
    }

    /// Sweeps `node`'s objects of every group in `confirmed`, each once the
    /// view, confirmed again after its listing, still shows `node` removed
    /// on `root`, then lists the directory again: what it holds, `None`
    /// once this node no longer leads meta in `term`.
    async fn sweep(
        &self,
        term: u64,
        node: NodeId,
        root: RootId,
        confirmed: &BTreeSet<RaftGroupId>,
    ) -> Option<RemovalDirectory> {
        let store = self.registry.snapshot_store();
        let groups = match store.list_node_groups(node.get()).await {
            Ok(groups) => groups,
            Err(error) => {
                tracing::warn!(
                    node = node.get(),
                    %error,
                    "removal pass: the node's snapshot directory did not list; trying later"
                );
                return Some(RemovalDirectory::Unlisted);
            }
        };
        for group in groups.intersection(confirmed).copied() {
            // A deposed or fenced leader stops its pass.
            if !self.leads(term) {
                return None;
            }
            let answer = async {
                self.still_removed(term, node, root)
                    .await
                    .then_some(MembershipAnswer::NotMember)
            };
            match sweep_removed_group(&store, node, group, answer).await {
                Ok(GroupSweep::Empty) => {}
                Ok(GroupSweep::Swept { deleted }) => tracing::info!(
                    node = node.get(),
                    group = group.0,
                    deleted,
                    "removal pass: deleted the removed node's snapshot objects of the group"
                ),
                Ok(GroupSweep::Kept { answer }) => tracing::debug!(
                    node = node.get(),
                    group = group.0,
                    ?answer,
                    "removal pass: the view no longer shows the node removed on its root"
                ),
                Err(error) => tracing::warn!(
                    node = node.get(),
                    group = group.0,
                    %error,
                    "removal pass: the group's objects were not swept; trying later"
                ),
            }
        }
        match store.list_node_groups(node.get()).await {
            Ok(left) if left.is_empty() => Some(RemovalDirectory::Swept),
            Ok(left) => Some(RemovalDirectory::Unswept(left)),
            Err(error) => {
                tracing::warn!(
                    node = node.get(),
                    %error,
                    "removal pass: the node's snapshot directory did not list; trying later"
                );
                Some(RemovalDirectory::Unlisted)
            }
        }
    }

    /// Whether a view this node confirmed as the meta leader in `term`,
    /// now, still shows `node` removed on `root`, and this node still leads
    /// meta in `term` with an admitting fence.
    async fn still_removed(&self, term: u64, node: NodeId, root: RootId) -> bool {
        let Ok(confirmed) = self.reader.confirm_view().await else {
            return false;
        };
        let proven = confirmed.stamp().applied.get();
        proven.leader == self.me
            && proven.term == term
            && self.leads(term)
            && confirmed
                .view()
                .cluster()
                .and_then(|cluster| cluster.node(node))
                .is_some_and(|record| {
                    record.lifecycle == NodeLifecycle::Removed && record.root.root() == root
                })
    }

    /// Whether this node's committed or effective meta membership lists
    /// `node`, as voter or learner, by the kernel's one classifier. A node
    /// that runs no meta replica, or whose memberships do not read, counts
    /// as listing it.
    fn meta_lists(&self, node: NodeId) -> bool {
        let Some(active) = self.registry.meta() else {
            return true;
        };
        let watched = active.core.owner().metrics();
        let metrics = watched.borrow_watched();
        LeaderMemberships::of(&metrics).answer(node) != Ok(MembershipAnswer::NotMember)
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
                    "removal pass: the answer's memberships are malformed"
                ),
            }
        }
        None
    }
}

/// The nodes `view` shows removed, but `me`, with the root of each removed
/// life.
fn removed_nodes(view: &PlacementView, me: NodeId) -> BTreeMap<NodeId, RootId> {
    view.cluster().map_or_else(BTreeMap::new, |cluster| {
        cluster
            .nodes()
            .iter()
            .filter(|(node, record)| **node != me && record.lifecycle == NodeLifecycle::Removed)
            .map(|(node, record)| (*node, record.root.root()))
            .collect()
    })
}
