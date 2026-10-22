//! What the `placement-move` family does to a placed [`MetaCluster`] to
//! move its groups (design 15.2): control commands through a node's meta
//! handle, as `ursulactl` sends them to the admin plane, held reconcile
//! and teardown steps through the node's one pause registry
//! ([`MetaPausePoint::ShellStep`], [`MetaPausePoint::Teardown`]),
//! leadership transfers through OpenRaft,
//! partitions of one plane, and a drained node. The family's runs
//! ([`super::placement_move`]) wait on the results with their checks.

use std::collections::BTreeSet;

use ursula_control::ControlApplied;
use ursula_control::ControlCommand;
use ursula_control::MoveLimit;
use ursula_control::TeardownStep;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::ReconcileState;
use ursula_proto::placement::StepKind;
use ursula_proto::placement::VoterSet;
use ursula_raft::LeadershipShedReason;
use ursula_raft::RaftTarget;
use ursula_raft::meta::handle::MetaWriteError;
use ursula_raft::meta::pause::MetaPausePoint;
use ursula_shard::RaftGroupId;

use super::meta_cluster::MetaCluster;

/// `nodes` as a voter set.
pub(super) fn voter_set(nodes: &BTreeSet<u64>) -> VoterSet {
    VoterSet::new(nodes.iter().copied().map(NodeId::new).collect()).expect("a move names voters")
}

/// The pause point of a shell step of `group`.
fn shell_step(group: u32, step: StepKind) -> MetaPausePoint {
    MetaPausePoint::ShellStep {
        target: RaftTarget::Data(RaftGroupId(group)),
        step,
    }
}

/// The pause point of a teardown step of `group`.
fn teardown_step(group: u32, step: TeardownStep) -> MetaPausePoint {
    MetaPausePoint::Teardown {
        group: RaftGroupId(group),
        step,
    }
}

impl MetaCluster {
    /// Submits a move of `group` to `to` through `via`, at the placement
    /// the latest view shows: a move, a retarget, or a cancel when `to` is
    /// the move's source.
    pub(super) async fn submit_move(
        &self,
        via: u64,
        group: u32,
        to: &BTreeSet<u64>,
    ) -> Result<ControlApplied, MetaWriteError> {
        let Some(placement) = self.placement(group) else {
            return Err(MetaWriteError::Refused(
                ursula_control::ControlError::NotBootstrapped,
            ));
        };
        self.handle(via)
            .submit(ControlCommand::RequestMove {
                group: RaftGroupId(group),
                expected: placement.since(),
                to: voter_set(to),
                now_ms: ursula_runtime::unix_time_ms(),
            })
            .await
    }

    /// Submits the move limit `limit` through `via`.
    pub(super) async fn submit_limit(
        &self,
        via: u64,
        limit: u32,
    ) -> Result<ControlApplied, MetaWriteError> {
        self.handle(via)
            .submit(ControlCommand::SetMoveLimit {
                limit: MoveLimit::new(limit),
            })
            .await
    }

    /// The move `group`'s placement is in, by the latest view.
    pub(super) fn moving(&self, group: u32) -> Option<ursula_control::Move> {
        self.placement(group)
            .and_then(|placement| placement.moving().cloned())
    }

    /// The target voters of `group`'s placement in the latest view.
    pub(super) fn target_voters(&self, group: u32) -> Option<BTreeSet<u64>> {
        self.placement(group)
            .map(|placement| super::placed_hosting::node_ids(placement.target_voters().iter()))
    }

    /// Whether every running node's view places `group` with target `to`,
    /// moving or stable.
    pub(super) fn every_view_targets(&self, group: u32, to: &BTreeSet<u64>) -> bool {
        self.running().into_iter().all(|node_id| {
            self.view(node_id)
                .cluster()
                .and_then(|cluster| cluster.group(RaftGroupId(group)))
                .is_some_and(|placement| {
                    super::placed_hosting::node_ids(placement.target_voters().iter()) == *to
                })
        })
    }

    /// Closes `node_id`'s pause point before its shell of `group` runs a
    /// step of kind `step`, until [`Self::release`].
    pub(super) fn hold(&self, node_id: u64, group: u32, step: StepKind) {
        drop(
            self.registry(node_id)
                .pauses()
                .pause(shell_step(group, step)),
        );
    }

    /// Opens what [`Self::hold`] closed on `node_id`, if its process still
    /// runs: a new process starts with every point open.
    pub(super) fn release(&self, node_id: u64, group: u32, step: StepKind) {
        self.registry(node_id)
            .pauses()
            .release(shell_step(group, step));
    }

    /// Whether `node_id`'s shell of `group` publishes that it takes a step
    /// of kind `step`: one held at its pause point, while it is closed. The
    /// shell publishes a report as [`ReconcileState::Reporting`], every
    /// other step as [`ReconcileState::Stepping`].
    pub(super) fn stepping(&self, node_id: u64, group: u32, step: StepKind) -> bool {
        let published = match step {
            StepKind::ReportConverged => ReconcileState::Reporting,
            step => ReconcileState::Stepping { step },
        };
        self.registry(node_id)
            .metrics_snapshot()
            .into_iter()
            .find(|snapshot| snapshot.raft_group_id == group)
            .and_then(|snapshot| snapshot.reconcile)
            .is_some_and(|status| status.state == published)
    }

    /// Closes `node_id`'s pause point before its teardown of `group` runs
    /// step `step`, until [`Self::release_teardown`].
    pub(super) fn hold_teardown(&self, node_id: u64, group: u32, step: TeardownStep) {
        drop(
            self.registry(node_id)
                .pauses()
                .pause(teardown_step(group, step)),
        );
    }

    /// Opens what [`Self::hold_teardown`] closed on `node_id`, if its
    /// process still runs.
    pub(super) fn release_teardown(&self, node_id: u64, group: u32, step: TeardownStep) {
        self.registry(node_id)
            .pauses()
            .release(teardown_step(group, step));
    }

    /// Whether `node_id`'s teardown of `group` waits at its held step
    /// `step`.
    pub(super) fn teardown_held(&self, node_id: u64, group: u32, step: TeardownStep) -> bool {
        self.runtimes.contains_key(&node_id)
            && self
                .registry(node_id)
                .pauses()
                .waiting(teardown_step(group, step))
    }

    /// Asks `from`'s replica of `group` to hand its leadership to `to`,
    /// through OpenRaft, outside the reconcile shell.
    pub(super) async fn trigger_transfer(&self, group: u32, from: u64, to: u64) {
        let Some(raft) = self.hosted_raft(from, group) else {
            return;
        };
        if let Err(error) = raft.trigger().transfer_leader(to).await {
            tracing::info!(group, from, to, %error, "a leadership transfer did not start");
        }
    }

    /// Cuts `node_id` off from every other node on the data plane: its
    /// Raft RPCs, recovery calls and gate answers. Its meta plane stays.
    pub(super) fn cut_data(&self, node_id: u64) {
        for other in self.data.nodes().filter(|other| *other != node_id) {
            self.data.policy.partition_bidirectional(node_id, other);
        }
    }

    /// Ends [`Self::cut_data`].
    pub(super) fn heal_data(&self, node_id: u64) {
        for other in self.data.nodes().filter(|other| *other != node_id) {
            self.data.policy.heal_bidirectional(node_id, other);
        }
    }

    /// Cuts `node_id` off from every other node on the meta plane, so its
    /// meta replica falls behind and its view stays where it is. Its data
    /// plane stays.
    pub(super) fn cut_meta(&self, node_id: u64) {
        for other in self.data.nodes().filter(|other| *other != node_id) {
            self.meta_policy.partition_bidirectional(node_id, other);
        }
    }

    /// Ends [`Self::cut_meta`].
    pub(super) fn heal_meta(&self, node_id: u64) {
        for other in self.data.nodes().filter(|other| *other != node_id) {
            self.meta_policy.heal_bidirectional(node_id, other);
        }
    }

    /// Drains `node_id`'s leaderships, as `ursulactl drain` does: its
    /// replicas campaign only where they are their group's only placed
    /// voter.
    pub(super) fn drain(&self, node_id: u64) {
        self.registry(node_id)
            .mark_leadership_shed(LeadershipShedReason::MaintenanceDrain);
    }
}
