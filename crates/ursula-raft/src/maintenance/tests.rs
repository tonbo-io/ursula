//! The maintenance report's decision over typed inputs: design 15.3's
//! readiness cases as far as S2 reaches, and the voter checks.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

use ursula_control::Cluster;
use ursula_control::ClusterView;
use ursula_control::ConfirmedStamp;
use ursula_control::ControlCommand;
use ursula_control::ControlPlaneState;
use ursula_control::Counts;
use ursula_control::MetaDamageKind;
use ursula_control::MetaReplicaStatus;
use ursula_control::MetaRole;
use ursula_control::PlacementStatus;
use ursula_control::RegistrationStatus;
use ursula_control::samples;
use ursula_proto::placement::ClusterStage;
use ursula_proto::placement::GenesisClaim;
use ursula_proto::placement::MembershipAnswer;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::Registered;
use ursula_proto::placement::ReplicaRole;
use ursula_shard::RaftGroupId;

use super::META_UNAVAILABLE_AFTER;
use super::MOVE_STALLED_AFTER;
use super::MaintenanceInputs;
use super::NodePlacement;
use super::RaftMaintenanceIssue as Issue;
use super::RaftMaintenanceReport;
use super::check_raft_maintenance;
use crate::HostingStatus;
use crate::RaftGroupMaintenanceState;
use crate::RaftLogProgressSnapshot;
use crate::types::RaftGroupMetricsSnapshot;

/// A cluster of `nodes` bootstrapped with `groups` seeded groups at `rf`.
fn bootstrapped(nodes: &[u64], groups: u32, rf: u8) -> Cluster {
    let mut state = ControlPlaneState::default();
    let mut index = 0_u64;
    let mut apply = |command| {
        index = index.checked_add(1).unwrap();
        state.apply(samples::meta(1, index), command).unwrap();
    };
    for id in nodes {
        apply(samples::register(*id, 10));
    }
    apply(ControlCommand::Bootstrap {
        params: samples::params(nodes, groups, rf, 4),
        nodes: samples::node_set(nodes),
        now_ms: 20,
    });
    state.cluster().unwrap().clone()
}

/// Node 1's replica of group `id`, healthy on {1, 2, 3} with leader 2.
fn healthy(id: u32) -> RaftGroupMetricsSnapshot {
    let progress = RaftLogProgressSnapshot {
        term: 1,
        index: 20,
        node_id: 1,
    };
    RaftGroupMetricsSnapshot {
        apply_failure: None,
        raft_group_id: id,
        node_id: 1,
        current_term: 1,
        current_leader: Some(2),
        last_log: Some(progress),
        committed: Some(progress),
        last_applied: Some(progress),
        snapshot: None,
        purged: None,
        voter_ids: vec![1, 2, 3],
        voter_configurations: vec![BTreeSet::from([1, 2, 3])],
        committed_voter_configurations: vec![BTreeSet::from([1, 2, 3])],
        learner_ids: vec![],
        maintenance: RaftGroupMaintenanceState {
            running: true,
            recovery_ready: true,
            accepting_transfers: true,
            membership_joint: false,
            membership_log_index: Some(10),
            stopped_for_operator: false,
        },
        log: Default::default(),
        history: None,
        recovery: None,
        recovery_gate: ursula_proto::admin::RecoveryGateStatus::Open,
        leads_live: false,
        reconcile: None,
        needs_vote_floor: false,
    }
}

/// What node `me` knows: by default a bootstrapped view of `cluster` from
/// a running meta replica with a leader, registered, holding and running a
/// healthy replica of each group it is placed in.
struct Node {
    me: u64,
    status: PlacementStatus,
    claim: GenesisClaim,
    leaderless_for: Option<Duration>,
    hosting: HostingStatus,
    groups: Vec<RaftGroupMetricsSnapshot>,
    not_voters: BTreeSet<u32>,
    now_ms: u64,
}

impl Node {
    fn placed(cluster: Cluster, id: u64) -> Self {
        let me = NodeId::new(id);
        let params = cluster.params().clone();
        let placed = cluster
            .groups()
            .iter()
            .filter(|(_, placement)| placement.role(me).is_some())
            .map(|(group, _)| group)
            .collect::<BTreeSet<_>>();
        let record = *cluster.node(me).unwrap();
        Self {
            me: id,
            status: PlacementStatus {
                node: me,
                applied: Some(samples::meta(1, 9)),
                view: ClusterView::Bootstrapped(cluster.clone()),
                meta: MetaReplicaStatus::Running {
                    role: MetaRole::Voter,
                    leader: Some(NodeId::new(2)),
                },
                registration: RegistrationStatus::Answered(Registered::Referenced(record)),
                last_bootstrap_refusal: None,
            },
            claim: GenesisClaim {
                meta_voters: params.meta_voters.clone(),
                peers: samples::node_set(
                    &cluster
                        .nodes()
                        .keys()
                        .map(|id| id.get())
                        .collect::<Vec<_>>(),
                ),
                group_count: params.group_count,
                replication_factor: params.replication_factor,
            },
            leaderless_for: None,
            hosting: HostingStatus {
                held: placed.clone(),
                ..HostingStatus::default()
            },
            groups: placed.iter().map(|group| healthy(group.0)).collect(),
            not_voters: BTreeSet::new(),
            now_ms: 1_000_000_000,
        }
    }

    /// A node of a cluster on {1, 2, 3} with groups 0 and 1.
    fn three() -> Self {
        Self::placed(bootstrapped(&[1, 2, 3], 2, 3), 1)
    }

    /// The node with an empty view, holding and running nothing: a wiped
    /// voter, or a node before genesis.
    fn knowing_nothing(mut self) -> Self {
        self.status.view = ClusterView::MetaCatchingUp;
        self.status.applied = None;
        self.status.registration = RegistrationStatus::Pending;
        self.hosting = HostingStatus::default();
        self.groups.clear();
        self
    }

    fn report(&self) -> RaftMaintenanceReport {
        check_raft_maintenance(MaintenanceInputs {
            node_id: self.me,
            lag_tolerance: 16,
            now_ms: self.now_ms,
            groups: &self.groups,
            counts: |group: RaftGroupId| {
                if self.not_voters.contains(&group.0) {
                    Counts::NotVoter
                } else {
                    Counts::Voter
                }
            },
            placement: Some(NodePlacement {
                status: &self.status,
                claim: &self.claim,
                leaderless_for: self.leaderless_for,
                hosting: &self.hosting,
            }),
        })
    }
}

#[test]
fn a_placed_node_with_healthy_replicas_is_ready_and_reports_its_placement() {
    let node = Node::three();
    let report = node.report();
    assert!(report.node_issues.is_empty(), "{report:?}");
    assert!(report.group_issues.is_empty(), "{report:?}");
    assert!(report.serving_ready() && report.ready());
    let placement = report.placement.unwrap();
    assert_eq!(placement.view, ClusterStage::Bootstrapped);
    assert_eq!(placement.applied, Some(samples::meta(1, 9)));
    assert_eq!(placement.meta_role, Some(MetaRole::Voter));
    assert_eq!(placement.meta_leader, Some(NodeId::new(2)));
    assert_eq!(
        placement.lifecycle,
        Some(ursula_proto::placement::NodeLifecycle::Active)
    );
    assert!(matches!(
        placement.registration,
        Some(Registered::Referenced(_))
    ));
    assert_eq!(
        report.expected_groups.keys().copied().collect::<Vec<_>>(),
        vec![0, 1]
    );
    let expected = &report.expected_groups[&0];
    assert_eq!(expected.role, ReplicaRole::Voter);
    assert_eq!(expected.target, samples::voters(&[1, 2, 3]));
    assert!(expected.counts_as_voter);
}

/// The bug: an empty inventory made 0.9's report unready for good, so a
/// node placed nowhere never passed its probe. Its meta replica holds the
/// placement, so it stays ready with meta down too.
#[test]
fn a_zero_group_node_is_ready_also_restarted_with_meta_down() {
    // The one group is seeded on {1, 2, 3}: node 4 has no role.
    let mut node = Node::placed(bootstrapped(&[1, 2, 3, 4], 1, 3), 4);
    assert!(node.groups.is_empty() && node.hosting.held.is_empty());
    let report = node.report();
    assert!(report.expected_groups.is_empty());
    assert!(report.serving_ready() && report.ready(), "{report:?}");
    // Restarted while meta has no leader: its own meta journal still gives
    // it a bootstrapped view, and it holds nothing a disruption could take.
    node.status.meta = MetaReplicaStatus::Running {
        role: MetaRole::Learner,
        leader: None,
    };
    node.status.registration = RegistrationStatus::Pending;
    node.leaderless_for = Some(META_UNAVAILABLE_AFTER.saturating_mul(2));
    let report = node.report();
    assert!(report.node_issues.is_empty(), "{report:?}");
    assert!(report.serving_ready() && report.ready());
}

/// The bug: a wiped voter that knows no group reads as a zero-group node
/// and serves before its groups exist. It knows nothing however often it
/// restarts while meta is down, then its view names its groups and their
/// replicas hold it until they caught up.
#[test]
fn a_wiped_voter_restarted_twice_with_meta_down_is_unready_until_its_groups_caught_up() {
    let placed = Node::three();
    let mut node = Node::three().knowing_nothing();
    node.status.meta = MetaReplicaStatus::Running {
        role: MetaRole::NotMember,
        leader: None,
    };
    for _restart in 0..2 {
        node.leaderless_for = Some(META_UNAVAILABLE_AFTER);
        let report = node.report();
        assert!(report.node_issues.contains(&Issue::PlacementUnknown));
        assert!(!report.serving_ready() && !report.ready());
    }
    // Meta came back: the view names groups 0 and 1, no replica runs yet.
    node.status = placed.status.clone();
    node.leaderless_for = None;
    let report = node.report();
    assert!(report.node_issues.is_empty(), "{report:?}");
    assert_eq!(
        report.group_issues,
        BTreeMap::from([
            (0, vec![Issue::MissingGroup]),
            (1, vec![Issue::MissingGroup])
        ])
    );
    assert!(!report.serving_ready());
    // Created, catching up behind a closed gate.
    node.groups = placed.groups.clone();
    for group in &mut node.groups {
        group.maintenance.recovery_ready = false;
    }
    assert!(!node.report().serving_ready());
    node.groups = placed.groups;
    assert!(node.report().ready());
}

/// The bug: a learner, or a replica its group answered is no voter, holds
/// the node out of service by its own state.
#[test]
fn a_replica_that_counts_for_nothing_is_checked_only_for_being_stopped() {
    let mut node = Node::three();
    node.not_voters.insert(1);
    let learner = &mut node.groups[1];
    learner.voter_ids = vec![2, 3];
    learner.voter_configurations = vec![BTreeSet::from([2, 3])];
    learner.learner_ids = vec![1];
    learner.current_leader = None;
    learner.maintenance.recovery_ready = false;
    learner.maintenance.membership_joint = true;
    learner.committed = None;
    let report = node.report();
    assert!(!report.expected_groups[&1].counts_as_voter);
    assert!(!report.group_issues.contains_key(&1), "{report:?}");
    assert!(report.serving_ready() && report.ready());
    node.groups[1].maintenance.running = false;
    let report = node.report();
    assert_eq!(report.group_issues[&1], vec![Issue::RaftStopped]);
    assert!(report.serving_ready() && report.ready());
    // The same state on a replica that counts blocks serving.
    node.not_voters.clear();
    assert!(!node.report().serving_ready());
}

/// Meta down blocks nothing (restarts need no meta), and is reported only
/// past the window.
#[test]
fn meta_down_is_reported_past_its_window_and_blocks_nothing() {
    let mut node = Node::three();
    node.status.meta = MetaReplicaStatus::Running {
        role: MetaRole::Voter,
        leader: None,
    };
    node.leaderless_for = Some(
        META_UNAVAILABLE_AFTER
            .checked_sub(Duration::from_millis(1))
            .unwrap(),
    );
    assert!(node.report().node_issues.is_empty());
    node.leaderless_for = Some(META_UNAVAILABLE_AFTER);
    let report = node.report();
    assert_eq!(report.node_issues, vec![Issue::MetaUnavailable]);
    assert!(report.serving_ready() && report.ready());
}

/// The bug (0.9 and S2(h)): a node whose meta is damaged expects no group
/// and is unready for the whole run, so its pod never passes its probe.
/// Damaged meta replaces every view-derived issue, and its replicas keep
/// their voter checks.
#[test]
fn damaged_meta_keeps_only_the_voter_checks() {
    let mut node = Node::three();
    node.status.meta = MetaReplicaStatus::Damaged {
        kind: MetaDamageKind::Journal,
        message: "unreadable".to_owned(),
    };
    node.status.view = ClusterView::MetaGenesis {
        unanswered: BTreeSet::new(),
        genesis_mismatch: BTreeMap::new(),
    };
    node.status.applied = None;
    node.leaderless_for = Some(META_UNAVAILABLE_AFTER);
    let report = node.report();
    assert_eq!(report.node_issues, vec![Issue::MetaDamaged]);
    assert!(report.expected_groups.is_empty() && report.unplaced.is_empty());
    assert!(report.group_issues.is_empty(), "{report:?}");
    assert!(report.serving_ready() && report.ready());
    // A bootstrapped view that names a group it runs no replica of gives
    // no `missing_group` while meta is damaged.
    let mut stale = Node::three();
    stale.status.meta = node.status.meta.clone();
    stale.groups.truncate(1);
    let report = stale.report();
    assert_eq!(report.node_issues, vec![Issue::MetaDamaged]);
    assert!(report.group_issues.is_empty(), "{report:?}");
    // The voter checks stay.
    node.groups[0].current_leader = None;
    let report = node.report();
    assert_eq!(report.group_issues[&0], vec![Issue::LeaderUnknown]);
    assert!(!report.serving_ready());
}

#[test]
fn a_view_before_bootstrap_holds_maintenance_when_replicas_are_held() {
    for view in [ClusterView::FormationCut, ClusterView::Registering {
        registered: BTreeMap::new(),
        waiting_for: BTreeSet::new(),
    }] {
        let mut node = Node::three();
        node.status.view = view.clone();
        let report = node.report();
        assert_eq!(report.node_issues, vec![Issue::PlacementNotBootstrapped]);
        assert!(report.group_issues.is_empty(), "{report:?}");
        assert!(report.serving_ready() && !report.ready());
        // Holding nothing, confirming nothing and answered nothing, the
        // node does not know whether it is a voter.
        let mut node = Node::three().knowing_nothing();
        node.status.view = view;
        assert_eq!(node.report().node_issues, vec![Issue::PlacementUnknown]);
        // A confirmed view, or an answer that it waits for its placement
        // or has none, tells it is no voter yet.
        node.hosting.last_confirmed = Some(ConfirmedStamp {
            history: samples::meta(1, 1),
            applied: samples::meta(1, 3),
        });
        assert!(node.report().ready());
        node.hosting.last_confirmed = None;
        node.status.registration = RegistrationStatus::Answered(Registered::Forming);
        assert!(node.report().ready());
    }
}

/// The bug: a scale-up pod the old pods do not list yet holds nothing and
/// waits for no group, yet stays unready.
#[test]
fn an_unreferenced_node_before_its_view_is_ready() {
    let mut node = Node::three().knowing_nothing();
    let record = *bootstrapped(&[1, 2, 3], 2, 3).node(NodeId::new(1)).unwrap();
    node.status.registration = RegistrationStatus::Answered(Registered::Unreferenced(record));
    node.leaderless_for = Some(META_UNAVAILABLE_AFTER);
    let report = node.report();
    assert!(report.node_issues.is_empty(), "{report:?}");
    assert!(report.ready());
}

#[test]
fn a_genesis_mismatch_holds_maintenance() {
    let mut node = Node::three().knowing_nothing();
    node.hosting.held.insert(RaftGroupId(0));
    node.status.view = ClusterView::MetaGenesis {
        unanswered: BTreeSet::new(),
        genesis_mismatch: BTreeMap::from([(NodeId::new(2), node.claim.clone())]),
    };
    let report = node.report();
    assert_eq!(report.node_issues, vec![
        Issue::PlacementNotBootstrapped,
        Issue::GenesisMismatch
    ]);
    assert!(report.serving_ready() && !report.ready());
}

/// The configured group count decides routing, so a mismatch blocks
/// serving. The replication factor only decides the fsync default and
/// later moves, so it blocks maintenance only.
#[test]
fn a_configuration_the_cluster_does_not_share_is_reported() {
    let mut node = Node::three();
    node.claim.group_count = samples::params(&[1], 3, 1, 4).group_count;
    let report = node.report();
    assert_eq!(report.node_issues, vec![Issue::GroupCountMismatch]);
    assert!(!report.serving_ready() && !report.ready());
    let mut node = Node::three();
    node.claim.replication_factor = samples::params(&[1], 1, 1, 4).replication_factor;
    let report = node.report();
    assert_eq!(report.node_issues, vec![Issue::ReplicationFactorMismatch]);
    assert!(report.serving_ready() && !report.ready());
}

/// A replica the view gives no role is reported with its count, and a
/// counting one keeps its voter checks.
#[test]
fn a_replica_without_a_role_is_unplaced() {
    let mut node = Node::three();
    node.groups.push(healthy(7));
    node.hosting.unplaced.insert(RaftGroupId(9));
    node.not_voters.insert(9);
    let report = node.report();
    assert_eq!(report.unplaced.keys().copied().collect::<Vec<_>>(), vec![
        7, 9
    ]);
    assert!(report.unplaced[&7].counts_as_voter);
    assert!(!report.unplaced[&9].counts_as_voter);
    assert_eq!(report.group_issues[&7], vec![Issue::UnplacedReplica]);
    assert_eq!(report.group_issues[&9], vec![Issue::UnplacedReplica]);
    assert!(report.serving_ready() && !report.ready());
    node.groups[2].current_leader = None;
    assert_eq!(node.report().group_issues[&7], vec![
        Issue::UnplacedReplica,
        Issue::LeaderUnknown
    ]);
}

/// A held replica without a role reports what its group's leader last
/// answered the membership asker, `null` before an answer. The bug it
/// catches: an answer the asker got that the report drops or misplaces.
#[test]
fn an_unplaced_replica_reports_the_last_answer_of_its_group() {
    let mut node = Node::three();
    node.hosting
        .unplaced
        .extend([RaftGroupId(7), RaftGroupId(9)]);
    node.hosting
        .answers
        .insert(RaftGroupId(9), crate::GroupAnswer {
            leader: NodeId::new(3),
            vote: crate::types::UrsulaVote::new_committed(4, 3),
            answer: MembershipAnswer::NotMember,
        });
    let report = node.report();
    assert_eq!(report.unplaced[&7].last_answer, None);
    assert_eq!(
        report.unplaced[&9].last_answer,
        Some(MembershipAnswer::NotMember)
    );
}

/// Every role in a group the view shows moving raises `move_in_progress`,
/// also on a replica that does not count as a voter, and a settled group
/// does not (which readiness it blocks is the issue's row, pinned in
/// `ursula-proto`). The bug it catches: a node taken down while a group it
/// hosts changes its voters.
#[test]
fn a_move_in_progress_is_reported_on_every_replica_of_the_group() {
    let mut cluster = bootstrapped(&[1, 2, 3, 4], 2, 3);
    let placed = cluster.group(RaftGroupId(0)).unwrap().clone();
    let mut state = ControlPlaneState::Bootstrapped(cluster);
    state
        .apply(samples::meta(1, 20), ControlCommand::ReportConverged {
            group: RaftGroupId(0),
            since: placed.since(),
            voters: placed.target_voters().clone(),
            membership: samples::membership(1, 5),
            now_ms: 30,
        })
        .unwrap();
    let since = state
        .cluster()
        .unwrap()
        .group(RaftGroupId(0))
        .unwrap()
        .since();
    let mut voters = placed.target_voters().iter().collect::<BTreeSet<_>>();
    let outgoing = *voters.iter().next().unwrap();
    voters.remove(&outgoing);
    let incoming = [1, 2, 3, 4]
        .map(NodeId::new)
        .into_iter()
        .find(|node| !placed.target_voters().contains(*node))
        .unwrap();
    voters.insert(incoming);
    state
        .apply(samples::meta(1, 21), ControlCommand::RequestMove {
            group: RaftGroupId(0),
            expected: since,
            to: ursula_proto::placement::VoterSet::new(voters).unwrap(),
            now_ms: 40,
        })
        .unwrap();
    cluster = state.cluster().unwrap().clone();
    assert!(cluster.group(RaftGroupId(0)).unwrap().moving().is_some());
    for (id, counts) in [(outgoing, true), (incoming, false)] {
        let mut node = Node::placed(cluster.clone(), id.get());
        if !counts {
            node.not_voters.insert(0);
        }
        let report = node.report();
        assert_eq!(
            report.expected_groups[&0].counts_as_voter, counts,
            "{report:?}"
        );
        assert!(
            report.group_issues[&0].contains(&Issue::MoveInProgress),
            "{report:?}"
        );
        assert!(
            report
                .group_issues
                .values()
                .flatten()
                .filter(|issue| **issue == Issue::MoveInProgress)
                .count()
                == 1,
            "{report:?}"
        );
    }
}

/// A member without a role blocks maintenance but not serving, and nothing
/// dials it. A target voter missing from the membership is an incomplete
/// voter set, not an `unplaced_member`.
#[test]
fn a_member_without_a_role_blocks_maintenance_but_not_serving() {
    for (voters, learners) in [(vec![1, 2, 3, 4], vec![]), (vec![1, 2, 3], vec![4])] {
        let mut node = Node::three();
        let group = &mut node.groups[1];
        group.voter_ids = voters.clone();
        group.voter_configurations = vec![voters.iter().copied().collect()];
        group.learner_ids = learners;
        let report = node.report();
        assert_eq!(report.group_issues[&1], vec![
            Issue::IncompleteVoterSet,
            Issue::UnplacedMember,
        ]);
        assert!(report.serving_ready() && !report.ready());
    }
    let mut node = Node::three();
    let group = &mut node.groups[1];
    group.voter_ids = vec![1, 2];
    group.voter_configurations = vec![BTreeSet::from([1, 2])];
    group.learner_ids = vec![3];
    assert_eq!(node.report().group_issues[&1], vec![
        Issue::IncompleteVoterSet
    ]);
}

#[test]
fn a_node_without_a_meta_runtime_knows_no_placement() {
    let groups = [healthy(0)];
    let report = check_raft_maintenance(MaintenanceInputs {
        node_id: 1,
        lag_tolerance: 16,
        now_ms: 0,
        groups: &groups,
        counts: |_group| Counts::Voter,
        placement: None,
    });
    assert_eq!(report.placement, None);
    assert_eq!(report.node_issues, vec![Issue::PlacementUnknown]);
    assert!(report.group_issues.is_empty());
    assert!(!report.serving_ready());
}

/// Only the leader of a moving group reports `move_stalled`, once its
/// reconciler made no progress for [`MOVE_STALLED_AFTER`]. The bugs it
/// catches: the stall raised for a stable or seeded group, by a follower,
/// or before the five minutes passed.
#[test]
fn only_the_leader_of_a_moving_group_without_progress_reports_move_stalled() {
    // Group 0 is seeded, group 1 stable and group 2 moving, each with a
    // role for node 1.
    let cluster = samples::sample_state().cluster().unwrap().clone();
    let mut node = Node::placed(cluster, 1);
    let stalled_after = u64::try_from(MOVE_STALLED_AFTER.as_millis()).unwrap();
    let status = |progress_ms| ursula_proto::placement::ReconcileStatus {
        state: ursula_proto::placement::ReconcileState::Waiting(
            ursula_proto::placement::WaitReason::GateUnknown {
                nodes: samples::ids(&[2]),
            },
        ),
        since_ms: 7,
        progress_ms,
        last_failure: None,
        attempts: 3,
        target_since: None,
        view_applied: None,
    };
    let stalled_at = node.now_ms - stalled_after;
    for group in &mut node.groups {
        group.current_leader = Some(1);
        group.reconcile = Some(status(stalled_at));
    }
    let stalled = |node: &Node| {
        node.report()
            .group_issues
            .into_iter()
            .filter(|(_, issues)| issues.contains(&Issue::MoveStalled))
            .map(|(group, _)| group)
            .collect::<Vec<_>>()
    };
    assert_eq!(stalled(&node), vec![2]);
    assert!(!node.report().ready());

    node.groups[2].reconcile = Some(status(stalled_at + 1));
    assert_eq!(stalled(&node), Vec::<u32>::new());

    node.groups[2].reconcile = Some(status(stalled_at));
    node.groups[2].current_leader = Some(3);
    assert_eq!(stalled(&node), Vec::<u32>::new());
}

#[test]
fn an_outgoing_joint_voter_is_not_serving_even_while_in_the_union() {
    let mut node = Node::three();
    let group = &mut node.groups[0];
    group.maintenance.membership_joint = true;
    group.voter_configurations = vec![BTreeSet::from([1, 2, 3]), BTreeSet::from([2, 3])];
    let report = node.report();
    assert!(report.group_issues[&0].contains(&Issue::LocalReplicaNotVoter));
    assert!(!report.serving_ready());
}

#[test]
fn a_stopped_group_blocks_maintenance_and_only_its_own_serving() {
    let mut node = Node::three();
    let group = &mut node.groups[0];
    group.maintenance.running = false;
    group.apply_failure = Some(ursula_proto::admin::ApplyFailure {
        term: 2,
        index: 9,
        kind: ursula_proto::admin::ApplyFailureKind::InvariantViolation,
        message: "invariant".to_owned(),
    });
    group.current_leader = None;
    let report = node.report();
    assert_eq!(
        report.group_issues,
        BTreeMap::from([(0, vec![Issue::ApplyStopped])])
    );
    assert!(report.serving_ready() && !report.ready());
}

#[test]
fn a_new_ready_process_cannot_replace_a_voter_before_promotion_is_applied() {
    let mut node = Node::three();
    let group = &mut node.groups[1];
    group.voter_ids = vec![2, 3];
    group.voter_configurations = vec![BTreeSet::from([2, 3])];
    group.learner_ids = vec![1];
    assert!(node.report().group_issues[&1].contains(&Issue::IncompleteVoterSet));
    let group = &mut node.groups[1];
    group.voter_ids = vec![1, 2, 3];
    group.voter_configurations = vec![BTreeSet::from([1, 2, 3])];
    group.learner_ids.clear();
    group.maintenance.membership_log_index = Some(21);
    assert!(node.report().group_issues[&1].contains(&Issue::MembershipNotApplied));
    node.groups[1].last_applied = Some(RaftLogProgressSnapshot {
        term: 1,
        index: 21,
        node_id: 1,
    });
    assert!(node.report().ready());
}

#[test]
fn stopped_recovering_joint_and_operator_stopped_replicas_are_ineligible() {
    for maintenance in [
        RaftGroupMaintenanceState {
            running: false,
            ..healthy(1).maintenance
        },
        RaftGroupMaintenanceState {
            recovery_ready: false,
            ..healthy(1).maintenance
        },
        RaftGroupMaintenanceState {
            membership_joint: true,
            ..healthy(1).maintenance
        },
        RaftGroupMaintenanceState {
            stopped_for_operator: true,
            ..healthy(1).maintenance
        },
        RaftGroupMaintenanceState::default(),
    ] {
        let mut node = Node::three();
        node.groups[1].maintenance = maintenance;
        assert!(!node.report().ready());
    }
}

#[test]
fn a_missing_leader_or_large_apply_gap_prevents_maintenance() {
    let mut node = Node::three();
    node.groups[1].current_leader = None;
    assert!(!node.report().ready());
    node.groups[1].current_leader = Some(4);
    assert!(!node.report().ready());
    node.groups[1].current_leader = Some(2);
    node.groups[1].committed = Some(RaftLogProgressSnapshot {
        term: 1,
        index: 37,
        node_id: 1,
    });
    assert!(!node.report().ready());
    node.groups[1].committed = Some(RaftLogProgressSnapshot {
        term: 1,
        index: 36,
        node_id: 1,
    });
    assert!(node.report().ready());
}

#[test]
fn duplicate_and_wrong_identity_observations_do_not_count_as_redundancy() {
    let mut node = Node::three();
    node.groups.push(healthy(1));
    assert!(!node.report().serving_ready());
    let mut node = Node::three();
    node.groups[1].node_id = 2;
    assert!(!node.report().serving_ready());
}
