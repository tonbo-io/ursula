use std::collections::BTreeMap;
use std::collections::BTreeSet;

use ursula_shard::RaftGroupId;

use crate::ClusterBootstrap;
use crate::ClusterId;
use crate::ClusterIdentity;
use crate::ControlCommand;
use crate::ControlPlaneState;
use crate::ControlResponse;
use crate::GroupPolicyOverride;
use crate::MembershipLogId;
use crate::NodeRegistration;
use crate::NodeState;
use crate::PlacementPolicy;
use crate::ReplicationFactor;
use crate::RoutingHashVersion;
use crate::VerifiedGroupMembership;

fn node(id: u64) -> NodeRegistration {
    NodeRegistration {
        node_id: id,
        client_url: format!("http://node{id}:4437"),
        cluster_url: format!("http://node{id}:4439"),
        admin_url: format!("http://node{id}:4438"),
        labels: BTreeMap::from([("zone".to_owned(), ((id - 1) % 3).to_string())]),
    }
}

fn recipe(meta_count: u64) -> ClusterBootstrap {
    ClusterBootstrap {
        identity: ClusterIdentity {
            cluster_id: ClusterId::try_from("bootstrap-test".to_owned()).unwrap(),
            group_count: 2,
            core_count: 2,
            routing_hash: RoutingHashVersion::Fnv1a64BucketSlashStreamV1,
        },
        initial_meta_voters: (1..=meta_count).collect(),
        nodes: (1..=5).map(|id| (id, node(id))).collect(),
        voters: BTreeMap::from([
            (RaftGroupId(0), BTreeSet::from([1, 2, 3])),
            (RaftGroupId(1), BTreeSet::from([1, 2, 3, 4, 5])),
        ]),
        placement: PlacementPolicy {
            group_overrides: vec![GroupPolicyOverride {
                raft_group_id: RaftGroupId(1),
                replication_factor: ReplicationFactor::Five,
            }],
            ..PlacementPolicy::default()
        },
    }
}

fn evidence(recipe: &ClusterBootstrap) -> BTreeMap<RaftGroupId, VerifiedGroupMembership> {
    recipe
        .voters
        .iter()
        .map(|(id, voters)| {
            (*id, VerifiedGroupMembership {
                voters: voters.clone(),
                learners: BTreeSet::new(),
                log_id: MembershipLogId {
                    term: 1,
                    node_id: 1,
                    index: u64::from(id.0) + 1,
                },
            })
        })
        .collect()
}

fn command(recipe: ClusterBootstrap) -> ControlCommand {
    ControlCommand::BootstrapCluster {
        memberships: evidence(&recipe),
        bootstrap: recipe,
        now_ms: 1,
    }
}

fn reject_without_changes(state: &mut ControlPlaneState, command: ControlCommand) {
    let before = state.clone();
    assert!(state.apply(command).is_rejected());
    assert_eq!(*state, before);
}

#[test]
fn bootstrap_is_atomic_persists_routing_directory_and_independent_meta_voters() {
    for meta_count in [3, 5] {
        let bootstrap = recipe(meta_count);
        let mut state = ControlPlaneState::default();
        assert_eq!(state.apply(command(bootstrap.clone())), ControlResponse::Ok);
        assert_eq!(state.config.initial_meta_voters.len(), meta_count as usize);
        assert_eq!(state.config.default_replication_factor, 3);
        assert_eq!(state.placements[&RaftGroupId(0)].voters.len(), 3);
        assert_eq!(state.placements[&RaftGroupId(1)].voters.len(), 5);
        assert_eq!(state.cluster_bootstrap.as_ref().unwrap().recipe, bootstrap);
        assert_eq!(
            state.nodes[&1].admin_url.as_deref(),
            Some("http://node1:4438")
        );
        assert_eq!(
            state.placement_view(RaftGroupId(0)).unwrap().nodes[&1].admin_url,
            state.nodes[&1].admin_url
        );
        let bytes = rmp_serde::to_vec_named(&state).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<ControlPlaneState>(&bytes).unwrap(),
            state
        );
        let bytes = serde_json::to_vec(&state).unwrap();
        assert_eq!(
            serde_json::from_slice::<ControlPlaneState>(&bytes).unwrap(),
            state
        );
        state.apply(ControlCommand::SetNodeState {
            node_id: 1,
            state: NodeState::Draining,
            now_ms: 2,
        });
        let before = state.clone();
        assert_eq!(state.apply(command(bootstrap)), ControlResponse::Ok);
        assert_eq!(
            state, before,
            "bootstrap replay never resets live node state"
        );
    }
}

#[test]
fn bootstrap_rejects_unverified_inconsistent_or_non_uniform_memberships_without_partial_state() {
    let original = recipe(3);
    let mut bad = evidence(&original);
    bad.remove(&RaftGroupId(1));
    reject_without_changes(
        &mut ControlPlaneState::default(),
        ControlCommand::BootstrapCluster {
            bootstrap: original.clone(),
            memberships: bad,
            now_ms: 1,
        },
    );
    for mutation in 0..3 {
        let mut bad = evidence(&original);
        let group = bad.get_mut(&RaftGroupId(0)).unwrap();
        match mutation {
            0 => {
                group.voters.remove(&3);
            }
            1 => {
                group.learners.insert(4);
            }
            _ => {
                group.log_id.node_id = 0;
            }
        }
        reject_without_changes(
            &mut ControlPlaneState::default(),
            ControlCommand::BootstrapCluster {
                bootstrap: original.clone(),
                memberships: bad,
                now_ms: 1,
            },
        );
    }
    for meta_count in [0, 1, 2, 4, 6] {
        reject_without_changes(
            &mut ControlPlaneState::default(),
            command(recipe(meta_count)),
        );
    }
    let mut bad = original.clone();
    bad.nodes.get_mut(&3).unwrap().labels = bad.nodes[&1].labels.clone();
    reject_without_changes(&mut ControlPlaneState::default(), command(bad));
    let mut bad = original;
    bad.nodes.remove(&2);
    reject_without_changes(&mut ControlPlaneState::default(), command(bad));
}

#[test]
fn immutable_bootstrap_rejects_identity_routing_rf_and_directory_drift() {
    let original = recipe(3);
    let mut state = ControlPlaneState::default();
    state.apply(command(original.clone()));
    for mutation in 0..6 {
        let mut changed = original.clone();
        match mutation {
            0 => {
                changed.identity.cluster_id =
                    ClusterId::try_from("other-cluster".to_owned()).unwrap()
            }
            1 => changed.identity.group_count = 3,
            2 => changed.identity.core_count = 4,
            3 => {
                changed.initial_meta_voters = (1..=5).collect();
            }
            4 => changed.placement.default_replication_factor = ReplicationFactor::Five,
            _ => changed.nodes.get_mut(&1).unwrap().admin_url = "http://other:4438".to_owned(),
        }
        reject_without_changes(&mut state, command(changed));
    }
    let mut nonempty = ControlPlaneState::default();
    nonempty.apply(ControlCommand::SeedPlacement {
        raft_group_id: RaftGroupId(0),
        voters: BTreeSet::from([1, 2, 3]),
        now_ms: 1,
    });
    reject_without_changes(&mut nonempty, command(original));
}

#[test]
fn managed_registration_requires_trusted_origins_and_preserves_node_identity() {
    let original = recipe(3);
    let mut state = ControlPlaneState::default();
    state.apply(command(original));
    let mut new = node(6);
    assert_eq!(
        state.apply(ControlCommand::RegisterManagedNode {
            node: new.clone(),
            now_ms: 2
        }),
        ControlResponse::Ok
    );
    new.admin_url.push('/');
    assert_eq!(
        state.apply(ControlCommand::RegisterManagedNode {
            node: new.clone(),
            now_ms: 3
        }),
        ControlResponse::Ok
    );
    new.admin_url = "http://different:4438".to_owned();
    reject_without_changes(&mut state, ControlCommand::RegisterManagedNode {
        node: new,
        now_ms: 4,
    });
    reject_without_changes(&mut state, ControlCommand::RegisterNode {
        node_id: 7,
        client_url: "http://node7:4437".to_owned(),
        cluster_url: "http://node7:4439".to_owned(),
        labels: BTreeMap::new(),
        now_ms: 2,
    });
    let mut collision = node(7);
    collision.cluster_url = "HTTP://NODE1:4439/".to_owned();
    reject_without_changes(&mut state, ControlCommand::RegisterManagedNode {
        node: collision,
        now_ms: 2,
    });
}

#[test]
fn trusted_directory_rejects_endpoint_aliases_and_non_origin_urls() {
    let mut duplicate = recipe(3);
    duplicate.nodes.get_mut(&2).unwrap().admin_url = "HTTP://NODE1:4439/".to_owned();
    reject_without_changes(&mut ControlPlaneState::default(), command(duplicate));
    for invalid in [
        "",
        "node:4439",
        "http://user:secret@node1:4439",
        "http://node1:4439/path",
        "http://node1:4439/?x=1",
        "http://node1:4439/#x",
        "ftp://node1:4439",
        " http://node1:4439",
    ] {
        let mut bad = recipe(3);
        bad.nodes.get_mut(&1).unwrap().cluster_url = invalid.to_owned();
        reject_without_changes(&mut ControlPlaneState::default(), command(bad));
    }
    for bad_id in ["", "cluster/id", "节点", &"x".repeat(129)] {
        assert!(
            serde_json::from_str::<ClusterId>(&serde_json::to_string(bad_id).unwrap()).is_err()
        );
    }
    assert!(
        serde_json::from_str::<RoutingHashVersion>("\"fnv1a64_bucket_slash_stream_v2\"").is_err()
    );
}
