//! The decommission pre-checks and the removal checks, on states the real
//! kernel built.

use std::collections::BTreeSet;

use reqwest::StatusCode;
use ursula_control::ControlCommand;
use ursula_control::ControlError;
use ursula_control::ControlPlaneState;
use ursula_control::LifecycleChange;
use ursula_control::LifecycleResponse;
use ursula_control::PlacementApiError;
use ursula_control::PlacementApiReason;
use ursula_control::samples::meta as meta_id;
use ursula_control::samples::node;
use ursula_control::samples::node_set;
use ursula_control::samples::params;
use ursula_control::samples::register;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeLifecycle;
use ursula_proto::placement::WriteEffect;

use super::DecommissionPass;
use super::LifecycleRefusal;
use super::Removal;
use super::RemovalRefusal;
use super::WriteAnswer;
use super::answered;
use super::check_removal;
use super::pending_changes;
use super::plan_decommission;

/// Six nodes, meta voters 1 to 3, four groups at three voters, all seeded,
/// so each of nodes 4 to 6 holds a role. Node 7 registers after Bootstrap,
/// as a scale-up node does, and holds nothing.
struct Meta {
    state: ControlPlaneState,
    index: u64,
}

impl Meta {
    fn new() -> Self {
        let mut meta = Self {
            state: ControlPlaneState::default(),
            index: 0,
        };
        let nodes = [1, 2, 3, 4, 5, 6];
        for id in nodes {
            meta.apply(register(id, 10));
        }
        meta.apply(ControlCommand::Bootstrap {
            params: params(&nodes, 4, 3, 4),
            nodes: node_set(&nodes),
            now_ms: 20,
        });
        meta.apply(register(7, 25));
        meta
    }

    fn apply(&mut self, command: ControlCommand) {
        self.index = self.index.checked_add(1).unwrap();
        self.state
            .apply(meta_id(1, self.index), command)
            .expect("the kernel takes it");
    }

    fn change(&mut self, id: u64, change: LifecycleChange) {
        self.apply(ControlCommand::SetNodeLifecycle {
            node: node(id),
            change,
            now_ms: 30,
        });
    }

    fn lifecycle(&self, id: u64) -> NodeLifecycle {
        self.state
            .cluster()
            .unwrap()
            .node(node(id))
            .unwrap()
            .lifecycle
    }

    /// A node that is no meta voter, with a role in some group or none.
    fn other(&self, with_role: bool) -> u64 {
        let cluster = self.state.cluster().unwrap();
        (4..=7)
            .find(|id| cluster.has_any_role(node(*id)) == with_role)
            .expect("the seeds and node 7 leave such a node")
    }
}

fn ids(values: &[u64]) -> BTreeSet<NodeId> {
    values.iter().copied().map(node).collect()
}

/// Catches a decommission that changes some nodes before it meets a meta
/// voter or an unknown node among the named ones: the kernel's own check
/// refuses the pass before anything is sent.
#[test]
fn a_meta_voter_or_an_unknown_node_refuses_the_whole_decommission() {
    let meta = Meta::new();
    let leaving = meta.other(true);
    assert_eq!(
        plan_decommission(&meta.state, &ids(&[2, leaving])),
        Err(LifecycleRefusal::MetaVoter { node: node(2) })
    );
    assert_eq!(
        plan_decommission(&meta.state, &ids(&[leaving, 9])),
        Err(LifecycleRefusal::Kernel {
            node: node(9),
            error: ControlError::UnknownNode { node: node(9) },
        })
    );
    assert_eq!(
        plan_decommission(&ControlPlaneState::default(), &ids(&[leaving])),
        Err(LifecycleRefusal::Kernel {
            node: node(leaving),
            error: ControlError::NotBootstrapped,
        })
    );
}

/// Catches a decommission that lets a node leave while another named node
/// still takes groups: every active node is cordoned before any is
/// decommissioned, and a removed node is done.
#[test]
fn every_named_node_is_cordoned_before_any_is_decommissioned() {
    let mut meta = Meta::new();
    let with_role = meta.other(true);
    let roleless = meta.other(false);
    let named = ids(&[with_role, roleless]);
    meta.change(roleless, LifecycleChange::Cordon);
    assert_eq!(
        plan_decommission(&meta.state, &named),
        Ok(DecommissionPass::Cordon(ids(&[with_role])))
    );
    meta.change(with_role, LifecycleChange::Cordon);
    assert_eq!(
        plan_decommission(&meta.state, &named),
        Ok(DecommissionPass::Decommission(named.clone()))
    );
    meta.change(with_role, LifecycleChange::Decommission);
    meta.change(roleless, LifecycleChange::Decommission);
    // The roleless node left at its decommission, the other keeps its role.
    assert_eq!(meta.lifecycle(roleless), NodeLifecycle::Removed);
    assert_eq!(meta.lifecycle(with_role), NodeLifecycle::Decommissioning);
    assert_eq!(
        plan_decommission(&meta.state, &named),
        Ok(DecommissionPass::Leaving(ids(&[with_role])))
    );
    assert_eq!(
        plan_decommission(&meta.state, &ids(&[roleless])),
        Ok(DecommissionPass::Leaving(BTreeSet::new()))
    );
}

/// Catches a cordon or uncordon sent again to a node already there, or
/// sent where the kernel refuses it.
#[test]
fn cordon_and_uncordon_send_only_what_the_kernel_takes() {
    let mut meta = Meta::new();
    let with_role = meta.other(true);
    let roleless = meta.other(false);
    meta.change(with_role, LifecycleChange::Cordon);
    let pending = pending_changes(&meta.state, &ids(&[1, with_role]), LifecycleChange::Cordon);
    assert_eq!(pending, Ok(ids(&[1])));
    meta.change(with_role, LifecycleChange::Decommission);
    assert_eq!(
        pending_changes(&meta.state, &ids(&[with_role]), LifecycleChange::Cordon),
        Err(LifecycleRefusal::Kernel {
            node: node(with_role),
            error: ControlError::Decommissioning {
                node: node(with_role)
            },
        })
    );
    assert_eq!(
        pending_changes(&meta.state, &ids(&[with_role]), LifecycleChange::Uncordon),
        Ok(ids(&[with_role]))
    );
    meta.change(roleless, LifecycleChange::Decommission);
    assert_eq!(
        pending_changes(&meta.state, &ids(&[roleless]), LifecycleChange::Uncordon),
        Err(LifecycleRefusal::Kernel {
            node: node(roleless),
            error: ControlError::NodeRemoved {
                node: node(roleless)
            },
        })
    );
}

/// Catches a scale-down that drops a node still holding a role, and a
/// scale-up that starts a removed id.
#[test]
fn removal_checks_read_each_nodes_lifecycle() {
    let mut meta = Meta::new();
    let with_role = meta.other(true);
    let roleless = meta.other(false);
    meta.change(roleless, LifecycleChange::Decommission);
    meta.change(with_role, LifecycleChange::Decommission);
    let cluster = meta.state.cluster();
    // 9 never registered: it holds nothing.
    assert_eq!(
        check_removal(cluster, &ids(&[roleless, 9]), Removal::Removed),
        Ok(())
    );
    assert_eq!(
        check_removal(cluster, &ids(&[roleless, with_role]), Removal::Removed),
        Err(RemovalRefusal::NotRemoved {
            node: node(with_role),
            lifecycle: NodeLifecycle::Decommissioning,
        })
    );
    assert_eq!(
        check_removal(cluster, &ids(&[with_role, 9]), Removal::NotRemoved),
        Ok(())
    );
    assert_eq!(
        check_removal(cluster, &ids(&[roleless]), Removal::NotRemoved),
        Err(RemovalRefusal::Removed {
            node: node(roleless)
        })
    );
    assert_eq!(
        check_removal(None, &ids(&[5]), Removal::Removed),
        Err(RemovalRefusal::NotBootstrapped)
    );
    assert_eq!(check_removal(None, &ids(&[5]), Removal::NotRemoved), Ok(()));
}

/// Catches a refusal that may pass on a later view ending the command, and
/// a permanent one decided again forever.
#[test]
fn an_answer_classifies_by_the_kernels_requeue() {
    let refused = |error: ControlError, write_effect| WriteAnswer::Refused {
        status: StatusCode::CONFLICT,
        refusal: Box::new(PlacementApiError {
            reason: PlacementApiReason::Refused(error),
            write_effect,
            current: None,
            message: String::new(),
        }),
    };
    let four = node(4);
    assert_eq!(
        answered(
            1,
            four,
            WriteAnswer::Applied(LifecycleResponse {
                lifecycle: NodeLifecycle::Removed,
            })
        ),
        Ok(Some(NodeLifecycle::Removed))
    );
    assert_eq!(
        answered(
            1,
            four,
            refused(
                ControlError::MetaVoter { node: four },
                WriteEffect::NotApplied
            )
        ),
        Err(LifecycleRefusal::MetaVoter { node: four })
    );
    for (error, effect) in [
        (
            ControlError::MetaVoter { node: four },
            WriteEffect::MayHaveApplied,
        ),
        (
            ControlError::UnknownNode { node: four },
            WriteEffect::NotApplied,
        ),
        (ControlError::NotBootstrapped, WriteEffect::NotApplied),
    ] {
        assert_eq!(answered(1, four, refused(error, effect)), Ok(None));
    }
    let lost = WriteAnswer::Lost(anyhow::anyhow!("connection reset"));
    assert_eq!(answered(1, four, lost), Ok(None));
}
