//! Lifecycle property test. Interleaves every maintenance command through the
//! public dispatcher from a multi-group cluster and checks that rejections
//! change nothing, that tokens, epochs, phases, prefix floors and replica
//! fences never move backwards, that placement changes only on completion,
//! and that every active operation can still abort, complete or report its
//! block.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use proptest::test_runner::Config as ProptestConfig;
use proptest::test_runner::RngSeed;
use ursula_shard::RaftGroupId;

use super::ExecutorGeneration;
use super::MaintenanceOperation;
use super::MembershipAction;
use super::OperationCommand;
use super::OperationError;
use super::OperationId;
use super::OperationKind;
use super::OperationOutcome;
use super::OperationPhase;
use super::OperationToken;
use super::PendingAction;
use super::PrefixEvidence;
use super::ProcessIdentity;
use super::ProcessState;
use super::ReplicaEvidence;
use super::ReplicaState;
use crate::ControlCommand;
use crate::ControlPlaneState;
use crate::ControlResponse;
use crate::NodeId;
use crate::NodeState;
use crate::ProcessIncarnation;
use crate::ReplicaIdentity;

const NOW: u64 = 1_000_000;
const STALE: u64 = 900_000;
const NODES: [NodeId; 5] = [1, 2, 3, 4, 5];
/// Upper bound on forward steps for one operation over two groups.
const FORWARD_STEPS: usize = 64;

fn pick<T: Copy>(items: &[T], selector: u8) -> Option<T> {
    items.iter().copied().cycle().nth(usize::from(selector))
}

#[derive(Debug, Clone)]
struct Harness {
    state: ControlPlaneState,
    incarnations: u128,
    index: u64,
}

impl Harness {
    /// Five registered nodes, groups 0 on {1, 2, 3} and 1 on {2, 3, 4}, and
    /// an active process and generation-1 replica on every node.
    fn new() -> Self {
        let mut harness = Self {
            state: ControlPlaneState::default(),
            incarnations: 1_000,
            index: 100,
        };
        for node_id in NODES {
            assert_eq!(
                harness.state.apply(ControlCommand::RegisterNode {
                    node_id,
                    client_url: format!("http://node{node_id}:4491"),
                    cluster_url: format!("http://node{node_id}:4492"),
                    labels: BTreeMap::new(),
                    now_ms: NOW,
                }),
                ControlResponse::Ok
            );
        }
        for (group, voters) in [(0, [1, 2, 3]), (1, [2, 3, 4])] {
            assert_eq!(
                harness.state.apply(ControlCommand::SeedPlacement {
                    raft_group_id: RaftGroupId(group),
                    voters: voters.into(),
                    now_ms: NOW,
                }),
                ControlResponse::Ok
            );
        }
        for node_id in NODES {
            let incarnation = ProcessIncarnation::from_bits(u128::from(node_id));
            let response = harness.operation(OperationCommand::ClaimProcess {
                node_id,
                expected_epoch: 0,
                incarnation: incarnation.clone(),
            });
            let ControlResponse::Operation(Ok(OperationOutcome::ProcessClaimed(process))) =
                response
            else {
                panic!("claim {node_id}: {response:?}");
            };
            let response = harness.operation(OperationCommand::RegisterReplica {
                node_id,
                process,
                identity: ReplicaIdentity {
                    generation: 1,
                    incarnation,
                },
            });
            assert!(!response.is_rejected(), "register {node_id}: {response:?}");
        }
        harness
    }

    fn operation(&mut self, command: OperationCommand) -> ControlResponse {
        self.state.apply(ControlCommand::Operation {
            command,
            now_ms: NOW,
        })
    }

    fn active(&self) -> Option<&MaintenanceOperation> {
        self.state.operations.active.as_ref()
    }

    fn fresh_incarnation(&mut self) -> ProcessIncarnation {
        self.incarnations = self.incarnations.checked_add(1).unwrap();
        ProcessIncarnation::from_bits(self.incarnations)
    }

    /// A committed index at or above every recorded prefix floor.
    fn fresh_index(&mut self) -> u64 {
        let floor = self
            .active()
            .and_then(|operation| operation.prefix_floor.values().max().copied())
            .unwrap_or(0);
        self.index = self.index.max(floor).checked_add(1).unwrap();
        self.index
    }

    fn token(&self) -> OperationToken {
        self.active().map_or_else(
            || OperationToken {
                operation_id: OperationId(0),
                generation: ExecutorGeneration(0),
                executor: ProcessIncarnation::from_bits(0),
            },
            |operation| operation.token.clone(),
        )
    }

    /// The current token, or with `selector` 6 one from a future generation.
    fn token_for(&self, selector: u8) -> OperationToken {
        let token = self.token();
        if selector % 7 == 6 {
            OperationToken {
                generation: ExecutorGeneration(token.generation.0.wrapping_add(1)),
                ..token
            }
        } else {
            token
        }
    }

    fn active_process(&self, node_id: NodeId) -> Option<ProcessIdentity> {
        match self.state.operations.processes.get(&node_id) {
            Some(ProcessState::Active(identity)) => Some(identity.clone()),
            Some(ProcessState::Retired { .. }) | None => None,
        }
    }

    fn replica_identity(&self, node_id: NodeId) -> Option<ReplicaIdentity> {
        match self.state.operations.replicas.get(&node_id) {
            Some(ReplicaState::Active { identity, .. }) => Some(identity.clone()),
            Some(ReplicaState::Pending { replacement, .. }) => Some(replacement.clone()),
            Some(ReplicaState::Retired(_)) | None => None,
        }
    }

    fn evidence(
        &mut self,
        group: RaftGroupId,
        voters: BTreeSet<NodeId>,
        fence: Option<(NodeId, ReplicaIdentity)>,
        observed_at_ms: u64,
    ) -> Option<PrefixEvidence> {
        let index = self.fresh_index();
        let operation = self.active()?;
        let replicas = voters
            .iter()
            .map(|node_id| {
                operation.participants.get(node_id).map(|process| {
                    (*node_id, ReplicaEvidence {
                        process: process.clone(),
                        applied_index: index,
                        installed_replica_identities: fence.clone().into_iter().collect(),
                    })
                })
            })
            .collect::<Option<_>>()?;
        Some(PrefixEvidence {
            raft_group_id: group,
            leader: *voters.first()?,
            term: 1,
            committed_index: index,
            voters,
            joint: false,
            replicas,
            observed_at_ms,
        })
    }

    /// One command decoded from three generated bytes. Arguments come from the
    /// current state, so most commands are plausible and some are stale.
    /// Selectors 16 to 19 take the next forward step, so interleavings reach
    /// every phase.
    fn generated(&mut self, op: u8, x: u8, y: u8) -> ControlCommand {
        let node = pick(&NODES, x).unwrap();
        let idle = self.active().is_none();
        // Without an operation, most token commands would only be stale.
        let op = if idle && !matches!(op % 20, 4..=6 | 15) && !y.is_multiple_of(4) {
            0
        } else {
            op % 20
        };
        if op >= 16
            && let Some(command) = self.forward_step()
        {
            return ControlCommand::Operation {
                command,
                now_ms: NOW,
            };
        }
        let command = match op {
            0 => self.generated_begin(node, y),
            1 => self.generated_observe(x, y),
            2 => OperationCommand::RetireSource {
                token: self.token_for(y),
            },
            3 => OperationCommand::Complete {
                token: self.token_for(y),
            },
            4 => {
                let epoch = self
                    .state
                    .operations
                    .processes
                    .get(&node)
                    .map_or(0, ProcessState::epoch);
                OperationCommand::ClaimProcess {
                    node_id: node,
                    expected_epoch: if y.is_multiple_of(4) {
                        epoch.wrapping_add(1)
                    } else {
                        epoch
                    },
                    incarnation: self.fresh_incarnation(),
                }
            }
            5 => OperationCommand::RestartProcess {
                node_id: node,
                previous: self
                    .active_process(node)
                    .unwrap_or_else(|| identity_of(0, 0)),
                incarnation: self.fresh_incarnation(),
                replica: match self.replica_identity(node) {
                    Some(identity) if !y.is_multiple_of(4) => identity,
                    _ => replica_of(9, 9),
                },
            },
            6 => {
                let process = self
                    .active_process(node)
                    .unwrap_or_else(|| identity_of(0, 0));
                let identity = match self.state.operations.replicas.get(&node) {
                    Some(ReplicaState::Retired(_)) => ReplicaIdentity {
                        generation: process.epoch,
                        incarnation: process.incarnation.clone(),
                    },
                    _ if y.is_multiple_of(3) => replica_of(process.epoch, 77),
                    _ => self
                        .replica_identity(node)
                        .unwrap_or_else(|| replica_of(1, 77)),
                };
                OperationCommand::RegisterReplica {
                    node_id: node,
                    process,
                    identity,
                }
            }
            7 => OperationCommand::ActivateReplica {
                token: self.token_for(y),
                node_id: node,
                identity: self
                    .replica_identity(node)
                    .unwrap_or_else(|| replica_of(1, 77)),
            },
            8 => OperationCommand::Abort {
                token: self.token_for(y),
            },
            9 => self.generated_prepare(x, y),
            10 => OperationCommand::MarkActionDispatched {
                token: self.token_for(y),
                sequence: self.pending_sequence(),
            },
            11 => self.generated_finish(y),
            12 => OperationCommand::CancelPreparedAction {
                token: self.token_for(y),
                sequence: self.pending_sequence(),
            },
            13 => {
                let receipt = self
                    .active()
                    .and_then(|operation| operation.pending_action.as_ref())
                    .map(|pending| pending.receipt().clone());
                let candidates = receipt
                    .as_ref()
                    .and_then(|receipt| self.group_members(receipt.group))
                    .unwrap_or_default();
                OperationCommand::ReassignAction {
                    token: self.token_for(y),
                    leader: pick(&candidates, x).unwrap_or(node),
                    drained: receipt.filter(|_| y.is_multiple_of(2)),
                }
            }
            14 => OperationCommand::TakeOver {
                expected: self.token_for(y),
                executor: self.fresh_incarnation(),
            },
            _ => return self.generated_node_command(node, y),
        };
        ControlCommand::Operation {
            command,
            now_ms: NOW,
        }
    }

    fn generated_begin(&mut self, node: NodeId, y: u8) -> OperationCommand {
        let hosted: Vec<RaftGroupId> = self
            .state
            .placements
            .iter()
            .filter(|(_, placement)| placement.voters.contains(&node))
            .map(|(group, _)| *group)
            .collect();
        let kind = match y % 3 {
            0 => OperationKind::MoveReplicas {
                source: node,
                target: pick(&NODES, y / 3).unwrap(),
                groups: if y.is_multiple_of(2) {
                    hosted.iter().copied().collect()
                } else {
                    hosted.first().copied().into_iter().collect()
                },
            },
            1 => OperationKind::RebuildReplica { node_id: node },
            _ => OperationKind::DecommissionNode {
                node_id: node,
                replacements: hosted
                    .iter()
                    .filter_map(|group| {
                        let voters = &self.state.placements[group].voters;
                        let candidates: Vec<NodeId> = NODES
                            .into_iter()
                            .filter(|candidate| !voters.contains(candidate))
                            .collect();
                        pick(&candidates, y / 3).map(|target| (*group, target))
                    })
                    .collect(),
            },
        };
        let mut required: BTreeSet<NodeId> = BTreeSet::from([node]);
        for (group, placement) in &self.state.placements {
            if !placement.voters.contains(&node) {
                continue;
            }
            required.extend(placement.voters.iter().copied());
            match &kind {
                OperationKind::MoveReplicas { target, .. } => {
                    required.insert(*target);
                }
                OperationKind::DecommissionNode { replacements, .. } => {
                    required.extend(replacements.get(group).copied());
                }
                OperationKind::RebuildReplica { .. } => {}
            }
        }
        OperationCommand::Begin {
            kind,
            executor: self.fresh_incarnation(),
            participants: required
                .into_iter()
                .filter_map(|node_id| {
                    self.active_process(node_id)
                        .map(|process| (node_id, process))
                })
                .collect(),
        }
    }

    fn generated_observe(&mut self, x: u8, y: u8) -> OperationCommand {
        let token = self.token_for(y);
        let Some(operation) = self.active().cloned() else {
            return OperationCommand::Observe {
                token,
                evidence: PrefixEvidence {
                    raft_group_id: RaftGroupId(0),
                    leader: 1,
                    term: 1,
                    committed_index: 1,
                    voters: BTreeSet::from([1]),
                    joint: false,
                    replicas: BTreeMap::new(),
                    observed_at_ms: NOW,
                },
            };
        };
        let source = operation.kind.source();
        let shape = |group: RaftGroupId| {
            let previous = operation.previous.get(&group).cloned().unwrap_or_default();
            match y % 5 {
                0 => previous,
                2 => without(&previous, source),
                3 => joining_node(&operation, group)
                    .map_or(previous.clone(), |node_id| without(&previous, node_id)),
                _ => operation.desired.get(&group).cloned().unwrap_or_default(),
            }
        };
        // Prefer a group whose evidence does not show this shape yet, so shapes
        // accumulate across groups as an executor's observations do.
        let groups: Vec<RaftGroupId> = operation.previous.keys().copied().collect();
        let group = groups
            .iter()
            .copied()
            .find(|group| {
                operation
                    .evidence
                    .get(group)
                    .is_none_or(|evidence| evidence.voters != shape(*group))
            })
            .or_else(|| pick(&groups, x))
            .unwrap_or(RaftGroupId(0));
        let voters = shape(group);
        let fence = (y % 5 == 3)
            .then(|| joining_node(&operation, group))
            .flatten()
            .and_then(|node_id| {
                self.replica_identity(node_id)
                    .map(|identity| (node_id, identity))
            });
        let observed_at_ms = if y % 5 == 4 { STALE } else { NOW };
        let evidence = self
            .evidence(group, voters, fence, observed_at_ms)
            .unwrap_or_else(|| PrefixEvidence {
                raft_group_id: group,
                leader: source,
                term: 1,
                committed_index: 1,
                voters: BTreeSet::from([source]),
                joint: true,
                replicas: BTreeMap::new(),
                observed_at_ms: NOW,
            });
        OperationCommand::Observe { token, evidence }
    }

    fn generated_prepare(&mut self, x: u8, y: u8) -> OperationCommand {
        let token = self.token_for(y);
        let operation = self.active().cloned();
        let groups: Vec<RaftGroupId> = operation
            .as_ref()
            .map(|operation| operation.previous.keys().copied().collect())
            .unwrap_or_default();
        let group = pick(&groups, y).unwrap_or(RaftGroupId(0));
        let joining = operation
            .as_ref()
            .and_then(|operation| joining_node(operation, group))
            .unwrap_or(1);
        let candidates = self.group_members(group).unwrap_or_default();
        let action = match x % 5 {
            0 => MembershipAction::PrepareReplica,
            1 => MembershipAction::AddLearner { node_id: joining },
            2 => MembershipAction::ChangeVoters,
            3 => MembershipAction::RetireReplica,
            _ => MembershipAction::InstallReplicaIdentity {
                node_id: joining,
                identity: self
                    .replica_identity(joining)
                    .unwrap_or_else(|| replica_of(1, 77)),
            },
        };
        OperationCommand::PrepareAction {
            token,
            group,
            leader: pick(&candidates, x / 5).unwrap_or(1),
            action,
        }
    }

    fn generated_finish(&mut self, y: u8) -> OperationCommand {
        let token = self.token_for(y);
        let sequence = self.pending_sequence();
        let fence_group = self
            .active()
            .and_then(|operation| match &operation.pending_action {
                Some(PendingAction::OutcomeUnknown(receipt))
                    if matches!(
                        receipt.action,
                        MembershipAction::InstallReplicaIdentity { .. }
                    ) =>
                {
                    Some(receipt.group)
                }
                _ => None,
            });
        match fence_group {
            Some(group) => OperationCommand::FinishReplicaFence {
                token,
                sequence,
                committed_index: self
                    .active()
                    .and_then(|operation| operation.evidence.get(&group))
                    .map_or(1, |evidence| evidence.committed_index),
            },
            None => OperationCommand::FinishAction { token, sequence },
        }
    }

    fn generated_node_command(&mut self, node: NodeId, y: u8) -> ControlCommand {
        if y.is_multiple_of(2) {
            let state = pick(
                &[NodeState::Active, NodeState::Draining, NodeState::Disabled],
                y / 2,
            )
            .unwrap();
            ControlCommand::SetNodeState {
                node_id: node,
                state,
                now_ms: NOW,
            }
        } else {
            let labels = self
                .state
                .nodes
                .get(&node)
                .map(|registered| registered.labels.clone())
                .unwrap_or_default();
            ControlCommand::RegisterNode {
                node_id: node,
                client_url: format!("http://node{node}-{y}:4491"),
                cluster_url: format!("http://node{node}-{y}:4492"),
                labels,
                now_ms: NOW,
            }
        }
    }

    fn pending_sequence(&self) -> super::ActionSequence {
        self.active()
            .and_then(|operation| operation.pending_action.as_ref())
            .map_or(super::ActionSequence(0), |pending| {
                pending.receipt().sequence
            })
    }

    fn group_members(&self, group: RaftGroupId) -> Option<Vec<NodeId>> {
        let operation = self.active()?;
        let mut members = operation.previous.get(&group)?.clone();
        members.extend(operation.desired.get(&group)?.iter().copied());
        Some(members.into_iter().collect())
    }

    /// The next command an executor would issue if every participant stays
    /// healthy and the data plane does what it is asked.
    fn forward_step(&mut self) -> Option<OperationCommand> {
        let operation = self.active()?.clone();
        let token = operation.token.clone();
        if let Some(pending) = &operation.pending_action {
            return Some(match pending {
                PendingAction::Prepared(receipt) => {
                    if self
                        .state
                        .operations
                        .accepts_process(receipt.leader, &receipt.process)
                    {
                        OperationCommand::MarkActionDispatched {
                            token,
                            sequence: receipt.sequence,
                        }
                    } else {
                        OperationCommand::CancelPreparedAction {
                            token,
                            sequence: receipt.sequence,
                        }
                    }
                }
                PendingAction::OutcomeUnknown(receipt) => match &receipt.action {
                    MembershipAction::InstallReplicaIdentity { node_id, identity } => {
                        let survivors = without(&operation.previous[&receipt.group], *node_id);
                        match operation.evidence.get(&receipt.group) {
                            Some(evidence)
                                if evidence.voters == survivors
                                    && evidence.replicas.values().all(|replica| {
                                        replica.installed_replica_identities.get(node_id)
                                            == Some(identity)
                                    }) =>
                            {
                                OperationCommand::FinishReplicaFence {
                                    token,
                                    sequence: receipt.sequence,
                                    committed_index: evidence.committed_index,
                                }
                            }
                            _ => OperationCommand::Observe {
                                token,
                                evidence: self.evidence(
                                    receipt.group,
                                    survivors,
                                    Some((*node_id, identity.clone())),
                                    NOW,
                                )?,
                            },
                        }
                    }
                    _ => OperationCommand::FinishAction {
                        token,
                        sequence: receipt.sequence,
                    },
                },
            });
        }
        let source = operation.kind.source();
        match (&operation.kind, operation.phase) {
            (OperationKind::RebuildReplica { .. }, OperationPhase::Retired) => {
                self.forward_rebuilt_replica(&operation)
            }
            (OperationKind::RebuildReplica { .. }, phase) => {
                if phase == OperationPhase::Preparing {
                    return self
                        .prepare_membership_change(&operation, MembershipAction::RetireReplica);
                }
                let wanted = operation
                    .previous
                    .iter()
                    .map(|(group, voters)| (*group, without(voters, source)))
                    .collect();
                self.observe_or(&operation, &wanted, OperationCommand::RetireSource {
                    token,
                })
            }
            (_, phase) => {
                for group in operation.desired.keys() {
                    let joining = joining_node(&operation, *group)?;
                    if !installed(&self.state, joining, *group) {
                        return self.prepare_install(&operation, *group, joining);
                    }
                }
                if phase == OperationPhase::Preparing && !operation.previous.is_empty() {
                    return self
                        .prepare_membership_change(&operation, MembershipAction::ChangeVoters);
                }
                let next = if matches!(operation.kind, OperationKind::MoveReplicas { .. })
                    || phase == OperationPhase::Retired
                {
                    OperationCommand::Complete { token }
                } else {
                    OperationCommand::RetireSource { token }
                };
                self.observe_or(&operation, &operation.desired, next)
            }
        }
    }

    fn forward_rebuilt_replica(
        &mut self,
        operation: &MaintenanceOperation,
    ) -> Option<OperationCommand> {
        let token = operation.token.clone();
        let source = operation.kind.source();
        let process = self.state.operations.processes.get(&source)?.clone();
        let ProcessState::Active(process) = process else {
            return Some(OperationCommand::ClaimProcess {
                node_id: source,
                expected_epoch: process.epoch(),
                incarnation: self.fresh_incarnation(),
            });
        };
        match self.state.operations.replicas.get(&source)?.clone() {
            ReplicaState::Retired(_) => Some(OperationCommand::RegisterReplica {
                node_id: source,
                identity: ReplicaIdentity {
                    generation: process.epoch,
                    incarnation: process.incarnation.clone(),
                },
                process,
            }),
            ReplicaState::Pending {
                replacement,
                installed_groups,
                ..
            } => {
                if let Some(group) = operation
                    .previous
                    .keys()
                    .find(|group| !installed_groups.contains_key(group))
                {
                    return self.prepare_install(operation, *group, source);
                }
                Some(OperationCommand::ActivateReplica {
                    token,
                    node_id: source,
                    identity: replacement,
                })
            }
            ReplicaState::Active { .. } => {
                self.observe_or(operation, &operation.desired, OperationCommand::Complete {
                    token,
                })
            }
        }
    }

    /// Prepares the operation's voter change on its first group, the point of
    /// no return, as an executor does once its replicas are ready.
    fn prepare_membership_change(
        &self,
        operation: &MaintenanceOperation,
        action: MembershipAction,
    ) -> Option<OperationCommand> {
        let (group, voters) = operation.previous.iter().next()?;
        let leader = voters.iter().copied().find(|leader| {
            *leader != operation.kind.source()
                && operation
                    .participants
                    .get(leader)
                    .is_some_and(|process| self.state.operations.accepts_process(*leader, process))
        })?;
        Some(OperationCommand::PrepareAction {
            token: operation.token.clone(),
            group: *group,
            leader,
            action,
        })
    }

    fn prepare_install(
        &self,
        operation: &MaintenanceOperation,
        group: RaftGroupId,
        node_id: NodeId,
    ) -> Option<OperationCommand> {
        let leader = operation
            .previous
            .get(&group)?
            .iter()
            .copied()
            .find(|leader| {
                *leader != node_id
                    && operation.participants.get(leader).is_some_and(|process| {
                        self.state.operations.accepts_process(*leader, process)
                    })
            })?;
        Some(OperationCommand::PrepareAction {
            token: operation.token.clone(),
            group,
            leader,
            action: MembershipAction::InstallReplicaIdentity {
                node_id,
                identity: self.replica_identity(node_id)?,
            },
        })
    }

    /// Observes the first group whose recorded evidence does not show the
    /// wanted voters, or returns `next` once every group does.
    fn observe_or(
        &mut self,
        operation: &MaintenanceOperation,
        wanted: &BTreeMap<RaftGroupId, BTreeSet<NodeId>>,
        next: OperationCommand,
    ) -> Option<OperationCommand> {
        for (group, voters) in wanted {
            if operation
                .evidence
                .get(group)
                .is_none_or(|evidence| &evidence.voters != voters)
            {
                return Some(OperationCommand::Observe {
                    token: operation.token.clone(),
                    evidence: self.evidence(*group, voters.clone(), None, NOW)?,
                });
            }
        }
        Some(next)
    }

    /// Drives the active operation to completion with valid commands.
    fn complete_forward(&mut self) -> Result<(), String> {
        for _ in 0..FORWARD_STEPS {
            if self.active().is_none() {
                return Ok(());
            }
            let operation = self.active().cloned();
            let command = self
                .forward_step()
                .ok_or_else(|| format!("no forward step from {operation:?}"))?;
            let response = self.operation(command.clone());
            if response.is_rejected() {
                return Err(format!(
                    "forward step {command:?} was rejected with {response:?} in {operation:?}"
                ));
            }
        }
        Err(format!("did not complete: {:?}", self.active()))
    }
}

fn identity_of(epoch: u64, bits: u128) -> ProcessIdentity {
    ProcessIdentity {
        epoch,
        incarnation: ProcessIncarnation::from_bits(bits),
    }
}

fn replica_of(generation: u64, bits: u128) -> ReplicaIdentity {
    ReplicaIdentity {
        generation,
        incarnation: ProcessIncarnation::from_bits(bits),
    }
}

fn without(voters: &BTreeSet<NodeId>, node_id: NodeId) -> BTreeSet<NodeId> {
    voters
        .iter()
        .copied()
        .filter(|voter| *voter != node_id)
        .collect()
}

/// The node that gains a replica of `group`.
fn joining_node(operation: &MaintenanceOperation, group: RaftGroupId) -> Option<NodeId> {
    match &operation.kind {
        OperationKind::MoveReplicas { target, .. } => Some(*target),
        OperationKind::RebuildReplica { node_id } => Some(*node_id),
        OperationKind::DecommissionNode { replacements, .. } => replacements.get(&group).copied(),
    }
}

fn installed(state: &ControlPlaneState, node_id: NodeId, group: RaftGroupId) -> bool {
    matches!(
        state.operations.replicas.get(&node_id),
        Some(ReplicaState::Active { installed_groups, .. })
            if installed_groups.get(&group).is_some_and(|index| *index > 0)
    )
}

fn phase_rank(phase: OperationPhase) -> u8 {
    match phase {
        OperationPhase::Preparing => 0,
        OperationPhase::Reconfiguring { .. } => 1,
        OperationPhase::Retired => 2,
    }
}

fn replica_fences(state: &ReplicaState) -> Option<(&ReplicaIdentity, &BTreeMap<RaftGroupId, u64>)> {
    match state {
        ReplicaState::Active {
            identity,
            installed_groups,
        } => Some((identity, installed_groups)),
        ReplicaState::Pending {
            replacement,
            installed_groups,
            ..
        } => Some((replacement, installed_groups)),
        ReplicaState::Retired(_) => None,
    }
}

fn replica_generation(state: &ReplicaState) -> u64 {
    match state {
        ReplicaState::Active { identity, .. } => identity.generation,
        ReplicaState::Pending { replacement, .. } => replacement.generation,
        ReplicaState::Retired(identity) => identity.generation,
    }
}

/// Whether the same operation lost progress: an older token or action
/// sequence, an earlier phase, a new point of no return, a cleared block or a
/// lower prefix floor.
fn moved_backwards(operation: &MaintenanceOperation, next: &MaintenanceOperation) -> bool {
    next.token.generation < operation.token.generation
        || next.last_action_sequence < operation.last_action_sequence
        || phase_rank(next.phase) < phase_rank(operation.phase)
        || matches!(
            (operation.phase, next.phase),
            (
                OperationPhase::Reconfiguring { since },
                OperationPhase::Reconfiguring { since: next_since }
            ) if since != next_since
        )
        || !operation
            .blocked
            .keys()
            .all(|node_id| next.blocked.contains_key(node_id))
        || operation
            .prefix_floor
            .iter()
            .any(|(group, floor)| next.prefix_floor.get(group).is_none_or(|next| next < floor))
}

/// Safety invariants of one transition.
fn check_transition(
    before: &ControlPlaneState,
    after: &ControlPlaneState,
    response: &ControlResponse,
) -> Result<(), String> {
    if response.is_rejected() {
        return (before == after)
            .then_some(())
            .ok_or_else(|| format!("rejection {response:?} changed the state"));
    }
    if after.operations.last_operation_id < before.operations.last_operation_id {
        return Err("operation id moved backwards".into());
    }
    for (node_id, process) in &before.operations.processes {
        if after
            .operations
            .processes
            .get(node_id)
            .is_none_or(|next| next.epoch() < process.epoch())
        {
            return Err(format!("process epoch of node {node_id} moved backwards"));
        }
    }
    for (node_id, replica) in &before.operations.replicas {
        let next = after
            .operations
            .replicas
            .get(node_id)
            .ok_or_else(|| format!("replica of node {node_id} disappeared"))?;
        if replica_generation(next) < replica_generation(replica) {
            return Err(format!(
                "replica generation of node {node_id} moved backwards"
            ));
        }
        if let (Some((identity, fences)), Some((next_identity, next_fences))) =
            (replica_fences(replica), replica_fences(next))
            && identity == next_identity
            && fences
                .iter()
                .any(|(group, index)| next_fences.get(group).is_none_or(|next| next < index))
        {
            return Err(format!("installed fence of node {node_id} moved backwards"));
        }
    }
    for (node_id, node) in &before.nodes {
        if node.state == NodeState::Removed && after.nodes[node_id].state != NodeState::Removed {
            return Err(format!("removed node {node_id} came back"));
        }
    }
    if let (Some(operation), Some(next)) = (
        before.operations.active.as_ref(),
        after.operations.active.as_ref(),
    ) && operation.token.operation_id == next.token.operation_id
        && moved_backwards(operation, next)
    {
        return Err(format!(
            "operation moved backwards: {operation:?} -> {next:?}"
        ));
    }
    let completed = matches!(
        response,
        ControlResponse::Operation(Ok(OperationOutcome::Completed))
    );
    for (group, placement) in &before.placements {
        let next = &after.placements[group];
        let desired = before
            .operations
            .active
            .as_ref()
            .and_then(|operation| operation.desired.get(group))
            .filter(|_| completed);
        let expected_epoch = placement
            .epoch
            .checked_add(u64::from(desired.is_some()))
            .unwrap();
        if next.epoch != expected_epoch || &next.voters != desired.unwrap_or(&placement.voters) {
            return Err(format!("placement of {group:?} changed outside completion"));
        }
    }
    Ok(())
}

/// Every active operation can abort while `Preparing`, completes forward when
/// nothing blocks it, and reports its block otherwise.
fn check_liveness(harness: &Harness) -> Result<(), String> {
    let Some(operation) = harness.active() else {
        return Ok(());
    };
    let token = operation.token.clone();
    if operation.phase == OperationPhase::Preparing {
        let mut aborted = harness.clone();
        let response = aborted.operation(OperationCommand::Abort {
            token: token.clone(),
        });
        if response != ControlResponse::Operation(Ok(OperationOutcome::Aborted))
            || aborted.active().is_some()
            || aborted.state.placements != harness.state.placements
        {
            return Err(format!("a preparing operation did not abort: {response:?}"));
        }
    }
    if operation.blocked.is_empty() {
        check_retirement_shortcut(harness)?;
        harness.clone().complete_forward()
    } else {
        let mut blocked = harness.clone();
        match blocked.operation(OperationCommand::Complete { token }) {
            ControlResponse::Operation(Err(OperationError::Blocked { .. })) => Ok(()),
            response => Err(format!("a blocked operation answered {response:?}")),
        }
    }
}

/// Retirement accepted on evidence alone, as when the data plane already
/// shows the target voters, must leave the operation completable.
fn check_retirement_shortcut(harness: &Harness) -> Result<(), String> {
    let mut probe = harness.clone();
    let Some(operation) = probe.active().cloned() else {
        return Ok(());
    };
    if operation.pending_action.is_some() || !operation.phase.before_retirement() {
        return Ok(());
    }
    let source = operation.kind.source();
    let wanted: BTreeMap<RaftGroupId, BTreeSet<NodeId>> = match operation.kind {
        OperationKind::RebuildReplica { .. } => operation
            .previous
            .iter()
            .map(|(group, voters)| (*group, without(voters, source)))
            .collect(),
        OperationKind::MoveReplicas { .. } | OperationKind::DecommissionNode { .. } => {
            operation.desired.clone()
        }
    };
    for (group, voters) in wanted {
        let Some(evidence) = probe.evidence(group, voters, None, NOW) else {
            return Ok(());
        };
        probe.operation(OperationCommand::Observe {
            token: operation.token.clone(),
            evidence,
        });
    }
    let response = probe.operation(OperationCommand::RetireSource {
        token: operation.token.clone(),
    });
    if response.is_rejected() {
        return Ok(());
    }
    probe
        .complete_forward()
        .map_err(|error| format!("after an evidence-only retirement: {error}"))
}

proptest::proptest! {
    #![proptest_config(ProptestConfig {
        cases: 128,
        rng_seed: RngSeed::Fixed(442),
        // The seed is fixed, so a failure reproduces without a regression file.
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn interleaved_operations_stay_safe_and_can_always_finish(
        steps in proptest::collection::vec((0_u8..20, proptest::num::u8::ANY, proptest::num::u8::ANY), 1..64)
    ) {
        let mut harness = Harness::new();
        for (op, x, y) in steps {
            let command = harness.generated(op, x, y);
            let before = harness.state.clone();
            let response = harness.state.apply(command.clone());
            if let Err(violation) = check_transition(&before, &harness.state, &response) {
                proptest::prop_assert!(false, "{command:?}: {violation}");
            }
            if let Err(violation) = check_liveness(&harness) {
                proptest::prop_assert!(false, "after {command:?}: {violation}");
            }
        }
        let encoded = serde_json::to_vec(&harness.state).unwrap();
        let restored: ControlPlaneState = serde_json::from_slice(&encoded).unwrap();
        proptest::prop_assert_eq!(harness.state, restored);
    }
}
