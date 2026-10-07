//! Recovery driver wiring, transport seam and task ownership.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use openraft::rt::WatchReceiver;
use openraft::rt::WatchSender;
use openraft::vote::RaftLeaderId;

use super::GroupRejoin;
use super::PeerGroupLog;
use super::run_group_bootstrap;
use super::run_rejoin_heal;
use super::run_rejoin_vote_barrier;
use crate::registry::RaftGroupHandleRegistry;
use crate::types::UrsulaVote;

#[cfg(madsim)]
type RecoveryTask = madsim::task::JoinHandle<()>;
#[cfg(not(madsim))]
type RecoveryTask = tokio::task::JoinHandle<()>;

/// Background recovery work belongs to the engine that opened the group.
/// Drop cancels it even when startup fails before normal shutdown.
#[derive(Debug, Default)]
pub struct RecoveryGate {
    handles: std::sync::Mutex<Vec<RecoveryTask>>,
}

impl RecoveryGate {
    pub(crate) fn push(&self, handle: RecoveryTask) {
        self.handles
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(handle);
    }

    pub(crate) async fn shutdown(&self) {
        let handles = std::mem::take(
            &mut *self
                .handles
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()),
        );
        for handle in &handles {
            handle.abort();
        }
        for handle in handles {
            if let Err(error) = handle.await
                && !error.is_cancelled()
            {
                tracing::error!(%error, "recovery task failed");
            }
        }
    }
}

impl Drop for RecoveryGate {
    fn drop(&mut self) {
        for handle in self
            .handles
            .get_mut()
            .unwrap_or_else(|poison| poison.into_inner())
        {
            handle.abort();
        }
    }
}

#[cfg(all(test, not(madsim)))]
mod recovery_task_tests {
    use super::RecoveryGate;

    #[derive(Clone)]
    struct Votes {
        responders: std::collections::BTreeSet<u64>,
        unknown: std::collections::BTreeSet<u64>,
        mutate: Option<crate::MetaRaftHandle>,
    }
    impl super::RecoveryTransport for Votes {
        type Error = ();
        async fn probe(&self, _peer: u64, _address: String) -> Option<super::PeerGroupLog> {
            None
        }
        async fn vote(&self, peer: u64, _address: String) -> Option<crate::UrsulaVoteResponse> {
            if peer == 2
                && let Some(meta) = &self.mutate
            {
                meta.write(ursula_control::ControlCommand::Operation {
                    command: ursula_control::OperationCommand::ClaimProcess {
                        node_id: 2,
                        expected_epoch: 1,
                        incarnation: ursula_proto::admin::ProcessIncarnation::from_bits(222),
                    },
                    now_ms: 2,
                })
                .await
                .expect("commit the concurrent process claim");
            }
            self.responders.contains(&peer).then(|| {
                crate::UrsulaVoteResponse::new(
                    if self.unknown.contains(&peer) {
                        crate::UrsulaVote::new(0, 0)
                    } else {
                        crate::UrsulaVote::new_committed(5, 2)
                    },
                    None,
                    false,
                )
            })
        }
        async fn barrier(
            &self,
            _leader: u64,
            _address: String,
        ) -> Result<(crate::UrsulaVote, u64), ()> {
            Err(())
        }
    }

    #[tokio::test]
    async fn fresh_meta_authority_covers_pending_voter_sets_and_rejects_changes_during_sampling() {
        use std::collections::BTreeMap;
        use std::collections::BTreeSet;
        use std::sync::Arc;
        use std::time::Duration;

        use ursula_control::ControlCommand;
        use ursula_control::ControlResponse;
        use ursula_control::OperationCommand;
        use ursula_control::OperationOutcome;
        let root = tempfile::tempdir().unwrap();
        let meta = crate::MetaRaftHandle::new_durable(
            1,
            root.path().to_owned(),
            Arc::new(openraft::Config::default()),
        )
        .await
        .unwrap();
        meta.initialize_membership(BTreeMap::from([(1, openraft::BasicNode::new("local"))]))
            .await
            .unwrap();
        meta.wait_for_current_leader(1, Duration::from_secs(3))
            .await
            .unwrap();
        let mut identities = BTreeMap::new();
        for node in 1..=4 {
            meta.register_node(
                crate::MetaNodeRegistration::new(
                    node,
                    format!("http://node-{node}"),
                    format!("http://node-{node}"),
                ),
                0,
            )
            .await
            .unwrap();
            let response = meta
                .write(ControlCommand::Operation {
                    command: OperationCommand::ClaimProcess {
                        node_id: node,
                        expected_epoch: 0,
                        incarnation: ursula_proto::admin::ProcessIncarnation::from_bits(
                            u128::from(node),
                        ),
                    },
                    now_ms: 0,
                })
                .await
                .unwrap();
            let ControlResponse::Operation(Ok(OperationOutcome::ProcessClaimed(identity))) =
                response
            else {
                panic!("claim rejected")
            };
            identities.insert(node, identity);
        }
        let group = ursula_shard::RaftGroupId(0);
        meta.write(ControlCommand::SeedPlacement {
            raft_group_id: group,
            voters: BTreeSet::from([1, 2, 3]),
            now_ms: 0,
        })
        .await
        .unwrap();
        let registry = crate::RaftGroupHandleRegistry::default();
        registry.set_process_authority(1, identities[&1].clone(), meta.clone());
        let timeout = Duration::from_secs(2);
        let old = Votes {
            responders: BTreeSet::from([2, 3]),
            unknown: BTreeSet::new(),
            mutate: None,
        };
        assert!(
            super::authoritative_vote_floor(&registry, &old, 1, group, timeout)
                .await
                .is_some(),
            "fresh meta authority breaks the data ReadIndex recovery circularity"
        );
        let wiped_peer = Votes {
            unknown: BTreeSet::from([3]),
            ..old.clone()
        };
        assert!(
            super::authoritative_vote_floor(&registry, &wiped_peer, 1, group, timeout)
                .await
                .is_none(),
            "a completely wiped peer cannot attest to its historical votes"
        );
        let changing = Votes {
            mutate: Some(meta.clone()),
            ..old.clone()
        };
        assert!(
            super::authoritative_vote_floor(&registry, &changing, 1, group, timeout)
                .await
                .is_none(),
            "a process change across samples invalidates authority"
        );
        let state = meta.read_linearizable_state().await.unwrap();
        let participants = state
            .operations
            .processes
            .iter()
            .map(|(id, process)| match process {
                ursula_control::ProcessState::Active(identity) => (*id, identity.clone()),
                _ => panic!("active fixture"),
            })
            .collect();
        let response = meta
            .write(ControlCommand::Operation {
                command: OperationCommand::Begin {
                    kind: ursula_control::OperationKind::MoveReplicas {
                        source: 3,
                        target: 4,
                        groups: BTreeSet::from([group]),
                    },
                    executor: identities[&1].incarnation.clone(),
                    participants,
                    meta_voters: BTreeSet::from([1, 2, 3]),
                },
                now_ms: 3,
            })
            .await
            .unwrap();
        assert!(!response.is_rejected(), "operation begin: {response:?}");
        assert!(
            super::authoritative_vote_floor(&registry, &old, 1, group, timeout)
                .await
                .is_none(),
            "old voters do not intersect the desired config"
        );
        let joint = Votes {
            responders: BTreeSet::from([2, 3, 4]),
            unknown: BTreeSet::new(),
            mutate: None,
        };
        assert!(
            super::authoritative_vote_floor(&registry, &joint, 1, group, timeout)
                .await
                .is_some(),
            "samples intersect every possible current/joint config"
        );
        meta.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_joins_cancelled_recovery_work_and_drop_aborts_it() {
        for explicit_shutdown in [true, false] {
            let tasks = RecoveryGate::default();
            let (started, running) = tokio::sync::oneshot::channel();
            let (held, released) = tokio::sync::oneshot::channel::<()>();
            tasks.push(tokio::spawn(async move {
                let _held = held;
                started.send(()).unwrap();
                std::future::pending::<()>().await;
            }));
            running.await.unwrap();
            if explicit_shutdown {
                tasks.shutdown().await;
            }
            drop(tasks);
            assert!(matches!(
                tokio::time::timeout(std::time::Duration::from_secs(1), released).await,
                Ok(Err(tokio::sync::oneshot::error::RecvError { .. }))
            ));
        }
    }
}

/// A retry deadline is still needed for unavailable peers and stall reporting.
/// Local state changes wake the driver without waiting for that deadline.
pub(super) async fn wait_recovery_change<T: Send + Sync>(
    metrics: &mut openraft::type_config::alias::WatchReceiverOf<crate::UrsulaRaftTypeConfig, T>,
    gate: &mut openraft::type_config::alias::WatchReceiverOf<crate::UrsulaRaftTypeConfig, ()>,
    retry: Duration,
) -> bool {
    use futures_util::future::Either;
    let change =
        futures_util::future::select(Box::pin(metrics.changed()), Box::pin(gate.changed()));
    match crate::rt::time::timeout(retry, change).await {
        Ok(Either::Left((result, _))) => result.is_ok(),
        Ok(Either::Right((result, _))) => result.is_ok(),
        Err(_) => true,
    }
}

/// Transport required by recovery, shared by native and simulated wiring.
pub trait RecoveryTransport: Clone + Send + Sync + 'static {
    type Error: fmt::Debug + Send;
    fn probe(
        &self,
        peer: u64,
        address: String,
    ) -> impl Future<Output = Option<PeerGroupLog>> + Send;
    fn vote(
        &self,
        peer: u64,
        address: String,
    ) -> impl Future<Output = Option<crate::UrsulaVoteResponse>> + Send;
    fn barrier(
        &self,
        leader: u64,
        address: String,
    ) -> impl Future<Output = Result<(UrsulaVote, u64), Self::Error>> + Send;
}

/// Authority for data membership when no meta control plane is installed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryMembershipAuthority {
    /// The server must reject every membership mutation in this mode.
    ImmutableStatic,
    /// Unknown or mutable membership requires a fresh data-leader quorum proof.
    CurrentLeader,
}

#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    pub membership_authority: RecoveryMembershipAuthority,
    pub initialize: bool,
    pub interval: Duration,
    pub barrier_timeout: Duration,
    pub stall_after: Duration,
    pub bootstrap_interval: Duration,
    pub bootstrap_warn_after: Duration,
}

fn possible_voter_sets(
    state: &ursula_control::ControlPlaneState,
    group: ursula_shard::RaftGroupId,
) -> Vec<BTreeSet<u64>> {
    let mut sets = Vec::new();
    if let Some(placement) = state.placements.get(&group) {
        sets.push(placement.voters.clone());
    }
    if let Some(operation) = &state.operations.active {
        if let Some(previous) = operation.previous.get(&group) {
            sets.push(previous.clone());
            let mut survivors = previous.clone();
            survivors.remove(&operation.kind.source());
            if !survivors.is_empty() {
                sets.push(survivors);
            }
        }
        if let Some(desired) = operation.desired.get(&group) {
            sets.push(desired.clone());
        }
    }
    sets
}

async fn sampled_vote_floor<T: RecoveryTransport>(
    transport: &T,
    local: u64,
    nodes: &BTreeMap<u64, BasicNode>,
    voter_sets: &[BTreeSet<u64>],
    timeout: Duration,
) -> Option<(UrsulaVote, bool)> {
    if voter_sets.is_empty() || voter_sets.iter().any(BTreeSet::is_empty) {
        return None;
    }
    let responses =
        futures_util::future::join_all(nodes.iter().filter(|(id, _)| **id != local).map(
            |(id, node)| {
                let transport = transport.clone();
                async move {
                    (
                        *id,
                        crate::rt::time::timeout(timeout, transport.vote(*id, node.addr.clone()))
                            .await
                            .ok()
                            .flatten(),
                    )
                }
            },
        ))
        .await
        .into_iter()
        .filter_map(|(id, response)| {
            response
                .filter(|response| {
                    // An empty replacement has forgotten historical votes. It cannot
                    // count as an intersection witness for a previously committed term.
                    response.vote.leader_id().term() > 0
                })
                .map(|response| (id, response))
        })
        .collect::<BTreeMap<_, _>>();
    if !voter_sets.iter().all(|voters| {
        voters
            .iter()
            .filter(|id| responses.contains_key(id))
            .count()
            >= voters.len().saturating_sub(voters.len() / 2)
    }) {
        return None;
    }
    let initialized = responses.values().any(|response| {
        super::PeerGroupLog::from_vote_response(response) == super::PeerGroupLog::Initialized
    });
    responses
        .into_values()
        .map(|response| response.vote)
        .reduce(|left, right| if left >= right { left } else { right })
        .map(|floor| (floor, initialized))
}

async fn authoritative_vote_floor<T: RecoveryTransport>(
    registry: &RaftGroupHandleRegistry,
    transport: &T,
    local: u64,
    group: ursula_shard::RaftGroupId,
    timeout: Duration,
) -> Option<(UrsulaVote, bool)> {
    let (node, identity, meta) = registry.process_authority()?;
    let before = crate::rt::time::timeout(timeout, meta.read_linearizable_state())
        .await
        .ok()?
        .ok()?;
    if !before.operations.accepts_process(node, &identity) {
        return None;
    }
    let sets = possible_voter_sets(&before, group);
    let nodes = sets
        .iter()
        .flatten()
        .filter_map(|id| {
            before
                .nodes
                .get(id)
                .map(|node| (*id, BasicNode::new(node.cluster_url.clone())))
        })
        .collect();
    let floor = sampled_vote_floor(transport, local, &nodes, &sets, timeout).await?;
    let after = crate::rt::time::timeout(timeout, meta.read_linearizable_state())
        .await
        .ok()?
        .ok()?;
    // An operation or process change across sampling invalidates its authority.
    if before.operations != after.operations
        || before.placements.get(&group) != after.placements.get(&group)
    {
        return None;
    }
    Some(floor)
}

impl RecoveryGate {
    pub fn attach<T: RecoveryTransport>(
        &self,
        engine: &crate::RaftGroupEngine,
        rejoin: Arc<GroupRejoin>,
        registry: &RaftGroupHandleRegistry,
        nodes: BTreeMap<u64, BasicNode>,
        transport: T,
        config: RecoveryConfig,
    ) {
        rejoin.set_replica_fence_required_index(engine.replica_fences.required_index());
        rejoin.bind(&engine.raft_handle());
        registry.register_engine(engine, Some(rejoin.clone()));
        let election = registry.election_policy();
        let barrier_transport = transport.clone();
        let floor_rejoin = rejoin.clone();
        let floor_transport = transport.clone();
        let floor_nodes = nodes.clone();
        let floor_registry = registry.clone();
        let mut floor_metrics = engine.raft_handle().metrics();
        let mut floor_changes = rejoin.changes.subscribe();
        self.push(crate::rt::spawn(async move {
            while !floor_rejoin.vote_gate_open() && floor_rejoin.needs_vote_floor() {
                let authoritative = if floor_registry.process_authority().is_some() {
                    authoritative_vote_floor(
                        &floor_registry,
                        &floor_transport,
                        floor_rejoin.node_id,
                        floor_rejoin.raft_group_id,
                        config.barrier_timeout,
                    )
                    .await
                } else if config.membership_authority
                    == RecoveryMembershipAuthority::ImmutableStatic
                {
                    sampled_vote_floor(
                        &floor_transport,
                        floor_rejoin.node_id,
                        &floor_nodes,
                        &[floor_nodes.keys().copied().collect()],
                        config.barrier_timeout,
                    )
                    .await
                } else {
                    None
                };
                if let Some((floor, initialized)) = authoritative {
                    if initialized {
                        floor_rejoin.gate().observe_append(Some(1));
                    }
                    match floor_rejoin.establish_vote_floor(floor).await {
                        Ok(()) => return,
                        Err(error) => {
                            tracing::warn!(%error, "persist authoritative membership vote floor")
                        }
                    }
                }
                // Genesis needs followers to admit the first election before a
                // leader can produce ReadIndex. This exception requires every
                // configured peer to report no initialized history, never a
                // quorum sample. Existing groups use the current-leader proof.
                if (config.membership_authority == RecoveryMembershipAuthority::ImmutableStatic
                    || (floor_registry.genesis_initialization_allowed(floor_rejoin.raft_group_id)
                        && floor_registry.replica_genesis_prefix(floor_rejoin.raft_group_id) == 0))
                    && !floor_rejoin.holds_group_history()
                {
                    let genesis = futures_util::future::join_all(
                        floor_nodes
                            .iter()
                            .filter(|(id, _)| **id != floor_rejoin.node_id)
                            .map(|(id, node)| {
                                let transport = floor_transport.clone();
                                async move {
                                    crate::rt::time::timeout(
                                        config.barrier_timeout,
                                        transport.vote(*id, node.addr.clone()),
                                    )
                                    .await
                                    .ok()
                                    .flatten()
                                }
                            }),
                    )
                    .await;
                    if !genesis.is_empty()
                        && genesis.iter().all(|answer| {
                            answer.as_ref().is_some_and(|response| {
                                super::PeerGroupLog::from_vote_response(response)
                                    == super::PeerGroupLog::Empty
                            })
                        })
                        && let Some(floor) = genesis
                            .into_iter()
                            .flatten()
                            .map(|response| response.vote)
                            .reduce(|left, right| if left >= right { left } else { right })
                    {
                        match floor_rejoin.establish_vote_floor(floor).await {
                            Ok(()) => return,
                            Err(error) => tracing::warn!(%error, "persist genesis vote floor"),
                        }
                    }
                }
                // Static bootstrap voter intersections stop being authoritative
                // after membership changes. Only a fresh current-leader ReadIndex
                // can certify a floor against the current (including joint) quorum.
                let mut candidates = floor_nodes.clone();
                for (id, node) in floor_metrics
                    .borrow_watched()
                    .membership_config
                    .membership()
                    .nodes()
                {
                    candidates.insert(*id, node.clone());
                }
                if let Some(state) = floor_registry.control_state() {
                    for (id, node) in state.nodes {
                        candidates.insert(id, BasicNode::new(node.cluster_url));
                    }
                }
                let proofs = futures_util::future::join_all(
                    candidates
                        .iter()
                        .filter(|(id, _)| **id != floor_rejoin.node_id)
                        .map(|(id, node)| {
                            let transport = floor_transport.clone();
                            async move {
                                crate::rt::time::timeout(
                                    config.barrier_timeout,
                                    transport.barrier(*id, node.addr.clone()),
                                )
                                .await
                                .ok()
                                .and_then(Result::ok)
                            }
                        }),
                )
                .await;
                if let Some((floor, index)) = proofs.into_iter().flatten().max_by(|left, right| {
                    left.0
                        .partial_cmp(&right.0)
                        .unwrap_or(std::cmp::Ordering::Equal)
                }) {
                    if index >= 1 {
                        floor_rejoin.gate().observe_append(Some(index));
                    }
                    match floor_rejoin.establish_vote_floor(floor).await {
                        Ok(()) => {
                            floor_rejoin.confirm_barrier(floor, index);
                            return;
                        }
                        Err(error) => tracing::warn!(%error, "persist recovery vote floor"),
                    }
                }
                if !wait_recovery_change(&mut floor_metrics, &mut floor_changes, config.interval)
                    .await
                {
                    return;
                }
            }
        }));
        self.push(crate::rt::spawn(run_rejoin_heal(
            engine.raft_handle(),
            rejoin.clone(),
            nodes.clone(),
            config.interval,
        )));
        self.push(crate::rt::spawn(run_rejoin_vote_barrier(
            engine.read_barrier.owner().clone(),
            rejoin.clone(),
            election,
            nodes.clone(),
            move |leader, address| {
                let transport = barrier_transport.clone();
                async move { transport.barrier(leader, address).await }
            },
            config.barrier_timeout,
            config.interval,
            config.stall_after,
        )));
        if config.initialize && !rejoin.holds_group_history() {
            let raft = engine.raft_handle();
            self.push(crate::rt::spawn(async move {
                run_group_bootstrap(
                    rejoin.node_id,
                    raft,
                    rejoin,
                    nodes,
                    move |peer, address| {
                        let transport = transport.clone();
                        async move { transport.probe(peer, address).await }
                    },
                    config.bootstrap_interval,
                    config.bootstrap_warn_after,
                )
                .await;
            }));
        }
    }
}
