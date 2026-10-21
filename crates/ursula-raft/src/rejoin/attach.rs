//! Recovery driver wiring, transport seam and task ownership.

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use openraft::rt::WatchReceiver;
use ursula_control::ControlCommand;
use ursula_control::ControlResponse;
use ursula_control::SeedRoleOf;

use super::GroupRejoin;
use super::PeerGroupLog;
use super::run_group_bootstrap;
use super::run_rejoin_heal;
use super::run_rejoin_vote_barrier;
use crate::BarrierAnswer;
use crate::GroupNodes;
use crate::PlacementInputs;
use crate::RaftTarget;
use crate::SeedRole;
use crate::meta::MetaForwardError;
use crate::meta::confirm::BarrierFailure;
use crate::meta::genesis::GenesisProbe;
use crate::registry::RaftGroupHandleRegistry;
use crate::replica::ReplicaStateMachine;
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

/// What woke a recovery driver ([`wait_change`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Wake {
    /// The Raft metrics it watches changed.
    Metrics,
    /// The recovery gate changed.
    Gate,
    /// The retry deadline passed.
    Deadline,
}

/// A retry deadline is still needed for unavailable peers and stall reporting.
/// Local state changes wake the driver without waiting for that deadline.
/// `false` once the Raft or the gate is gone.
pub(super) async fn wait_recovery_change<T: Send + Sync>(
    metrics: &mut openraft::type_config::alias::WatchReceiverOf<crate::UrsulaRaftTypeConfig, T>,
    gate: &mut openraft::type_config::alias::WatchReceiverOf<crate::UrsulaRaftTypeConfig, ()>,
    retry: Duration,
) -> bool {
    wait_change(metrics, gate, retry).await.is_some()
}

/// [`wait_recovery_change`], saying what woke the driver, or `None` once the
/// Raft or the gate is gone. Allocates nothing.
pub(super) async fn wait_change<T: Send + Sync>(
    metrics: &mut openraft::type_config::alias::WatchReceiverOf<crate::UrsulaRaftTypeConfig, T>,
    gate: &mut openraft::type_config::alias::WatchReceiverOf<crate::UrsulaRaftTypeConfig, ()>,
    retry: Duration,
) -> Option<Wake> {
    use futures_util::future::Either;
    let metrics = std::pin::pin!(metrics.changed());
    let gate = std::pin::pin!(gate.changed());
    match crate::rt::time::timeout(retry, futures_util::future::select(metrics, gate)).await {
        Ok(Either::Left((result, _))) => result.is_ok().then_some(Wake::Metrics),
        Ok(Either::Right((result, _))) => result.is_ok().then_some(Wake::Gate),
        Err(_elapsed) => Some(Wake::Deadline),
    }
}

/// Transport required by recovery, shared by native and simulated wiring:
/// the one seam through which a replica's gate, the meta runtime and its
/// drivers ask other nodes, over gRPC or in process.
pub trait RecoveryTransport: Clone + Send + Sync + 'static {
    /// Why a barrier failed, which a meta read barrier classifies.
    type Error: fmt::Debug + Send + Into<BarrierFailure>;
    /// Asks `peer` with the bootstrap probe vote, about the transport's own
    /// data group.
    fn probe(&self, peer: u64) -> impl Future<Output = Option<PeerGroupLog>> + Send;
    /// Asks `peer` with this node's genesis probe: what it holds of the
    /// meta group, or the values it claims when they differ.
    fn genesis_probe(
        &self,
        peer: u64,
        probe: &GenesisProbe,
    ) -> impl Future<Output = Option<PeerGroupLog>> + Send;
    /// Asks `leader` for a fresh quorum barrier of `target`, answered with
    /// the memberships of the leader view it confirmed.
    fn barrier(
        &self,
        leader: u64,
        target: RaftTarget,
    ) -> impl Future<Output = Result<BarrierAnswer, Self::Error>> + Send;
    /// Forwards `command` to the meta leader `leader`, as this node: a
    /// `RegisterNode` naming another node is refused for good, before
    /// anything is proposed.
    fn forward_control(
        &self,
        leader: u64,
        command: ControlCommand,
    ) -> impl Future<Output = Result<ControlResponse, MetaForwardError>> + Send;
}

/// The timings of a replica's recovery drivers. What they ask and whom
/// they initialize comes from the group's placement inputs
/// ([`crate::PlacementInputs`]).
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    pub interval: Duration,
    pub barrier_timeout: Duration,
    pub stall_after: Duration,
    pub bootstrap_interval: Duration,
    pub bootstrap_warn_after: Duration,
}

/// How a replica's gate drivers ask a peer whether it holds the group: the
/// data plane's bootstrap probe vote, or another plane's probe.
pub(crate) type GateProbe = Arc<
    dyn Fn(u64) -> futures_util::future::BoxFuture<'static, Option<PeerGroupLog>> + Send + Sync,
>;

impl RecoveryGate {
    /// Publishes a data replica's recovery gate, then spawns its heal driver
    /// and its gate drivers (`spawn_gate_drivers`) with the
    /// transport's bootstrap probe vote, over the seed role and the node
    /// sets `inputs` gives the replica's group.
    pub fn attach<T: RecoveryTransport>(
        &self,
        engine: &crate::RaftGroupEngine,
        rejoin: Arc<GroupRejoin>,
        registry: &RaftGroupHandleRegistry,
        inputs: &dyn PlacementInputs,
        transport: T,
        config: RecoveryConfig,
    ) -> Result<(), ursula_runtime::GroupEngineError> {
        let group = engine.placement.raft_group_id;
        let nodes = inputs.nodes(group);
        engine.publish_recovery(rejoin.clone(), registry)?;
        self.push(crate::rt::spawn(run_rejoin_heal(
            engine.raft_handle(),
            rejoin.clone(),
            nodes.clone(),
            config.interval,
            registry.follower_log_reversion_counter(),
        )));
        let probe_transport = transport.clone();
        let probe: GateProbe = Arc::new(move |peer| {
            let transport = probe_transport.clone();
            Box::pin(async move { transport.probe(peer).await })
        });
        self.spawn_gate_drivers(
            engine.raft_handle(),
            engine.read_barrier.owner().clone(),
            rejoin,
            registry.election_policy(),
            nodes,
            inputs.seed_role(group),
            probe,
            transport,
            config,
        );
        Ok(())
    }

    /// Spawns the drivers that open one replica's recovery gate, for a
    /// replica of any state machine: the barrier driver, which asks the
    /// candidates of `nodes` for a leader's barrier, the genesis vote floor
    /// task, which records the floor once every other candidate answers
    /// `probe` empty and `seed` confirms a role, and, while `seed` shows the
    /// replica as its group's initializer, the bootstrap driver, which
    /// initializes the voters of `nodes` once every other candidate does.
    /// Each reads `nodes` at each use.
    ///
    /// Each driver starts once the node owes no predecessor wait, so a
    /// barrier round, a vote floor and an initialization all come after the
    /// predecessor stopped.
    pub(crate) fn spawn_gate_drivers<SM, T>(
        &self,
        raft: openraft::Raft<crate::UrsulaRaftTypeConfig, SM>,
        owner: crate::owner::OwnerRaftHandle<SM>,
        rejoin: Arc<GroupRejoin>,
        election: crate::ElectionPolicy,
        nodes: Arc<dyn GroupNodes>,
        seed: Arc<dyn SeedRole>,
        probe: GateProbe,
        transport: T,
        config: RecoveryConfig,
    ) where
        SM: ReplicaStateMachine,
        T: RecoveryTransport,
    {
        rejoin.set_stall_after(config.stall_after);
        let target = rejoin.group;
        let fence = owner.fence().clone();
        let barrier = run_rejoin_vote_barrier(
            owner,
            rejoin.clone(),
            election,
            nodes.clone(),
            move |leader| {
                let transport = transport.clone();
                async move { transport.barrier(leader, target).await }
            },
            config.barrier_timeout,
            config.interval,
            config.stall_after,
        );
        let waited = fence.clone();
        self.push(crate::rt::spawn(async move {
            waited.waited().await;
            barrier.await;
        }));
        if rejoin.needs_vote_floor() && !rejoin.holds_group_history() {
            let rejoin = rejoin.clone();
            let nodes = nodes.clone();
            let seed = seed.clone();
            let probe = probe.clone();
            let fence = fence.clone();
            self.push(crate::rt::spawn(async move {
                fence.waited().await;
                while rejoin.needs_vote_floor() && !rejoin.holds_group_history() {
                    let mut all_empty = true;
                    for peer in &nodes.candidates() {
                        if *peer != rejoin.node_id
                            && !matches!(
                                probe(*peer).await,
                                Some(PeerGroupLog::Empty | PeerGroupLog::Unprepared)
                            )
                        {
                            all_empty = false;
                            break;
                        }
                    }
                    if all_empty {
                        // The floor is written only while a confirmed
                        // placement gives the replica a role in its group.
                        match seed.confirmed_role().await {
                            Ok(Some(_role)) => {}
                            Ok(None) => return,
                            Err(error) => {
                                tracing::debug!(%error, "genesis vote floor: no confirmed seed role yet, retrying");
                                crate::rt::time::sleep(config.bootstrap_interval).await;
                                continue;
                            }
                        }
                        if let Err(error) = rejoin.establish_vote_floor(UrsulaVote::new(0, 0)).await
                        {
                            // Journal I/O failures poison/stop the writer. Never retry
                            // bootstrap after them or publish admission without fsync.
                            tracing::error!(%error, "genesis vote persistence failed; leaving recovery closed");
                        }
                        return;
                    }
                    crate::rt::time::sleep(config.bootstrap_interval).await;
                }
            }));
        }
        if seed.local_role() == Some(SeedRoleOf::Initializer) && !rejoin.holds_group_history() {
            self.push(crate::rt::spawn(async move {
                fence.waited().await;
                run_group_bootstrap(
                    rejoin.node_id,
                    raft,
                    rejoin,
                    &*nodes,
                    &*seed,
                    &*probe,
                    config.bootstrap_interval,
                    config.bootstrap_warn_after,
                )
                .await;
            }));
        }
    }
}
