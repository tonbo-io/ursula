//! Recovery driver wiring, transport seam and task ownership.

use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use openraft::rt::WatchReceiver;

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

/// Transport required by recovery, shared by native and simulated wiring.
pub trait RecoveryTransport: Clone + Send + Sync + 'static {
    type Error: fmt::Debug + Send;
    /// Asks `peer` with the bootstrap probe vote.
    fn probe(&self, peer: u64) -> impl Future<Output = Option<PeerGroupLog>> + Send;
    /// Asks `leader` for a fresh quorum barrier.
    fn barrier(
        &self,
        leader: u64,
    ) -> impl Future<Output = Result<(UrsulaVote, u64), Self::Error>> + Send;
}

#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    pub initialize: bool,
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
    /// transport's bootstrap probe vote.
    pub fn attach<T: RecoveryTransport>(
        &self,
        engine: &crate::RaftGroupEngine,
        rejoin: Arc<GroupRejoin>,
        registry: &RaftGroupHandleRegistry,
        nodes: BTreeSet<u64>,
        transport: T,
        config: RecoveryConfig,
    ) -> Result<(), ursula_runtime::GroupEngineError> {
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
            nodes.clone(),
            nodes,
            probe,
            transport,
            config,
        );
        Ok(())
    }

    /// Spawns the drivers that open one replica's recovery gate, for a
    /// replica of any state machine: the barrier driver, which asks
    /// `candidates` for a leader's barrier, the genesis vote floor task,
    /// which records the floor once every other candidate answers `probe`
    /// empty, and, for an initializer, the bootstrap driver, which
    /// initializes `members` once every other candidate does.
    pub(crate) fn spawn_gate_drivers<SM, T>(
        &self,
        raft: openraft::Raft<crate::UrsulaRaftTypeConfig, SM>,
        owner: crate::owner::OwnerRaftHandle<SM>,
        rejoin: Arc<GroupRejoin>,
        election: crate::ElectionPolicy,
        candidates: BTreeSet<u64>,
        members: BTreeSet<u64>,
        probe: GateProbe,
        transport: T,
        config: RecoveryConfig,
    ) where
        SM: openraft::storage::RaftStateMachine<
                crate::UrsulaRaftTypeConfig,
                SnapshotData = crate::types::SnapshotData,
            >,
        T: RecoveryTransport,
    {
        rejoin.set_stall_after(config.stall_after);
        self.push(crate::rt::spawn(run_rejoin_vote_barrier(
            owner,
            rejoin.clone(),
            election,
            candidates.clone(),
            move |leader| {
                let transport = transport.clone();
                async move { transport.barrier(leader).await }
            },
            config.barrier_timeout,
            config.interval,
            config.stall_after,
        )));
        if rejoin.needs_vote_floor() && !rejoin.holds_group_history() {
            let rejoin = rejoin.clone();
            let candidates = candidates.clone();
            let probe = probe.clone();
            self.push(crate::rt::spawn(async move {
                while rejoin.needs_vote_floor() && !rejoin.holds_group_history() {
                    let mut all_empty = true;
                    for peer in &candidates {
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
        if config.initialize && !rejoin.holds_group_history() {
            self.push(crate::rt::spawn(async move {
                run_group_bootstrap(
                    rejoin.node_id,
                    raft,
                    rejoin,
                    candidates,
                    members,
                    &*probe,
                    config.bootstrap_interval,
                    config.bootstrap_warn_after,
                )
                .await;
            }));
        }
    }
}
