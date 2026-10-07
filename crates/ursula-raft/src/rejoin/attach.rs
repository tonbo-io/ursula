//! Recovery driver wiring, transport seam and task ownership.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
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
    fn barrier(
        &self,
        leader: u64,
        address: String,
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
        rejoin.bind(&engine.raft_handle());
        registry.register_engine(engine, Some(rejoin.clone()));
        let election = registry.election_policy();
        let barrier_transport = transport.clone();
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
