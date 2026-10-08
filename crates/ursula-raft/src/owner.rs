//! Owner-runtime mailbox for protocol, read and administrative Raft calls.
//! Only the mailbox worker owns a raw handle; remote callers keep metrics and
//! a sender. Jobs are polled concurrently so a waiting write cannot prevent
//! the AppendEntries or vote that lets it complete.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use openraft::BasicNode;
use openraft::RaftMetrics;
use openraft::error::ClientWriteError;
use openraft::error::Fatal;
use openraft::error::InitializeError;
use openraft::error::RaftError;
use openraft::raft::ClientWriteResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::TransferLeaderRequest;
use openraft::rt::WatchReceiver;
use openraft::type_config::alias::SnapshotOf;
use openraft::type_config::alias::WatchReceiverOf;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupWriteCommand;

use crate::registry::RaftGroupHandle;
use crate::rt::sync::mpsc;
use crate::rt::sync::oneshot;
use crate::state_machine::RaftGroupStateMachine;
use crate::types::UrsulaAppendEntriesRequest;
use crate::types::UrsulaAppendEntriesResponse;
use crate::types::UrsulaRaftTypeConfig as C;
use crate::types::UrsulaVote;
use crate::types::UrsulaVoteRequest;
use crate::types::UrsulaVoteResponse;

type Job = Box<dyn FnOnce(RaftGroupHandle) -> BoxFuture<'static, ()> + Send>;

struct Mailbox {
    sender: mpsc::UnboundedSender<Job>,
    task: crate::rt::task::JoinHandle<()>,
}

impl Drop for Mailbox {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A cloneable command endpoint, with no cross-thread raw Raft handle.
#[derive(Clone)]
pub struct OwnerRaftHandle {
    mailbox: Arc<Mailbox>,
    metrics: WatchReceiverOf<C, RaftMetrics<C>>,
}

impl std::fmt::Debug for OwnerRaftHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnerRaftHandle").finish_non_exhaustive()
    }
}

impl OwnerRaftHandle {
    pub(crate) fn new(raft: RaftGroupHandle) -> Self {
        let metrics = raft.metrics();
        let (sender, mut receiver) = mpsc::unbounded_channel::<Job>();
        let task = crate::rt::spawn(async move {
            let mut jobs = FuturesUnordered::new();
            loop {
                crate::rt::select! {
                    biased;
                    _ = jobs.next(), if !jobs.is_empty() => {},
                    job = receiver.recv() => match job {
                        Some(job) => jobs.push(job(raft.clone())),
                        None => break,
                    },
                }
            }
            while jobs.next().await.is_some() {}
        });
        Self {
            mailbox: Arc::new(Mailbox { sender, task }),
            metrics,
        }
    }

    pub(crate) async fn call<F, Fut, R>(&self, operation: F) -> Result<R, Fatal<C>>
    where
        F: FnOnce(RaftGroupHandle) -> Fut + Send + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: Send + 'static,
    {
        let (mut reply, response) = oneshot::channel();
        self.mailbox
            .sender
            .send(Box::new(move |raft| {
                Box::pin(async move {
                    crate::rt::select! {
                        biased;
                        _ = reply.closed() => {},
                        result = operation(raft) => {
                            if reply.send(result).is_err() {
                                tracing::trace!("owner Raft caller stopped waiting");
                            }
                        },
                    }
                })
            }))
            .map_err(|_closed| Fatal::Stopped)?;
        response.await.map_err(|_closed| Fatal::Stopped)
    }

    pub fn metrics(&self) -> WatchReceiverOf<C, RaftMetrics<C>> {
        self.metrics.clone()
    }

    pub fn wait(&self, timeout: Option<Duration>) -> openraft::metrics::Wait<C> {
        openraft::metrics::Wait {
            timeout: timeout.unwrap_or(Duration::from_secs(86400)),
            rx: self.metrics(),
        }
    }

    pub fn is_leader(&self) -> bool {
        let metrics = self.metrics.borrow_watched();
        metrics.state == openraft::ServerState::Leader
    }

    pub async fn current_leader(&self) -> Option<u64> {
        self.metrics.borrow_watched().current_leader
    }

    pub fn trigger(&self) -> OwnerTrigger {
        OwnerTrigger(self.clone())
    }

    pub(crate) fn elect(&self, enabled: impl FnOnce() -> bool + Send + 'static) {
        if self
            .mailbox
            .sender
            .send(Box::new(move |raft| {
                // Set before returning the job future, preserving mailbox order.
                raft.runtime_config().elect(enabled());
                Box::pin(async {})
            }))
            .is_err()
        {
            tracing::debug!("owner Raft stopped before election policy update");
        }
    }

    pub async fn append_entries(
        &self,
        request: UrsulaAppendEntriesRequest,
    ) -> Result<UrsulaAppendEntriesResponse, RaftError<C>> {
        self.call(move |raft| async move { raft.append_entries(request).await })
            .await?
    }
    pub async fn vote(
        &self,
        request: UrsulaVoteRequest,
    ) -> Result<UrsulaVoteResponse, RaftError<C>> {
        self.call(move |raft| async move { raft.vote(request).await })
            .await?
    }
    pub async fn install_full_snapshot(
        &self,
        vote: UrsulaVote,
        snapshot: SnapshotOf<C>,
    ) -> Result<SnapshotResponse<C>, Fatal<C>> {
        self.call(move |raft| async move { raft.install_full_snapshot(vote, snapshot).await })
            .await?
    }
    pub async fn handle_transfer_leader(
        &self,
        request: TransferLeaderRequest<C>,
    ) -> Result<openraft::raft::TransferLeaderResponse<C>, Fatal<C>> {
        self.call(move |raft| async move { raft.handle_transfer_leader(request).await })
            .await?
    }
    pub async fn get_snapshot(&self) -> Result<Option<SnapshotOf<C>>, RaftError<C>> {
        self.call(move |raft| async move { raft.get_snapshot().await })
            .await?
    }
    pub async fn client_write(
        &self,
        command: GroupWriteCommand,
    ) -> Result<ClientWriteResponse<C>, RaftError<C, ClientWriteError<C>>> {
        self.call(move |raft| async move { raft.client_write(command).await })
            .await?
    }
    pub async fn add_learner(
        &self,
        id: u64,
        node: BasicNode,
        blocking: bool,
    ) -> Result<ClientWriteResponse<C>, RaftError<C, ClientWriteError<C>>> {
        self.call(move |raft| async move { raft.add_learner(id, node, blocking).await })
            .await?
    }
    pub async fn change_membership(
        &self,
        members: BTreeSet<u64>,
        retain: bool,
    ) -> Result<ClientWriteResponse<C>, RaftError<C, ClientWriteError<C>>> {
        self.call(move |raft| async move { raft.change_membership(members, retain).await })
            .await?
    }
    pub async fn initialize(
        &self,
        members: BTreeMap<u64, BasicNode>,
    ) -> Result<(), RaftError<C, InitializeError<C>>> {
        self.call(move |raft| async move { raft.initialize(members).await })
            .await?
    }
    /// Update the timer on its owner and wait for submission.
    pub async fn set_tick(&self, enabled: bool) -> Result<(), Fatal<C>> {
        self.call(move |raft| async move {
            raft.runtime_config().tick(enabled);
        })
        .await
    }

    /// Submit a read-only Raft-state callback on the owner without waiting
    /// for the callback itself. Useful for queue barriers and diagnostics.
    pub async fn external_request<F>(&self, request: F) -> Result<(), Fatal<C>>
    where F: FnOnce(&openraft::RaftState<C>) + Send + 'static {
        self.call(move |raft| async move { raft.external_request(request).await })
            .await?
    }

    pub async fn with_raft_state<F, R>(&self, f: F) -> Result<R, Fatal<C>>
    where
        F: FnOnce(&openraft::RaftState<C>) -> R + Send + 'static,
        R: Send + 'static,
    {
        self.call(move |raft| async move { raft.with_raft_state(f).await })
            .await?
    }
    pub async fn with_state_machine<F, R>(&self, f: F) -> Result<R, Fatal<C>>
    where
        F: FnOnce(&mut RaftGroupStateMachine) -> BoxFuture<'_, R> + Send + 'static,
        R: Send + 'static,
    {
        self.call(move |raft| async move { raft.with_state_machine(f).await })
            .await?
    }
    pub async fn shutdown(&self) -> Result<(), GroupEngineError> {
        self.call(move |raft| async move { raft.shutdown().await.map_err(owner_stopped) })
            .await
            .map_err(owner_stopped)?
    }
}

/// Administrative triggers execute on the owner, including their task creation.
pub struct OwnerTrigger(OwnerRaftHandle);

macro_rules! trigger {
    ($name:ident ( $($arg:ident : $ty:ty),* )) => {
        pub async fn $name(&self, $($arg: $ty),*) -> Result<(), Fatal<C>> {
            self.0.call(move |raft| async move { raft.trigger().$name($($arg),*).await }).await?
        }
    };
}
impl OwnerTrigger {
    trigger!(snapshot());
    trigger!(elect(pre_vote: bool));
    trigger!(heartbeat());
    trigger!(purge_log(upto: u64));
    trigger!(transfer_leader(to: u64));
    pub async fn allow_next_revert(
        &self,
        target: &u64,
        allow: bool,
    ) -> Result<Result<(), openraft::error::AllowNextRevertError<C>>, Fatal<C>> {
        let target = *target;
        self.0
            .call(move |raft| async move { raft.trigger().allow_next_revert(&target, allow).await })
            .await?
    }
}

/// Keep the transport classification structured while recording the original fatal cause.
pub(crate) fn owner_stopped(error: impl std::fmt::Display) -> GroupEngineError {
    tracing::debug!(%error, "owner Raft operation stopped");
    GroupEngineError::Infra(ursula_runtime::GroupInfraError::OwnerStopped)
}

pub(crate) fn owner_raft_error(error: RaftError<C>) -> GroupEngineError {
    match error {
        RaftError::Fatal(error) => owner_stopped(error),
        RaftError::APIError(impossible) => match impossible {},
    }
}
