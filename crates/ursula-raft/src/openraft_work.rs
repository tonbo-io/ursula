//! The work OpenRaft runs for one group engine on tasks of its own (#709).
//!
//! `Raft::shutdown` joins RaftCore and its tick task only. The state-machine
//! worker then finishes the commands it already received (applies, a full
//! snapshot install, a snapshot build, external functions) and stops once
//! its channel closes. Each snapshot build runs detached, each replication
//! task holds a log reader until it sees its cancellation, and heartbeat,
//! vote and snapshot-sending tasks hold network clients. All of that work
//! reaches OpenRaft through objects the engine handed it: the state machine
//! with the snapshot builders it creates, the log store with the readers it
//! hands out, and the network factory with the clients it creates. The
//! snapshot installs the engine admits outside OpenRaft join them
//! ([`WorkAdmission`]). Each of them holds a [`WorkToken`], so the engine
//! knows the work ended once every token is gone, and [`OpenRaftWork::stop`]
//! waits for that. A shutdown that returns leaves nothing of the engine
//! running: no state change, no gauge update, no snapshot store or reference
//! write, no RPC, and no handle on the group's log store, which may open
//! again at once. A shutdown that gave up keeps the group closed: the WAL's
//! [`EngineWorkLedger`] refuses to open it until that work ended.

use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::io;
use std::ops::RangeBounds;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::sync::PoisonError;
use std::time::Duration;

use futures_util::Stream;
use openraft::OptionalSend;
use openraft::RaftNetworkFactory;
use openraft::RaftNetworkV2;
use openraft::RaftTypeConfig;
use openraft::alias::EntryOf;
use openraft::alias::LeaderIdOf;
use openraft::alias::LogIdOf;
use openraft::alias::SnapshotOf;
use openraft::alias::VoteOf;
use openraft::base::BoxFuture;
use openraft::base::BoxStream;
use openraft::error::RPCError;
use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::error::Unreachable;
use openraft::network::Backoff;
use openraft::network::RPCOption;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::StreamAppendResult;
use openraft::raft::TransferLeaderRequest;
use openraft::raft::TransferLeaderResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::storage::IOFlushed;
use openraft::storage::LeaderBoundedStreamResult;
use openraft::storage::LogState;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use ursula_runtime::GroupInfraError;
use ursula_shard::RaftGroupId;

use crate::rt::sync::watch;
use crate::types::UrsulaRaftTypeConfig;

type NodeIdOf = <UrsulaRaftTypeConfig as RaftTypeConfig>::NodeId;
type NodeOf = <UrsulaRaftTypeConfig as RaftTypeConfig>::Node;

/// How long a group engine's shutdown waits for the work OpenRaft runs for
/// it. That work only finishes what it already started: a snapshot build
/// stops its upload, pin and housekeeping, an install its download and
/// publication, a network client its RPC, and nothing waits on another
/// group. Work still running after this is stuck.
///
/// It is longer than the server's shutdown grace (20 s): a process that
/// shuts down exits before a stuck engine's typed error surfaces. The bound
/// matters to a group restarted in a running process.
pub(crate) const ENGINE_WORK_STOP_TIMEOUT: Duration = Duration::from_secs(30);

/// Whether a group engine still runs or is shutting down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkStage {
    Running,
    Stopping,
}

/// The engine's side of the work OpenRaft runs for it. Every [`WorkToken`]
/// holds a receiver of `stage`, so the work ended once `stage` has no
/// receiver left.
#[derive(Clone)]
pub(crate) struct OpenRaftWork {
    stage: Arc<watch::Sender<WorkStage>>,
    live: Arc<LiveTokens>,
}

/// What a [`WorkToken`] is held by, for the triage of a stuck engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkKind {
    /// The state machine, which the state-machine worker owns.
    Worker,
    /// A snapshot builder.
    Builder,
    /// The log store or a log reader it handed out.
    Reader,
    /// The network factory or a client it created.
    Client,
    /// An admitted snapshot install, or the admission itself.
    Install,
}

/// How many tokens of each [`WorkKind`] live.
#[derive(Debug, Default)]
struct LiveTokens([std::sync::atomic::AtomicUsize; 5]);

impl LiveTokens {
    fn counter(&self, kind: WorkKind) -> &std::sync::atomic::AtomicUsize {
        let [worker, builder, reader, client, install] = &self.0;
        match kind {
            WorkKind::Worker => worker,
            WorkKind::Builder => builder,
            WorkKind::Reader => reader,
            WorkKind::Client => client,
            WorkKind::Install => install,
        }
    }

    fn add(&self, kind: WorkKind) {
        self.counter(kind)
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn remove(&self, kind: WorkKind) {
        self.counter(kind)
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn count(&self, kind: WorkKind) -> usize {
        self.counter(kind)
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Whether one engine's OpenRaft work still runs. The engine and the
/// node's [`EngineWorkLedger`] share it, and any number of callers may
/// check it or wait for it without a lock.
#[derive(Clone)]
pub(crate) struct WorkEnded(Arc<watch::Sender<WorkStage>>);

/// Held by every object OpenRaft runs the engine's work with. It is dropped
/// with that object, last, after the object's own resources.
pub(crate) struct WorkToken {
    stage: watch::Receiver<WorkStage>,
    kind: WorkKind,
    live: Arc<LiveTokens>,
}

impl Clone for WorkToken {
    fn clone(&self) -> Self {
        self.as_kind(self.kind)
    }
}

impl Drop for WorkToken {
    fn drop(&mut self) {
        self.live.remove(self.kind);
    }
}

impl OpenRaftWork {
    /// The work of a new engine, and the first token for it, the state
    /// machine's.
    pub(crate) fn start() -> (Self, WorkToken) {
        let (stage, stage_rx) = watch::channel(WorkStage::Running);
        let live = Arc::new(LiveTokens::default());
        live.add(WorkKind::Worker);
        (
            Self {
                stage: Arc::new(stage),
                live: Arc::clone(&live),
            },
            WorkToken {
                stage: stage_rx,
                kind: WorkKind::Worker,
                live,
            },
        )
    }

    /// The work of an engine value that hands OpenRaft nothing: no token
    /// exists, so a stop returns at once.
    pub(crate) fn none() -> Self {
        let (work, _token) = Self::start();
        work
    }

    /// Tells the work the engine is shutting down: a snapshot build stops
    /// its upload and its housekeeping, a snapshot install its download and
    /// publication, and a network client its RPC.
    pub(crate) fn begin_stop(&self) {
        self.stage.send_replace(WorkStage::Stopping);
    }

    /// Tells the work the engine is shutting down ([`Self::begin_stop`]),
    /// and waits until every token is gone, at most `timeout`. Call it once
    /// OpenRaft stopped handing out new work, after `Raft::shutdown`.
    /// Calling it again, or from several callers, waits again.
    pub(crate) async fn stop(
        &self,
        raft_group_id: RaftGroupId,
        timeout: Duration,
    ) -> Result<(), GroupInfraError> {
        self.begin_stop();
        match crate::rt::time::timeout(timeout, self.stage.closed()).await {
            Ok(()) => Ok(()),
            Err(_elapsed) => {
                let waited_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
                tracing::error!(
                    raft_group_id = raft_group_id.0,
                    waited_ms,
                    workers = self.live.count(WorkKind::Worker),
                    builders = self.live.count(WorkKind::Builder),
                    readers = self.live.count(WorkKind::Reader),
                    clients = self.live.count(WorkKind::Client),
                    installs = self.live.count(WorkKind::Install),
                    "EngineWorkOutlivedShutdown: OpenRaft work of a stopped group engine still \
                     runs, and its group stays closed until that work ended"
                );
                Err(GroupInfraError::EngineWorkOutlivedShutdown {
                    raft_group_id,
                    waited_ms,
                })
            }
        }
    }

    /// What the node's [`EngineWorkLedger`] checks before the group opens
    /// again.
    pub(crate) fn ended(&self) -> WorkEnded {
        WorkEnded(Arc::clone(&self.stage))
    }
}

impl WorkEnded {
    /// Whether some of the work still runs.
    pub(crate) fn is_running(&self) -> bool {
        !self.0.is_closed()
    }

    /// Waits until the work ended.
    #[cfg(test)]
    pub(crate) async fn ended(&self) {
        self.0.closed().await;
    }
}

impl WorkToken {
    /// A token of no engine, for a state machine or builder outside one:
    /// nothing waits for it, and it never stops.
    pub(crate) fn detached() -> Self {
        let (_work, token) = OpenRaftWork::start();
        token
    }

    /// A token of the same engine held by an object of `kind`.
    pub(crate) fn as_kind(&self, kind: WorkKind) -> Self {
        self.live.add(kind);
        Self {
            stage: self.stage.clone(),
            kind,
            live: Arc::clone(&self.live),
        }
    }

    /// Whether the engine is shutting down.
    pub(crate) fn is_stopping(&self) -> bool {
        matches!(*self.stage.borrow(), WorkStage::Stopping)
    }

    /// Returns once the engine is shutting down. The engine of a token that
    /// went away without shutting down never does.
    pub(crate) async fn stopping(&self) {
        let mut stage = self.stage.clone();
        loop {
            if matches!(*stage.borrow_and_update(), WorkStage::Stopping) {
                return;
            }
            if stage.changed().await.is_err() {
                return std::future::pending().await;
            }
        }
    }

    /// `work`, or `None` once the engine is shutting down first.
    pub(crate) async fn until_stopping<T>(&self, work: impl Future<Output = T>) -> Option<T> {
        crate::rt::select! {
            biased;
            () = self.stopping() => None,
            done = work => Some(done),
        }
    }
}

/// Admits work the engine starts itself that its shutdown waits for with
/// OpenRaft's, such as a snapshot install a peer sent (#709): an admitted
/// task holds a [`WorkToken`]. Once closed it admits nothing more, and holds
/// no token of its own.
#[derive(Default)]
pub(crate) struct WorkAdmission(std::sync::Mutex<Option<WorkToken>>);

impl WorkAdmission {
    pub(crate) fn new(work: WorkToken) -> Self {
        Self(std::sync::Mutex::new(Some(work)))
    }

    /// A token for one admitted task, or `None` once closed.
    pub(crate) fn admit(&self) -> Option<WorkToken> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Admits nothing more.
    pub(crate) fn close(&self) {
        drop(self.0.lock().unwrap_or_else(PoisonError::into_inner).take());
    }
}

impl Debug for WorkAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let open = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some();
        f.debug_struct("WorkAdmission")
            .field("open", &open)
            .finish()
    }
}

/// The work of the latest engine of each group a node opened (#709). A
/// group opens again only once its previous engine's work ended, whatever
/// stopped that engine: a shutdown that gave up, or the shutdown an engine
/// dropped before its own runs in the background. The check takes no time and holds no lock across a
/// wait: a group whose previous work still runs is refused at once, and
/// opens on a later request.
#[derive(Clone, Default)]
pub(crate) struct EngineWorkLedger(Arc<std::sync::Mutex<HashMap<RaftGroupId, LedgerEntry>>>);

/// The work of one group's latest engine, and the task that stops it when
/// it was dropped before its shutdown.
struct LedgerEntry {
    ended: WorkEnded,
    stopping: Option<crate::rt::task::JoinHandle<()>>,
}

impl EngineWorkLedger {
    /// Whether `raft_group_id` may open: its previous engine's work ended.
    pub(crate) fn admit(&self, raft_group_id: RaftGroupId) -> Result<(), PreviousEngineRunning> {
        let running = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&raft_group_id)
            .is_some_and(|entry| entry.ended.is_running());
        if running {
            Err(PreviousEngineRunning { raft_group_id })
        } else {
            Ok(())
        }
    }

    /// Records the work of `raft_group_id`'s new engine.
    pub(crate) fn record(&self, raft_group_id: RaftGroupId, ended: WorkEnded) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(raft_group_id, LedgerEntry {
                ended,
                stopping: None,
            });
    }

    /// Keeps the task that stops `raft_group_id`'s latest engine, which was
    /// dropped before its shutdown.
    pub(crate) fn keep_stopping(
        &self,
        raft_group_id: RaftGroupId,
        stopping: crate::rt::task::JoinHandle<()>,
    ) {
        if let Some(entry) = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_mut(&raft_group_id)
        {
            entry.stopping = Some(stopping);
        }
    }

    /// Waits until the work of `raft_group_id`'s latest engine ended.
    #[cfg(test)]
    pub(crate) async fn ended(&self, raft_group_id: RaftGroupId) {
        let latest = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&raft_group_id)
            .map(|entry| entry.ended.clone());
        if let Some(latest) = latest {
            latest.ended().await;
        }
    }
}

/// A group that does not open because its previous engine's OpenRaft work
/// still runs (#709).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("OpenRaft work of the previous engine of raft group {raft_group_id:?} still runs")]
pub(crate) struct PreviousEngineRunning {
    pub(crate) raft_group_id: RaftGroupId,
}

impl Debug for EngineWorkLedger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let groups = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .copied()
            .collect::<Vec<_>>();
        f.debug_struct("EngineWorkLedger")
            .field("groups", &groups)
            .finish()
    }
}

/// Why a network client of a stopped engine sends nothing more.
#[derive(Debug, thiserror::Error)]
#[error("the group engine shut down")]
pub(crate) struct EngineStopped;

/// A network factory whose clients hold the engine's [`WorkToken`]:
/// OpenRaft hands them to heartbeat, vote, replication and snapshot-sending
/// tasks that `Raft::shutdown` does not join.
pub(crate) struct TrackedNetworkFactory<NF> {
    factory: NF,
    work: WorkToken,
}

impl<NF> TrackedNetworkFactory<NF> {
    pub(crate) fn new(factory: NF, work: WorkToken) -> Self {
        Self { factory, work }
    }
}

/// A network client holding the engine's [`WorkToken`]. Once the engine is
/// shutting down, each RPC of it ends at once.
pub(crate) struct TrackedNetwork<N> {
    /// Declared before the token, so the client drops first.
    network: N,
    work: WorkToken,
}

impl<NF> RaftNetworkFactory<UrsulaRaftTypeConfig> for TrackedNetworkFactory<NF>
where
    NF: RaftNetworkFactory<UrsulaRaftTypeConfig>,
    NF::Network: RaftNetworkV2<UrsulaRaftTypeConfig>,
{
    type Network = TrackedNetwork<NF::Network>;

    async fn new_client(&mut self, target: NodeIdOf, node: &NodeOf) -> Self::Network {
        TrackedNetwork {
            network: self.factory.new_client(target, node).await,
            work: self.work.clone(),
        }
    }

    // The heartbeat and snapshot clients go through the inner factory too,
    // so an override of its own is kept, and each holds the token.
    async fn new_heartbeat_client(&mut self, target: NodeIdOf, node: &NodeOf) -> Self::Network {
        TrackedNetwork {
            network: self.factory.new_heartbeat_client(target, node).await,
            work: self.work.clone(),
        }
    }

    async fn new_snapshot_client(&mut self, target: NodeIdOf, node: &NodeOf) -> Self::Network {
        TrackedNetwork {
            network: self.factory.new_snapshot_client(target, node).await,
            work: self.work.clone(),
        }
    }
}

fn engine_stopped() -> RPCError<UrsulaRaftTypeConfig> {
    RPCError::Unreachable(Unreachable::new(&EngineStopped))
}

impl<N> RaftNetworkV2<UrsulaRaftTypeConfig> for TrackedNetwork<N>
where N: RaftNetworkV2<UrsulaRaftTypeConfig>
{
    type SnapshotData = N::SnapshotData;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<UrsulaRaftTypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<UrsulaRaftTypeConfig>, RPCError<UrsulaRaftTypeConfig>> {
        let Self { network, work } = self;
        work.until_stopping(RaftNetworkV2::append_entries(network, rpc, option))
            .await
            .unwrap_or_else(|| Err(engine_stopped()))
    }

    fn stream_append<'s, S>(
        &'s mut self,
        input: S,
        option: RPCOption,
    ) -> BoxFuture<
        's,
        Result<
            BoxStream<
                's,
                Result<StreamAppendResult<UrsulaRaftTypeConfig>, RPCError<UrsulaRaftTypeConfig>>,
            >,
            RPCError<UrsulaRaftTypeConfig>,
        >,
    >
    where
        S: Stream<Item = AppendEntriesRequest<UrsulaRaftTypeConfig>>
            + OptionalSend
            + Unpin
            + 'static,
    {
        // Replication goes through this stream, not `append_entries`, which
        // OpenRaft calls only through the default `stream_append` of a
        // network that does not override it. The stream is not raced with
        // the engine's stop: a replication task ends with the cancellation
        // RaftCore sends as it stops, while heartbeats and ReadIndex rounds
        // end with their RPC timeouts. Its client holds the token until then.
        RaftNetworkV2::stream_append(&mut self.network, input, option)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<UrsulaRaftTypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<UrsulaRaftTypeConfig>, RPCError<UrsulaRaftTypeConfig>> {
        let Self { network, work } = self;
        work.until_stopping(RaftNetworkV2::vote(network, rpc, option))
            .await
            .unwrap_or_else(|| Err(engine_stopped()))
    }

    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<UrsulaRaftTypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<UrsulaRaftTypeConfig>, RPCError<UrsulaRaftTypeConfig>> {
        let Self { network, work } = self;
        work.until_stopping(RaftNetworkV2::pre_vote(network, rpc, option))
            .await
            .unwrap_or_else(|| Err(engine_stopped()))
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<UrsulaRaftTypeConfig>,
        snapshot: SnapshotOf<UrsulaRaftTypeConfig, N::SnapshotData>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<UrsulaRaftTypeConfig>, StreamingError<UrsulaRaftTypeConfig>> {
        let Self { network, work } = self;
        work.until_stopping(RaftNetworkV2::full_snapshot(
            network, vote, snapshot, cancel, option,
        ))
        .await
        .unwrap_or_else(|| {
            Err(StreamingError::Closed(ReplicationClosed::new(
                EngineStopped,
            )))
        })
    }

    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<UrsulaRaftTypeConfig>,
        option: RPCOption,
    ) -> Result<TransferLeaderResponse<UrsulaRaftTypeConfig>, RPCError<UrsulaRaftTypeConfig>> {
        let Self { network, work } = self;
        work.until_stopping(RaftNetworkV2::transfer_leader(network, req, option))
            .await
            .unwrap_or_else(|| Err(engine_stopped()))
    }

    fn backoff(&self) -> Option<Backoff> {
        RaftNetworkV2::backoff(&self.network)
    }
}

/// A log store whose readers hold the engine's [`WorkToken`]: OpenRaft
/// hands one to its state-machine worker and one to each replication task,
/// and neither is joined by `Raft::shutdown`.
pub(crate) struct TrackedLogStore<LS> {
    store: LS,
    work: WorkToken,
}

impl<LS> TrackedLogStore<LS> {
    pub(crate) fn new(store: LS, work: WorkToken) -> Self {
        Self { store, work }
    }
}

/// A log reader holding the engine's [`WorkToken`].
pub(crate) struct TrackedLogReader<R> {
    /// Declared before the token, so the reader drops first: once the token
    /// is gone, so is the reader's handle on the group's log store, and
    /// with the last handle the group is closed in its core journal.
    reader: R,
    _work: WorkToken,
}

impl<R> RaftLogReader<UrsulaRaftTypeConfig> for TrackedLogReader<R>
where R: RaftLogReader<UrsulaRaftTypeConfig>
{
    async fn leader_bounded_stream<RB>(
        &mut self,
        leader: LeaderIdOf<UrsulaRaftTypeConfig>,
        range: RB,
    ) -> impl Stream<Item = LeaderBoundedStreamResult<UrsulaRaftTypeConfig>> + OptionalSend
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        self.reader.leader_bounded_stream(leader, range).await
    }

    async fn entries_stream<RB>(
        &mut self,
        range: RB,
    ) -> impl Stream<Item = Result<EntryOf<UrsulaRaftTypeConfig>, io::Error>> + OptionalSend
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        self.reader.entries_stream(range).await
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<UrsulaRaftTypeConfig>>, io::Error> {
        self.reader.try_get_log_entries(range).await
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<UrsulaRaftTypeConfig>>, io::Error> {
        self.reader.read_vote().await
    }

    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Vec<EntryOf<UrsulaRaftTypeConfig>>, io::Error> {
        self.reader.limited_get_log_entries(start, end).await
    }

    async fn get_key_log_ids(
        &mut self,
        range: RangeInclusive<LogIdOf<UrsulaRaftTypeConfig>>,
    ) -> Result<Vec<LogIdOf<UrsulaRaftTypeConfig>>, io::Error> {
        self.reader.get_key_log_ids(range).await
    }
}

impl<LS> RaftLogStorage<UrsulaRaftTypeConfig> for TrackedLogStore<LS>
where LS: RaftLogStorage<UrsulaRaftTypeConfig>
{
    type LogReader = TrackedLogReader<LS::LogReader>;

    async fn get_log_state(&mut self) -> Result<LogState<UrsulaRaftTypeConfig>, io::Error> {
        self.store.get_log_state().await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        TrackedLogReader {
            reader: self.store.get_log_reader().await,
            _work: self.work.clone(),
        }
    }

    async fn save_vote(&mut self, vote: &VoteOf<UrsulaRaftTypeConfig>) -> Result<(), io::Error> {
        self.store.save_vote(vote).await
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<UrsulaRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        self.store.save_committed(committed).await
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<UrsulaRaftTypeConfig>>, io::Error> {
        self.store.read_committed().await
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<UrsulaRaftTypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<UrsulaRaftTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        self.store.append(entries, callback).await
    }

    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<UrsulaRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        self.store.truncate_after(last_log_id).await
    }

    async fn purge(&mut self, log_id: LogIdOf<UrsulaRaftTypeConfig>) -> Result<(), io::Error> {
        self.store.purge(log_id).await
    }
}

#[cfg(all(test, not(madsim)))]
mod tests {
    use std::time::Duration;

    use ursula_runtime::GroupInfraError;
    use ursula_shard::RaftGroupId;

    use super::EngineWorkLedger;
    use super::OpenRaftWork;
    use super::WorkAdmission;
    use super::WorkKind;
    use super::WorkToken;

    #[tokio::test]
    async fn stop_waits_for_every_token() {
        let (work, token) = OpenRaftWork::start();
        let builder = token.clone();
        assert!(!token.is_stopping());
        let stopped = work.stop(RaftGroupId(3), Duration::from_millis(50)).await;
        assert!(matches!(
            stopped,
            Err(GroupInfraError::EngineWorkOutlivedShutdown {
                raft_group_id: RaftGroupId(3),
                waited_ms: 50,
            })
        ));
        assert!(token.is_stopping());
        builder.stopping().await;
        drop(token);
        drop(builder);
        assert!(matches!(
            work.stop(RaftGroupId(3), Duration::from_secs(5)).await,
            Ok(())
        ));
    }

    #[test]
    fn live_tokens_are_counted_by_kind() {
        let (work, token) = OpenRaftWork::start();
        let builder = token.as_kind(WorkKind::Builder);
        let second_builder = builder.clone();
        let client = token.as_kind(WorkKind::Client);
        assert_eq!(work.live.count(WorkKind::Worker), 1);
        assert_eq!(work.live.count(WorkKind::Builder), 2);
        assert_eq!(work.live.count(WorkKind::Client), 1);
        assert_eq!(work.live.count(WorkKind::Reader), 0);
        drop(second_builder);
        drop(client);
        assert_eq!(work.live.count(WorkKind::Builder), 1);
        assert_eq!(work.live.count(WorkKind::Client), 0);
        drop(builder);
        drop(token);
        assert_eq!(work.live.count(WorkKind::Worker), 0);
        assert_eq!(work.live.count(WorkKind::Builder), 0);
    }

    #[tokio::test]
    async fn a_detached_token_never_stops() {
        let token = WorkToken::detached();
        assert!(!token.is_stopping());
        assert!(matches!(
            tokio::time::timeout(Duration::from_millis(20), token.stopping()).await,
            Err(_elapsed)
        ));
    }

    #[tokio::test]
    async fn a_group_opens_again_only_once_its_previous_work_ended() {
        let ledger = EngineWorkLedger::default();
        ledger
            .admit(RaftGroupId(4))
            .expect("a group with no previous engine opens");
        let (work, token) = OpenRaftWork::start();
        ledger.record(RaftGroupId(4), work.ended());
        assert!(matches!(
            ledger.admit(RaftGroupId(4)),
            Err(super::PreviousEngineRunning {
                raft_group_id: RaftGroupId(4),
            })
        ));
        ledger.admit(RaftGroupId(5)).expect("another group opens");
        drop(token);
        ledger.ended(RaftGroupId(4)).await;
        ledger
            .admit(RaftGroupId(4))
            .expect("the group opens once the work ended");
        drop(work);
    }

    #[tokio::test]
    async fn two_stops_wait_together_and_report_their_own_bound() {
        let (work, token) = OpenRaftWork::start();
        let (first, second) = tokio::join!(
            work.stop(RaftGroupId(2), Duration::from_millis(30)),
            work.stop(RaftGroupId(2), Duration::from_millis(40)),
        );
        assert!(matches!(
            first,
            Err(GroupInfraError::EngineWorkOutlivedShutdown { waited_ms: 30, .. })
        ));
        assert!(matches!(
            second,
            Err(GroupInfraError::EngineWorkOutlivedShutdown { waited_ms: 40, .. })
        ));
        drop(token);
        work.stop(RaftGroupId(2), Duration::from_secs(5))
            .await
            .expect("the stop returns once the token is gone");
    }

    #[tokio::test]
    async fn a_closed_admission_holds_no_token() {
        let (work, token) = OpenRaftWork::start();
        let admission = WorkAdmission::new(token);
        let admitted = admission.admit().expect("an open admission admits");
        admission.close();
        assert!(admission.admit().is_none());
        assert!(matches!(
            work.stop(RaftGroupId(1), Duration::from_millis(20)).await,
            Err(GroupInfraError::EngineWorkOutlivedShutdown { .. })
        ));
        drop(admitted);
        work.stop(RaftGroupId(1), Duration::from_secs(5))
            .await
            .expect("the stop returns once the admitted task ended");
    }
}
