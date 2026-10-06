//! Memory-WAL full-restart guard (0.6.2): a restart of every voter of a
//! group stops the group instead of re-initializing it empty.
//!
//! A memory-WAL group whose voters all restarted answers the bootstrap probe
//! exactly like a fresh install: every voter is empty. The only thing that
//! can tell the two apart is a record that outlives every process, so the
//! group keeps one in object storage, next to the format-epoch marker:
//! `URSULA_GROUP_INITIALIZED/group-{id}` (see
//! `ursula_runtime::format_marker`). It means "this group may hold
//! acknowledged writes".
//!
//! - **Before the first write.** A leader proposes no client write until the
//!   marker is in object storage ([`RestartGuard::ensure_marked`], checked
//!   once per process and group). So no write is acknowledged without it,
//!   and a crash during the first bootstrap, before any write, leaves no
//!   marker and needs no operator.
//! - **Upgraded groups.** A 0.6.1 cluster has no marker. A replica whose
//!   applied state holds client writes writes it ([`run_init_marker_driver`]),
//!   so a 0.6.2 cluster gains the protection without any new write.
//! - **Bootstrap.** When every voter answers empty, the initializer reads the
//!   marker ([`bootstrap_action`]): absent, it initializes as before; present,
//!   it stops and waits for an operator who accepts the loss
//!   ([`RestartGuard::accept_data_loss`]); unreadable, it retries and never
//!   initializes blind.
//!
//! Without object storage nothing outlives a full restart, so it cannot be
//! detected: the group initializes again, empty.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures_util::future::BoxFuture;
use openraft::BasicNode;
use openraft::rt::WatchReceiver;
use ursula_runtime::format_marker::FormatEpochNamespace;
use ursula_shard::RaftGroupId;

use crate::registry::RaftGroupHandle;
use crate::rejoin::BootstrapDecision;
use crate::rejoin::GroupRejoin;
use crate::rejoin::PeerGroupLog;
use crate::rejoin::bootstrap_decision;

/// How long one marker read or write may take before it counts as failed.
const MARKER_IO_TIMEOUT: Duration = Duration::from_secs(5);

/// Where the per-group "initialized" markers live.
pub trait InitMarkerStore: fmt::Debug + Send + Sync {
    /// Whether the group's marker exists. An unreachable store is an error.
    fn is_marked(&self, raft_group_id: RaftGroupId) -> BoxFuture<'_, io::Result<bool>>;
    /// Write the group's marker (idempotent).
    fn mark(&self, raft_group_id: RaftGroupId) -> BoxFuture<'_, io::Result<()>>;
}

impl InitMarkerStore for FormatEpochNamespace {
    fn is_marked(&self, raft_group_id: RaftGroupId) -> BoxFuture<'_, io::Result<bool>> {
        Box::pin(self.group_marker_exists(raft_group_id.0))
    }

    fn mark(&self, raft_group_id: RaftGroupId) -> BoxFuture<'_, io::Result<()>> {
        Box::pin(self.write_group_marker(raft_group_id.0))
    }
}

/// An in-memory marker store shared by simulated nodes (DST and tests). It
/// can be made unreachable.
#[derive(Debug, Default)]
pub struct MemoryInitMarkers {
    marked: Mutex<BTreeSet<u32>>,
    unreachable: AtomicBool,
}

impl MemoryInitMarkers {
    pub fn set_unreachable(&self, unreachable: bool) {
        self.unreachable.store(unreachable, Ordering::SeqCst);
    }

    pub fn contains(&self, raft_group_id: RaftGroupId) -> bool {
        self.marked
            .lock()
            .expect("init marker mutex")
            .contains(&raft_group_id.0)
    }

    fn check_reachable(&self) -> io::Result<()> {
        if self.unreachable.load(Ordering::SeqCst) {
            Err(io::Error::other("marker store unreachable"))
        } else {
            Ok(())
        }
    }
}

impl InitMarkerStore for MemoryInitMarkers {
    fn is_marked(&self, raft_group_id: RaftGroupId) -> BoxFuture<'_, io::Result<bool>> {
        Box::pin(async move {
            self.check_reachable()?;
            Ok(self.contains(raft_group_id))
        })
    }

    fn mark(&self, raft_group_id: RaftGroupId) -> BoxFuture<'_, io::Result<()>> {
        Box::pin(async move {
            self.check_reachable()?;
            self.marked
                .lock()
                .expect("init marker mutex")
                .insert(raft_group_id.0);
            Ok(())
        })
    }
}

/// What object storage says about a group whose voters are all empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitEvidence {
    /// No object storage configured: a full restart cannot be detected.
    NotConfigured,
    /// No marker: the group never acknowledged a write.
    Absent,
    /// The group was initialized and may have acknowledged writes.
    Present,
    /// The store did not answer.
    Unreadable,
}

/// What a memory-WAL initializer does next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapAction {
    /// Fresh group (or the operator accepted the loss): run `Initialize`.
    Initialize,
    /// Some voter holds the group: wait to be replicated to.
    Rejoin,
    /// Not every voter answered yet, or the marker could not be read: probe
    /// again.
    Wait,
    /// Every voter restarted empty after the group held writes: stop and
    /// wait for an operator.
    StopForOperator,
}

/// The decision table. `evidence` is only consulted when every voter is
/// empty.
pub fn bootstrap_action(
    probe: BootstrapDecision,
    evidence: InitEvidence,
    loss_accepted: bool,
) -> BootstrapAction {
    match probe {
        BootstrapDecision::Rejoin => BootstrapAction::Rejoin,
        BootstrapDecision::Wait => BootstrapAction::Wait,
        BootstrapDecision::Initialize => match evidence {
            InitEvidence::NotConfigured | InitEvidence::Absent => BootstrapAction::Initialize,
            _ if loss_accepted => BootstrapAction::Initialize,
            InitEvidence::Present => BootstrapAction::StopForOperator,
            InitEvidence::Unreadable => BootstrapAction::Wait,
        },
    }
}

/// One memory-WAL replica's full-restart guard for one group.
pub struct RestartGuard {
    raft_group_id: RaftGroupId,
    store: Option<Arc<dyn InitMarkerStore>>,
    /// This process has seen the marker in object storage (or wrote it).
    marked: AtomicBool,
    marking: crate::rt::sync::Mutex<()>,
    stopped: AtomicBool,
    loss_accepted: AtomicBool,
}

impl fmt::Debug for RestartGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RestartGuard")
            .field("raft_group_id", &self.raft_group_id)
            .field("store", &self.store.is_some())
            .field("marked", &self.is_marked())
            .field("stopped", &self.stopped_for_operator())
            .finish_non_exhaustive()
    }
}

impl RestartGuard {
    pub(crate) fn new(raft_group_id: RaftGroupId, store: Option<Arc<dyn InitMarkerStore>>) -> Self {
        Self {
            raft_group_id,
            store,
            marked: AtomicBool::new(false),
            marking: crate::rt::sync::Mutex::new(()),
            stopped: AtomicBool::new(false),
            loss_accepted: AtomicBool::new(false),
        }
    }

    /// The marker is known to exist (always true without object storage).
    pub fn is_marked(&self) -> bool {
        self.store.is_none() || self.marked.load(Ordering::Acquire)
    }

    /// Make sure the group's marker is in object storage before a client
    /// write is proposed. Cheap once it succeeded in this process.
    pub async fn ensure_marked(&self) -> Result<(), String> {
        if self.is_marked() {
            return Ok(());
        }
        let Some(store) = &self.store else {
            return Ok(());
        };
        let _marking = self.marking.lock().await;
        if self.is_marked() {
            return Ok(());
        }
        match crate::rt::time::timeout(MARKER_IO_TIMEOUT, store.mark(self.raft_group_id)).await {
            Ok(Ok(())) => {
                self.marked.store(true, Ordering::Release);
                Ok(())
            }
            Ok(Err(err)) => Err(err.to_string()),
            Err(_) => Err(format!("timed out after {MARKER_IO_TIMEOUT:?}")),
        }
    }

    /// Read the marker for the bootstrap decision.
    pub async fn evidence(&self) -> InitEvidence {
        let Some(store) = &self.store else {
            return InitEvidence::NotConfigured;
        };
        match crate::rt::time::timeout(MARKER_IO_TIMEOUT, store.is_marked(self.raft_group_id)).await
        {
            Ok(Ok(true)) => InitEvidence::Present,
            Ok(Ok(false)) => InitEvidence::Absent,
            Ok(Err(_)) | Err(_) => InitEvidence::Unreadable,
        }
    }

    /// The group stopped because every voter restarted empty after it held
    /// writes; it waits for [`Self::accept_data_loss`].
    pub fn stopped_for_operator(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }

    /// Operator recovery: let the stopped initializer run `Initialize`, which
    /// drops whatever the group held. Returns whether the group was stopped
    /// on this replica (the call is a no-op otherwise).
    pub fn accept_data_loss(&self) -> bool {
        if !self.stopped_for_operator() {
            return false;
        }
        self.loss_accepted.store(true, Ordering::SeqCst);
        true
    }

    fn loss_accepted(&self) -> bool {
        self.loss_accepted.load(Ordering::SeqCst)
    }
}

/// How a memory-WAL bootstrap ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryWalBootstrap {
    /// The group was already initialized when the loop looked.
    AlreadyInitialized,
    Initialized,
    Rejoined,
    /// The Raft stopped, or `Initialize` failed.
    Stopped,
}

/// Membership bootstrap of a memory-WAL initializer. Its log is empty after
/// every start, so an empty log says nothing about the group: it runs
/// `Initialize` only once every other configured voter answered that it is
/// empty too, no leader was seen, and object storage holds no marker for the
/// group (or the operator accepted the loss). `probe` asks one peer
/// (`(id, address)`) with the bootstrap probe vote.
pub async fn run_memory_wal_bootstrap<P, F>(
    node_id: u64,
    raft: RaftGroupHandle,
    rejoin: Arc<GroupRejoin>,
    nodes: BTreeMap<u64, BasicNode>,
    probe: P,
    interval: Duration,
    warn_every: Duration,
) -> MemoryWalBootstrap
where
    P: Fn(u64, String) -> F,
    F: Future<Output = Option<PeerGroupLog>>,
{
    let group = rejoin.raft_group_id().0;
    let guard = rejoin.restart_guard();
    let peers = nodes
        .iter()
        .filter(|(peer_id, _)| **peer_id != node_id)
        .map(|(peer_id, node)| (*peer_id, node.addr.clone()))
        .collect::<Vec<_>>();
    let mut last_warning = crate::rt::time::Instant::now();
    let mut warned_stop = false;
    loop {
        match raft.is_initialized().await {
            Ok(true) => {
                guard.stopped.store(false, Ordering::SeqCst);
                return MemoryWalBootstrap::AlreadyInitialized;
            }
            Ok(false) => {}
            Err(err) => {
                tracing::error!(
                    "raft bootstrap: node {node_id} group {group} failed to check initialization: {err}"
                );
                return MemoryWalBootstrap::Stopped;
            }
        }
        let leader_seen = raft.metrics().borrow_watched().current_leader.is_some();
        let answers = if leader_seen {
            Vec::new()
        } else {
            futures_util::future::join_all(
                peers
                    .iter()
                    .map(|(peer_id, address)| probe(*peer_id, address.clone())),
            )
            .await
        };
        let probe_decision = if leader_seen {
            BootstrapDecision::Rejoin
        } else {
            bootstrap_decision(&answers)
        };
        let evidence = if probe_decision == BootstrapDecision::Initialize {
            guard.evidence().await
        } else {
            InitEvidence::NotConfigured
        };
        let now = crate::rt::time::Instant::now();
        match bootstrap_action(probe_decision, evidence, guard.loss_accepted()) {
            BootstrapAction::Initialize => {
                if guard.loss_accepted() {
                    tracing::warn!(
                        "raft bootstrap: memory-WAL node {node_id} group {group} initializes again empty: the operator accepted the loss of its writes"
                    );
                }
                rejoin.allow_fresh_bootstrap();
                if let Err(err) = raft.initialize(nodes).await {
                    tracing::error!(
                        "raft bootstrap: node {node_id} group {group} failed to initialize membership: {err}"
                    );
                    return MemoryWalBootstrap::Stopped;
                }
                guard.stopped.store(false, Ordering::SeqCst);
                return MemoryWalBootstrap::Initialized;
            }
            BootstrapAction::Rejoin => {
                guard.stopped.store(false, Ordering::SeqCst);
                tracing::warn!(
                    "raft bootstrap: memory-WAL node {node_id} group {group} restarted empty in an initialized group; not re-initializing, waiting to be rebuilt from the leader"
                );
                return MemoryWalBootstrap::Rejoined;
            }
            BootstrapAction::StopForOperator => {
                guard.stopped.store(true, Ordering::SeqCst);
                if !warned_stop || now.saturating_duration_since(last_warning) >= warn_every {
                    tracing::error!(
                        node_id,
                        raft_group_id = group,
                        "raft bootstrap: memory-WAL group {group} lost its log on every voter after it held writes (object storage holds its initialized marker); it stays without a leader and refuses writes. To initialize it again empty and accept the loss, run POST /__ursula/raft/{group}/rejoin/reinitialize?accept_data_loss=true on node {node_id}"
                    );
                    warned_stop = true;
                    last_warning = now;
                }
            }
            BootstrapAction::Wait => {
                if now.saturating_duration_since(last_warning) >= warn_every {
                    if evidence == InitEvidence::Unreadable {
                        tracing::warn!(
                            "raft bootstrap: memory-WAL node {node_id} group {group} cannot read its initialized marker in object storage; not initializing until it can"
                        );
                    } else {
                        let answered = answers.iter().filter(|answer| answer.is_some()).count();
                        tracing::warn!(
                            "raft bootstrap: memory-WAL node {node_id} group {group} waits for every voter before initializing; {answered}/{} answered",
                            peers.len()
                        );
                    }
                    last_warning = now;
                }
            }
        }
        crate::rt::time::sleep(interval).await;
    }
}

/// Write the marker of an upgraded group: once this replica's applied state
/// holds client writes, the group is marked even if no new write arrives.
/// Returns once marked, or when the Raft stops.
pub async fn run_init_marker_driver(
    raft: RaftGroupHandle,
    rejoin: Arc<GroupRejoin>,
    interval: Duration,
) {
    let guard = rejoin.restart_guard();
    while !guard.is_marked() {
        crate::rt::time::sleep(interval).await;
        if raft.metrics().borrow_watched().running_state.is_err() {
            return;
        }
        let holds_writes = raft
            .with_state_machine(|state_machine| {
                Box::pin(async move { state_machine.engine.holds_client_state() })
            })
            .await
            .unwrap_or(false);
        if !holds_writes {
            continue;
        }
        if let Err(err) = guard.ensure_marked().await {
            tracing::warn!(
                raft_group_id = rejoin.raft_group_id().0,
                "memory-WAL group holds writes but its initialized marker could not be written yet: {err}"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decision_table() {
        use BootstrapAction as A;
        use BootstrapDecision as P;
        use InitEvidence as E;
        // Fresh install: every voter empty, no marker (or no object storage).
        assert_eq!(
            bootstrap_action(P::Initialize, E::Absent, false),
            A::Initialize
        );
        assert_eq!(
            bootstrap_action(P::Initialize, E::NotConfigured, false),
            A::Initialize
        );
        // Full restart after writes: stop, unless the operator accepted it.
        assert_eq!(
            bootstrap_action(P::Initialize, E::Present, false),
            A::StopForOperator
        );
        assert_eq!(
            bootstrap_action(P::Initialize, E::Present, true),
            A::Initialize
        );
        // Evidence unreadable: never initialize blind.
        assert_eq!(
            bootstrap_action(P::Initialize, E::Unreadable, false),
            A::Wait
        );
        // Partial restart: a voter holds the group, the marker is irrelevant.
        for evidence in [E::Absent, E::Present, E::Unreadable, E::NotConfigured] {
            assert_eq!(bootstrap_action(P::Rejoin, evidence, false), A::Rejoin);
            assert_eq!(bootstrap_action(P::Wait, evidence, true), A::Wait);
        }
    }

    #[tokio::test]
    async fn marker_is_written_once_and_an_unreachable_store_refuses() {
        let store = Arc::new(MemoryInitMarkers::default());
        let guard = RestartGuard::new(RaftGroupId(7), Some(store.clone()));
        assert_eq!(guard.evidence().await, InitEvidence::Absent);
        store.set_unreachable(true);
        assert_eq!(guard.evidence().await, InitEvidence::Unreadable);
        assert!(guard.ensure_marked().await.is_err());
        assert!(!guard.is_marked());
        store.set_unreachable(false);
        guard.ensure_marked().await.expect("mark");
        assert!(guard.is_marked() && store.contains(RaftGroupId(7)));
        // Once marked, a later outage does not block writes in this process.
        store.set_unreachable(true);
        guard.ensure_marked().await.expect("cached");
        store.set_unreachable(false);
        assert_eq!(guard.evidence().await, InitEvidence::Present);

        let unguarded = RestartGuard::new(RaftGroupId(7), None);
        assert!(unguarded.is_marked());
        assert_eq!(unguarded.evidence().await, InitEvidence::NotConfigured);
    }

    #[test]
    fn data_loss_is_accepted_only_where_the_group_stopped() {
        let guard = RestartGuard::new(RaftGroupId(1), None);
        assert!(!guard.accept_data_loss());
        assert!(!guard.loss_accepted());
        guard.stopped.store(true, Ordering::SeqCst);
        assert!(guard.accept_data_loss());
        assert!(guard.loss_accepted());
    }
}
