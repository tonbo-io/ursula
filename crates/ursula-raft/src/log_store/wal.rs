//! Node WAL lifecycle and per-core journal ownership, and the meta journal's.

use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures_util::future::join_all;
use ursula_config::WalFsync;
use ursula_control::Admission;
use ursula_control::LocalLog;
use ursula_headers::ClusterId;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::ProcessIncarnation;
use ursula_runtime::GroupEngineMetrics;
use ursula_runtime::NodeFence;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;

use crate::log_store::CleanExit;
use crate::log_store::CoreFileLogWriter;
use crate::log_store::CoreJournalError;
use crate::log_store::CoreJournalOptions;
use crate::log_store::JournalTuning;
use crate::log_store::LaggingGroups;
use crate::log_store::META_FSYNC;
use crate::log_store::MetaJournalDamage;
use crate::log_store::MetaOpening;
use crate::log_store::NodeWal;
use crate::log_store::Predecessor;
use crate::log_store::RaftGroupFileLogStore;
use crate::log_store::RaftWalError;
use crate::log_store::RecoveryState;
use crate::log_store::RootStanding;
use crate::log_store::WaitFrom;
use crate::log_store::WalOpening;
use crate::log_store::WriterExited;
use crate::log_store::core_dir;
use crate::rt::time::Instant;

/// The open writer of one core's journal, if any, and the signal that its
/// worker stopped, which outlives the writer's last handle.
type CoreWriterSlot = Arc<Mutex<CoreWriterState>>;

#[derive(Debug, Default)]
struct CoreWriterState {
    writer: Weak<CoreFileLogWriter>,
    /// Whether the run keeps the writer.
    held: HeldCore,
    exited: Option<WriterExited>,
    /// Closes once the writer is gone and the journal's lock is free.
    released: Option<WriterExited>,
}

/// Whether a run keeps a core's writer (7.2 of #768).
#[derive(Debug, Default)]
enum HeldCore {
    /// No writer opened since [`RaftWal::open_held_cores`] ran, or it has
    /// not run: a writer a store's open opens lives as long as a store
    /// holds it.
    #[default]
    Lazy,
    /// The run keeps the core's writer: it was open when
    /// [`RaftWal::open_held_cores`] ran, or opened after it.
    Held(Arc<CoreFileLogWriter>),
    /// The run keeps the core's writer once it opens, but it did not open.
    /// Its groups are `Unknown` until an open of one of them opens the
    /// writer.
    Unopened,
}

/// What the node's journals hold of each data group
/// ([`RaftWal::local_groups`]). Once [`RaftWal::open_held_cores`] ran, a
/// group they do not name is absent.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LocalGroups {
    groups: BTreeMap<RaftGroupId, LocalLog>,
}

impl LocalGroups {
    /// What the journals hold of `group`, if they name it.
    pub fn get(&self, group: RaftGroupId) -> Option<LocalLog> {
        self.groups.get(&group).copied()
    }

    /// Every group the journals name, in group order.
    pub fn iter(&self) -> impl Iterator<Item = (RaftGroupId, LocalLog)> + '_ {
        self.groups.iter().map(|(group, log)| (*group, *log))
    }
}

/// The core writers of a running WAL, by core.
#[derive(Debug)]
enum CoreWriterSlots {
    Running(BTreeMap<u16, CoreWriterSlot>),
    /// [`RaftWal::shutdown`] closed every writer; no
    /// journal opens again in this run.
    ShutDown,
}

/// The meta journal's writer in a run. Unlike a core's, it is held for the
/// whole run once open, so a restart of the meta replica reopens its group
/// on the same writer.
#[derive(Debug, Default)]
enum MetaWriterSlot {
    /// No meta store opened yet in this run.
    #[default]
    Unopened,
    Open(Arc<CoreFileLogWriter>),
    /// [`RaftWal::shutdown`] closed it; it opens no more in this run.
    ShutDown,
}

/// Where the meta group's log lives on its own journal: core 0, shard 0 and
/// group 0 of the meta journal's namespace, which no data group shares.
pub(crate) const META_PLACEMENT: ShardPlacement = ShardPlacement {
    core_id: CoreId(0),
    shard_id: ShardId(0),
    raft_group_id: RaftGroupId(0),
};

/// The meta group's cache of recent entries: the default per-group cache,
/// whatever the data journals' tuning sets.
const META_CACHE_BYTES: u64 = JournalTuning::DEFAULT_GROUP_CACHE_BYTES;

/// Failure to open the meta group's log store ([`RaftWal::open_meta`]).
#[derive(Debug, thiserror::Error)]
pub enum MetaOpenError {
    /// The meta journal does not open in this run: the start could not read
    /// or mark it, or its writer did not open. The node runs on without it.
    #[error("the meta journal is damaged: {0}")]
    Damaged(#[source] MetaJournalDamage),
    /// The OpenRaft work of the meta group's previous engine still runs.
    #[error("OpenRaft work of the previous engine of the meta group still runs")]
    PreviousEngineRunning,
    /// The meta group's store did not open on the open meta journal.
    #[error("open the meta group's store: {0}")]
    OpenGroup(#[source] CoreJournalError),
    #[error("the Raft WAL under '{}' has shut down", .root.display())]
    ShutDown { root: PathBuf },
    #[error("Raft WAL state mutex poisoned")]
    LockPoisoned,
}

/// Opens each group's durable log store over its core's shared journal, for
/// one run of the node's Raft WAL.
///
/// [`RaftWal::start`] begins the run: it reads the run
/// state the previous run left, decides how the journals open
/// ([`WalOpening`], [`MetaOpening`]) and records this run before any journal
/// write. [`RaftWal::shutdown`] ends it cleanly.
///
/// The meta group's log lives on a journal of its own, under `meta` beside
/// the cores' directories ([`RaftWal::open_meta`]).
#[derive(Debug, Clone)]
pub struct RaftWal {
    node: Arc<NodeWal>,
    tuning: JournalTuning,
    /// The core and group counts the root was started with.
    topology: ursula_shard::StaticShardMap,
    lagging: Arc<LaggingGroups>,
    /// One slot per core. Opening a journal holds only its core's slot, so
    /// cores recover their journals in parallel.
    core_writers: Arc<Mutex<CoreWriterSlots>>,
    /// Whether the run keeps every core writer it opens (7.2 of #768):
    /// set by [`Self::open_held_cores`] before it opens a core, and read
    /// under a core's slot lock, so a writer that opens after a core's turn
    /// in it is kept too and a group a journal names stays listed
    /// ([`Self::local_groups`]) after its last store closes. Before, a
    /// writer lives as long as a store holds it.
    keeps_writers: Arc<AtomicBool>,
    /// The OpenRaft work of each group's latest engine in this run: a group
    /// opens again only once its previous engine's work ended (#709).
    engine_work: crate::openraft_work::EngineWorkLedger,
    /// The meta journal's writer.
    meta_writer: Arc<Mutex<MetaWriterSlot>>,
    /// The meta journal's lagging groups, apart from the data cores'.
    meta_lagging: Arc<LaggingGroups>,
}

impl RaftWal {
    /// Starts a run of node 1's Raft WAL under `root` with the `fsync`
    /// policy and the default segment size and entry cache.
    pub fn start(
        root: impl Into<PathBuf>,
        fsync: WalFsync,
        topology: &ursula_shard::StaticShardMap,
    ) -> Result<Self, RaftWalError> {
        Self::start_with(root, JournalTuning::new(fsync), topology)
    }

    /// Starts a run of node 1's Raft WAL under `root` with `tuning`,
    /// keeping the cluster id the root holds or generating one for a new
    /// root.
    pub fn start_with(
        root: impl Into<PathBuf>,
        tuning: JournalTuning,
        topology: &ursula_shard::StaticShardMap,
    ) -> Result<Self, RaftWalError> {
        Self::start_in_cluster(
            root,
            tuning,
            topology,
            NodeId::new(1),
            None,
            NodeFence::unleased(),
        )
    }

    /// Starts a run of node `node`'s Raft WAL under `root` with `tuning` for
    /// the configured `cluster_id`. A new root records the node, a root id
    /// it draws, and the cluster id, or a generated one when `None`. An
    /// existing root must hold the same node id
    /// ([`RaftWalError::OtherNode`]) and cluster id
    /// ([`RaftWalError::ClusterIdMismatch`]). The start counts its run in
    /// the root's identity file before it returns ([`Self::incarnation`]).
    ///
    /// A leased `fence` takes that synced write as its lease's first
    /// success, and a probe keeps the lease for as long as the WAL lives. It
    /// owes the predecessor wait when a process of an earlier run of the
    /// root may still run ([`Self::predecessor_wait`]).
    pub fn start_in_cluster(
        root: impl Into<PathBuf>,
        tuning: JournalTuning,
        topology: &ursula_shard::StaticShardMap,
        node: NodeId,
        cluster_id: Option<&ClusterId>,
        fence: NodeFence,
    ) -> Result<Self, RaftWalError> {
        Ok(Self {
            node: Arc::new(NodeWal::start(
                root.into(),
                tuning.fsync,
                topology,
                cluster_id,
                node,
                fence,
            )?),
            tuning,
            topology: topology.clone(),
            lagging: Arc::new(LaggingGroups::default()),
            core_writers: Arc::new(Mutex::new(CoreWriterSlots::Running(BTreeMap::new()))),
            keeps_writers: Arc::new(AtomicBool::new(false)),
            engine_work: crate::openraft_work::EngineWorkLedger::default(),
            meta_writer: Arc::new(Mutex::new(MetaWriterSlot::Unopened)),
            meta_lagging: Arc::new(LaggingGroups::default()),
        })
    }

    /// Refuses `configured` when the WAL root at `root` already holds
    /// another cluster id ([`RaftWalError::ClusterIdMismatch`]), reading
    /// only, so a node checks it before it contacts any peer.
    pub fn check_cluster_id(root: &Path, configured: &ClusterId) -> Result<(), RaftWalError> {
        super::topology::check_cluster_id(root, configured)
    }

    pub fn root(&self) -> &Path {
        self.node.root()
    }

    /// The cluster id bound to this WAL root.
    pub fn cluster_id(&self) -> &ClusterId {
        self.node.cluster_id()
    }

    /// This process's incarnation: the root's id, the run this start
    /// counted, durably, and the nonce it drew.
    pub fn incarnation(&self) -> ProcessIncarnation {
        self.node.incarnation()
    }

    /// Whether the process of an earlier run of this root may still run,
    /// as this start read it.
    pub fn predecessor(&self) -> Predecessor {
        self.node.predecessor()
    }

    /// This process's fence.
    pub fn fence(&self) -> NodeFence {
        self.node.fence().clone()
    }

    /// The wait this run owes a predecessor that may still run, if it owes
    /// one, as a handle that holds neither the root's lock nor any journal
    /// ([`PredecessorWait`]).
    pub fn predecessor_wait(&self) -> Option<PredecessorWait> {
        self.node.wait().map(|from| PredecessorWait {
            from,
            root: self.root().to_owned(),
            fence: self.node.fence().clone(),
            node: Arc::downgrade(&self.node),
        })
    }

    /// How the root stood in meta when this run opened it, as its identity
    /// file recorded, with the record of its first binding
    /// ([`BindingRecord`]).
    pub(crate) fn binding(&self) -> BindingRecord {
        BindingRecord {
            standing: self.node.standing(),
            root: self.root().to_owned(),
            node: Arc::downgrade(&self.node),
        }
    }

    /// The data journals' fsync policy. The meta journal always runs
    /// `always`.
    pub fn fsync(&self) -> WalFsync {
        self.node.fsync()
    }

    /// The groups whose live records keep old journal segments alive, for
    /// the snapshot driver.
    pub fn lagging_groups(&self) -> Arc<LaggingGroups> {
        self.lagging.clone()
    }

    /// How this run opens the data journals the previous run left.
    pub fn opening(&self) -> WalOpening {
        self.node.opening()
    }

    /// How this run opens the meta journal, by the meta class's own record
    /// in the run state and the meta journal's own history.
    pub fn meta_opening(&self) -> MetaOpening {
        self.node.meta_opening()
    }

    /// Whether this node's data logs may be missing entries they
    /// acknowledged.
    pub fn recovery_state(&self) -> RecoveryState {
        self.node.opening().recovery
    }

    /// Where the root's shard map places `group`, `None` past its group
    /// count.
    pub fn placement(&self, group: RaftGroupId) -> Option<ShardPlacement> {
        self.topology.placement(group)
    }

    /// The journal directory of core `core_id`.
    pub fn core_dir(&self, core_id: CoreId) -> PathBuf {
        core_dir(self.root(), core_id.0)
    }

    /// Where a group's node-local record of its current snapshot lives, for
    /// a simulation that builds its engines itself.
    #[cfg(madsim)]
    pub fn sim_snapshot_metadata_path(&self, placement: ShardPlacement) -> PathBuf {
        self.snapshot_metadata_path(placement)
    }

    /// The path [`RaftGroupFileLogStore::snapshot_record`] names for the
    /// group of `placement`.
    ///
    /// [`RaftGroupFileLogStore::snapshot_record`]: super::RaftGroupFileLogStore::snapshot_record
    #[cfg(any(test, madsim))]
    pub(crate) fn snapshot_metadata_path(&self, placement: ShardPlacement) -> PathBuf {
        self.core_dir(placement.core_id)
            .join(super::file::snapshot_record_file(placement.raft_group_id.0))
    }

    /// The writer of `placement`'s core, opened if no writer is open. Once
    /// [`Self::open_held_cores`] ran, the run keeps it. An
    /// [`Admission::Open`] never creates a core's journal: on a core that
    /// holds nothing, so names no group, it is refused before the core
    /// opens ([`RaftWalError::NotHeld`]).
    fn core_writer(
        &self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
        admission: Admission,
    ) -> Result<Arc<CoreFileLogWriter>, RaftWalError> {
        let slot = match &mut *self
            .core_writers
            .lock()
            .map_err(|_poisoned| RaftWalError::LockPoisoned)?
        {
            CoreWriterSlots::Running(slots) => {
                slots.entry(placement.core_id.0).or_default().clone()
            }
            CoreWriterSlots::ShutDown => {
                return Err(RaftWalError::ShutDown {
                    root: self.root().to_owned(),
                });
            }
        };
        let mut slot = slot
            .lock()
            .map_err(|_poisoned| RaftWalError::LockPoisoned)?;
        let keep = self.keeps_writers.load(Ordering::SeqCst);
        if let Some(writer) = slot.writer.upgrade() {
            if keep {
                slot.held = HeldCore::Held(writer.clone());
            }
            return Ok(writer);
        }
        // A writer whose last handle is being dropped still holds the
        // journal's lock until it is gone, so a new writer would find it
        // locked.
        if let Some(released) = slot.released.clone()
            && released.has_changed().is_ok()
        {
            if keep {
                slot.held = HeldCore::Unopened;
            }
            return Err(RaftWalError::CoreWriterStopping {
                core: placement.core_id,
                released,
            });
        }
        let dir = self.core_dir(placement.core_id);
        if admission == Admission::Open && !super::core_holds_state(&dir) {
            return Err(RaftWalError::NotHeld {
                placement,
                admission,
            });
        }

        let opening = self.node.opening();
        let opened = CoreFileLogWriter::open(dir, CoreJournalOptions {
            previous_run: opening.previous_run,
            core: placement.core_id,
            tuning: self.tuning,
            recovery_epoch: opening.recovery_epoch,
            run_state: self.node.run_state().clone(),
            node_recovery: opening.recovery,
            lagging: self.lagging.clone(),
            metrics: Some((placement, metrics)),
        });
        let writer = match opened {
            Ok(writer) => writer,
            Err(source) => {
                if keep && !matches!(slot.held, HeldCore::Held(_)) {
                    slot.held = HeldCore::Unopened;
                }
                return Err(RaftWalError::OpenCore {
                    core: placement.core_id,
                    source,
                });
            }
        };
        let held = match std::mem::take(&mut slot.held) {
            HeldCore::Lazy if !keep => HeldCore::Lazy,
            HeldCore::Lazy | HeldCore::Held(_) | HeldCore::Unopened => {
                HeldCore::Held(writer.clone())
            }
        };
        *slot = CoreWriterState {
            writer: Arc::downgrade(&writer),
            held,
            exited: Some(writer.exited()),
            released: Some(writer.released()),
        };
        Ok(writer)
    }

    /// Opens `placement`'s log store as `admission` allows (4.2 of #768).
    /// Either admission opens a log the core's repaired metadata names,
    /// unless it is retired: replay names every group whose journal holds
    /// an entry or a purge, and every empty group whose snapshot record is
    /// of its generation, which opens recovering. [`Admission::Create`]
    /// also opens a group the metadata does not name, with a fresh log.
    /// Every other open is refused ([`RaftWalError::NotHeld`]), a retired
    /// log's included until its reopen lands. A refusal writes nothing for
    /// the group, and an `Open` on a core that holds nothing does not open
    /// the core.
    ///
    /// A group whose previous engine's OpenRaft work still runs is refused
    /// at once ([`RaftWalError::PreviousEngineRunning`]): that work may
    /// still change the group's state (#709).
    pub fn open(
        &self,
        placement: ShardPlacement,
        admission: Admission,
        metrics: GroupEngineMetrics,
    ) -> Result<Arc<RaftGroupFileLogStore>, RaftWalError> {
        self.engine_work
            .admit(placement.raft_group_id)
            .map_err(|_running| RaftWalError::PreviousEngineRunning { placement })?;
        let core_writer = self.core_writer(placement, metrics.clone(), admission)?;
        RaftGroupFileLogStore::open(placement, metrics, core_writer, admission).map_err(|source| {
            match source {
                CoreJournalError::NotHeld { admission, .. } => RaftWalError::NotHeld {
                    placement,
                    admission,
                },
                source => RaftWalError::OpenGroup { placement, source },
            }
        })
    }

    /// Opens the writer of every core whose directory holds metadata, a
    /// segment record or a snapshot record, and keeps each for the run
    /// (4.3 and 7.2 of #768), then reads what the journals hold
    /// ([`Self::local_groups`]). The cores replay in parallel, each on a
    /// thread of its own, so a caller on an async runtime runs this on its
    /// blocking pool. A core whose writer does not open stops nothing: its
    /// groups read [`LocalLog::Unknown`] until an open of one of them opens
    /// the writer. Only a WAL that shut down, or whose state lock is
    /// poisoned, fails the call.
    ///
    /// From then on the run keeps every core writer it opens, so
    /// [`Self::local_groups`] lists every group a journal names.
    pub fn open_held_cores(
        &self,
        metrics: GroupEngineMetrics,
    ) -> Result<LocalGroups, RaftWalError> {
        self.keeps_writers.store(true, Ordering::SeqCst);
        // The core's first group names the core for its metrics. A core
        // without a group holds nothing.
        let cores: Vec<(u16, ShardPlacement)> = (0..self.topology.core_count())
            .filter_map(|core| {
                let placement = self.topology.placement(RaftGroupId(u32::from(core)))?;
                Some((core, placement))
            })
            .collect();
        let open = |(core, placement): (u16, ShardPlacement)| {
            self.open_held_core(core, placement, metrics.clone())
        };
        // The simulated disk is in memory and its runtime runs one thread,
        // so the simulation replays the cores in turn.
        #[cfg(madsim)]
        let opened: Vec<Result<(), RaftWalError>> = cores.into_iter().map(open).collect();
        #[cfg(not(madsim))]
        let opened: Vec<Result<(), RaftWalError>> = std::thread::scope(|scope| {
            let open = &open;
            let replays: Vec<_> = cores
                .into_iter()
                .map(|core| scope.spawn(move || open(core)))
                .collect();
            replays
                .into_iter()
                .map(|replay| {
                    replay
                        .join()
                        .unwrap_or_else(|panicked| std::panic::resume_unwind(panicked))
                })
                .collect()
        });
        for result in opened {
            result?;
        }
        self.local_groups()
    }

    /// Opens and keeps `core`'s writer, whose first group is `placement`,
    /// for [`Self::open_held_cores`].
    fn open_held_core(
        &self,
        core: u16,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> Result<(), RaftWalError> {
        match self.core_writer(placement, metrics, Admission::Open) {
            // A core that holds nothing names no group and stays closed.
            Ok(_) | Err(RaftWalError::NotHeld { .. }) => Ok(()),
            Err(error @ (RaftWalError::ShutDown { .. } | RaftWalError::LockPoisoned)) => Err(error),
            Err(error) => {
                tracing::error!(
                    core = core,
                    %error,
                    "the core journal did not open; its groups stay unknown until one opens"
                );
                Ok(())
            }
        }
    }

    /// What the journals hold of each data group, as of now: the groups of
    /// every open core writer as its journal names them, and every group
    /// of a core whose writer [`Self::open_held_cores`] could not open, or
    /// that is opening right now, as [`LocalLog::Unknown`]. A core no
    /// writer holds open contributes nothing. After
    /// [`Self::open_held_cores`] that is only a core that holds nothing:
    /// the run then keeps every writer it opens.
    pub fn local_groups(&self) -> Result<LocalGroups, RaftWalError> {
        let slots = match &*self
            .core_writers
            .lock()
            .map_err(|_poisoned| RaftWalError::LockPoisoned)?
        {
            CoreWriterSlots::Running(slots) => slots.clone(),
            CoreWriterSlots::ShutDown => {
                return Err(RaftWalError::ShutDown {
                    root: self.root().to_owned(),
                });
            }
        };
        let mut groups = BTreeMap::new();
        for (core, slot) in slots {
            // Opening a journal holds its slot through the replay: its
            // groups are not known yet.
            let writer = match slot.try_lock() {
                Ok(slot) => match &slot.held {
                    HeldCore::Held(writer) => Some(writer.clone()),
                    HeldCore::Lazy => match slot.writer.upgrade() {
                        Some(writer) => Some(writer),
                        None => continue,
                    },
                    HeldCore::Unopened => None,
                },
                Err(std::sync::TryLockError::WouldBlock) => None,
                Err(std::sync::TryLockError::Poisoned(_poisoned)) => {
                    return Err(RaftWalError::LockPoisoned);
                }
            };
            match writer {
                Some(writer) => groups.extend(
                    writer
                        .local_logs()
                        .map_err(|_poisoned| RaftWalError::LockPoisoned)?,
                ),
                None => {
                    groups.extend(
                        self.core_groups(core)
                            .map(|group| (group, LocalLog::Unknown)),
                    );
                }
            }
        }
        Ok(LocalGroups { groups })
    }

    /// The groups the shard map places on `core`.
    fn core_groups(&self, core: u16) -> impl Iterator<Item = RaftGroupId> + '_ {
        (0..self.topology.raft_group_count())
            .map(RaftGroupId)
            .filter(move |group| {
                self.topology
                    .placement(*group)
                    .is_some_and(|placement| placement.core_id == CoreId(core))
            })
    }

    /// Opens `placement`'s log store as [`Self::open`] does, after waiting
    /// for the core's previous journal writer to stop when its last handle
    /// is being dropped, as when another group of the core just failed to
    /// open (#741). The refusal carries the signal it waits on, so no
    /// other writer's can stand in for it.
    pub async fn open_waiting(
        &self,
        placement: ShardPlacement,
        admission: Admission,
        metrics: GroupEngineMetrics,
    ) -> Result<Arc<RaftGroupFileLogStore>, RaftWalError> {
        loop {
            match self.open(placement, admission, metrics.clone()) {
                Err(RaftWalError::CoreWriterStopping { mut released, .. }) => {
                    while released.changed().await.is_ok() {}
                }
                opened => return opened,
            }
        }
    }

    /// Opens the meta group's log store on the meta journal under
    /// `raft-log/meta/`, which always runs `fsync = always`, with its WAL
    /// metrics in `metrics`, the meta group's own.
    ///
    /// The first call of a run opens the meta journal's writer, which the
    /// run then holds until [`Self::shutdown`], so a later call, after the
    /// previous store closed, reopens the group on the same writer. When the
    /// start found no meta journal, the first call puts the journal's
    /// metadata file in place before the writer opens, recording the group
    /// recovering if a data journal held records at start or the meta
    /// group's snapshot record remains. A meta journal the start could not
    /// read, or whose writer does not open, is refused as
    /// [`MetaOpenError::Damaged`] and never stops the node. A meta group
    /// whose previous engine's OpenRaft work still runs is refused at once.
    pub fn open_meta(
        &self,
        metrics: GroupEngineMetrics,
    ) -> Result<Arc<RaftGroupFileLogStore>, MetaOpenError> {
        let opening = match self.node.meta_opening() {
            MetaOpening::Decided(opening) => opening,
            MetaOpening::Damaged { damage, .. } => return Err(MetaOpenError::Damaged(damage)),
        };
        self.engine_work
            .admit_meta()
            .map_err(|_running| MetaOpenError::PreviousEngineRunning)?;
        let writer = {
            // Opening the journal holds the slot through its replay, as a
            // core's open holds its slot.
            let mut slot = self
                .meta_writer
                .lock()
                .map_err(|_poisoned| MetaOpenError::LockPoisoned)?;
            match &*slot {
                MetaWriterSlot::Open(writer) => writer.clone(),
                MetaWriterSlot::ShutDown => {
                    return Err(MetaOpenError::ShutDown {
                        root: self.root().to_owned(),
                    });
                }
                MetaWriterSlot::Unopened => {
                    let writer = self
                        .open_meta_writer(opening, metrics.clone())
                        .map_err(MetaOpenError::Damaged)?;
                    *slot = MetaWriterSlot::Open(writer.clone());
                    writer
                }
            }
        };
        // The meta group's journal decides its own creation
        // ([`NodeWal::create_meta_journal`]).
        RaftGroupFileLogStore::open(META_PLACEMENT, metrics, writer, Admission::Create)
            .map_err(MetaOpenError::OpenGroup)
    }

    fn open_meta_writer(
        &self,
        opening: WalOpening,
        metrics: GroupEngineMetrics,
    ) -> Result<Arc<CoreFileLogWriter>, MetaJournalDamage> {
        self.node
            .create_meta_journal(META_PLACEMENT.raft_group_id)?;
        CoreFileLogWriter::open(self.node.meta_dir(), CoreJournalOptions {
            previous_run: opening.previous_run,
            core: META_PLACEMENT.core_id,
            tuning: JournalTuning {
                fsync: meta_fsync(self.tuning.fsync),
                segment_bytes: self.tuning.segment_bytes,
                group_cache_bytes: META_CACHE_BYTES,
            },
            recovery_epoch: opening.recovery_epoch,
            run_state: self.node.run_state().clone(),
            node_recovery: opening.recovery,
            lagging: self.meta_lagging.clone(),
            metrics: Some((META_PLACEMENT, metrics)),
        })
        .map_err(MetaJournalDamage::OpenJournal)
    }

    /// How the meta journal was read when it opened in this run, if it has.
    #[cfg(all(test, not(madsim)))]
    pub(crate) fn meta_replay_mode(&self) -> Option<super::JournalReplayMode> {
        match &*self.meta_writer.lock().expect("the meta writer slot") {
            MetaWriterSlot::Open(writer) => Some(writer.replay_mode()),
            MetaWriterSlot::Unopened | MetaWriterSlot::ShutDown => None,
        }
    }

    /// The meta journal's lagging groups, for the meta group's snapshot
    /// trigger.
    pub fn meta_lagging_groups(&self) -> Arc<LaggingGroups> {
        self.meta_lagging.clone()
    }

    /// What the meta group's snapshot trigger needs of this WAL: the meta
    /// journal's lagging groups and its maintenance passes. Like
    /// [`Self::maintenance`], it holds neither the root's lock nor the
    /// journal.
    pub fn meta_maintenance(&self) -> WalMaintenance {
        WalMaintenance {
            lagging: self.meta_lagging.clone(),
            journals: MaintainedJournals::Meta(Arc::downgrade(&self.meta_writer)),
        }
    }

    /// The ledger engines of this WAL's groups record their OpenRaft work in
    /// as soon as their Raft starts (#709).
    pub(crate) fn engine_work_ledger(&self) -> crate::openraft_work::EngineWorkLedger {
        self.engine_work.clone()
    }

    /// Waits until the OpenRaft work of `raft_group_id`'s latest engine
    /// ended.
    #[cfg(test)]
    pub(crate) async fn engine_work_ended(&self, raft_group_id: ursula_shard::RaftGroupId) {
        self.engine_work.ended(raft_group_id).await;
    }

    /// What the snapshot driver needs of this WAL (#669): the lagging
    /// groups and the maintenance passes of its core journals. It holds
    /// neither the root's lock nor any journal, so a driver that outlives
    /// the run keeps nothing open.
    pub fn maintenance(&self) -> WalMaintenance {
        WalMaintenance {
            lagging: self.lagging.clone(),
            journals: MaintainedJournals::Cores(Arc::downgrade(&self.core_writers)),
        }
    }

    /// Ends this run cleanly: closes every core writer and the meta
    /// journal's, each of which `fsync`s its journal, and only then closes
    /// the node fence for good and records a clean shutdown. Stop the Raft
    /// groups and the predecessor wait first; a write after this fails and
    /// no journal opens again. When a writer cannot close, the run is not
    /// recorded as clean. The root's lock of a clean run comes back as a
    /// [`CleanExit`], which the process holds until it exits.
    pub async fn shutdown(&self) -> Result<CleanExit, RaftWalError> {
        let slots = {
            let mut core_writers = self
                .core_writers
                .lock()
                .map_err(|_poisoned| RaftWalError::LockPoisoned)?;
            match std::mem::replace(&mut *core_writers, CoreWriterSlots::ShutDown) {
                CoreWriterSlots::Running(slots) => slots,
                CoreWriterSlots::ShutDown => {
                    return Err(RaftWalError::ShutDown {
                        root: self.root().to_owned(),
                    });
                }
            }
        };
        let mut writers = Vec::with_capacity(slots.len());
        let mut exits = Vec::with_capacity(slots.len());
        for (core, slot) in slots {
            let slot = slot
                .lock()
                .map_err(|_poisoned| RaftWalError::LockPoisoned)?;
            writers.extend(slot.writer.upgrade().map(|writer| (core, writer)));
            exits.extend(slot.exited.clone());
        }
        let closed = join_all(writers.iter().map(|(core, writer)| async move {
            writer
                .close()
                .await
                .map_err(|source| RaftWalError::CloseJournal {
                    core: CoreId(*core),
                    source,
                })
        }))
        .await;
        closed.into_iter().collect::<Result<Vec<()>, _>>()?;
        let meta = std::mem::replace(
            &mut *self
                .meta_writer
                .lock()
                .map_err(|_poisoned| RaftWalError::LockPoisoned)?,
            MetaWriterSlot::ShutDown,
        );
        let meta_closed = match meta {
            MetaWriterSlot::Open(writer) => {
                exits.push(writer.exited());
                writer
                    .close()
                    .await
                    .map_err(|source| RaftWalError::CloseMetaJournal { source })?;
                true
            }
            MetaWriterSlot::Unopened | MetaWriterSlot::ShutDown => false,
        };
        // A writer whose last handle is being dropped still drains its queued
        // batch. Only once every worker stopped can no journal change.
        for mut exited in exits {
            while exited.changed().await.is_ok() {}
        }
        let exit = self.node.record_clean()?;
        tracing::info!(
            root = %self.root().display(),
            cores = writers.len(),
            meta = meta_closed,
            "shut down the Raft WAL cleanly"
        );
        Ok(exit)
    }
}

/// The predecessor wait a run owes ([`RaftWal::predecessor_wait`]): where
/// it is measured from, and the record of its end. It holds neither the
/// root's lock nor any journal, so a wait task never keeps the run's WAL
/// alive, and it records the end only through a WAL that still runs.
#[derive(Debug, Clone)]
pub struct PredecessorWait {
    from: WaitFrom,
    root: PathBuf,
    fence: NodeFence,
    node: Weak<NodeWal>,
}

impl PredecessorWait {
    /// Where the wait is measured from.
    pub fn from(&self) -> WaitFrom {
        self.from
    }

    /// The run's fence, which carries the wait.
    pub fn fence(&self) -> &NodeFence {
        &self.fence
    }

    /// Records the wait finished, `fsync`ed, then ends it on the fence.
    /// Refused while the fence does not admit ([`RaftWalError::Fenced`]),
    /// and for good once the run shut down or its WAL is gone, as by a
    /// closed fence.
    pub async fn finish(&self) -> Result<(), RaftWalError> {
        match self.node.upgrade() {
            Some(node) => node.finish_wait().await,
            None => Err(RaftWalError::Fenced {
                root: self.root.clone(),
                status: ursula_runtime::node_fence::FenceStatus::Closed,
            }),
        }
    }
}

/// How a run's root stands in meta ([`RaftWal::binding`]): as the identity
/// file recorded it at start, and the durable record of its first binding.
/// Like [`PredecessorWait`], it holds neither the root's lock nor any
/// journal, and it records only through a WAL that still runs.
#[derive(Debug, Clone)]
pub(crate) struct BindingRecord {
    standing: RootStanding,
    root: PathBuf,
    node: Weak<NodeWal>,
}

impl BindingRecord {
    /// How the root stood in meta when the run opened it.
    pub(crate) fn standing_at_open(&self) -> RootStanding {
        self.standing
    }

    /// Records the root bound, `fsync`ed, unless it is already. Refused
    /// while the fence does not admit ([`RaftWalError::Fenced`]), and for
    /// good once the run shut down or its WAL is gone.
    pub(crate) async fn record_bound(&self) -> Result<(), RaftWalError> {
        match self.node.upgrade() {
            Some(node) => node.record_bound().await,
            None => Err(RaftWalError::Fenced {
                root: self.root.clone(),
                status: ursula_runtime::node_fence::FenceStatus::Closed,
            }),
        }
    }
}

/// The snapshot driver's handle on a node's Raft WAL ([`RaftWal::maintenance`]),
/// or the meta group's snapshot trigger's on the meta journal
/// ([`RaftWal::meta_maintenance`]).
#[derive(Debug, Clone)]
pub struct WalMaintenance {
    lagging: Arc<LaggingGroups>,
    journals: MaintainedJournals,
}

/// The meta journal's policy: `always`, whatever the data journals run
/// (`data`). A negative control turns it off (`Defence::MetaAlwaysSync`),
/// and the meta journal then runs the data journals' policy.
fn meta_fsync(data: WalFsync) -> WalFsync {
    if data != META_FSYNC
        && ursula_stream::sim_controls::off(ursula_stream::sim_controls::Defence::MetaAlwaysSync)
    {
        ursula_stream::sim_controls::note_fired(
            ursula_stream::sim_controls::Defence::MetaAlwaysSync,
        );
        return data;
    }
    META_FSYNC
}

/// The journals a [`WalMaintenance`] maintains.
#[derive(Debug, Clone)]
enum MaintainedJournals {
    /// Every open core journal.
    Cores(Weak<Mutex<CoreWriterSlots>>),
    /// The meta journal, once open.
    Meta(Weak<Mutex<MetaWriterSlot>>),
}

impl WalMaintenance {
    /// The groups whose live records keep old journal segments alive.
    pub fn lagging_groups(&self) -> Arc<LaggingGroups> {
        self.lagging.clone()
    }

    /// Runs one maintenance pass of every open core journal, or of the meta
    /// journal, as of `now` on the snapshot driver's clock (#669): each writer stamps the dead
    /// frames it learned of since its last pass with `now`, seals its active
    /// segment once that segment has held dead frames for `max_dirty_age`,
    /// and frees the oldest kept segment while a kept segment has, copying
    /// at most its pass budget. The writers have no timer of their own: the
    /// snapshot driver calls this, at most once per drive interval. Returns
    /// once every pass ran, with the first writer that could not take one.
    /// A core whose journal is opening right now is skipped until the next
    /// call, and a WAL that shut down or is gone has nothing to maintain.
    pub async fn maintain(
        &self,
        now: Instant,
        max_dirty_age: Duration,
    ) -> Result<(), RaftWalError> {
        match &self.journals {
            MaintainedJournals::Cores(core_writers) => {
                maintain_cores(core_writers, now, max_dirty_age).await
            }
            MaintainedJournals::Meta(meta_writer) => {
                maintain_meta(meta_writer, now, max_dirty_age).await
            }
        }
    }
}

async fn maintain_cores(
    core_writers: &Weak<Mutex<CoreWriterSlots>>,
    now: Instant,
    max_dirty_age: Duration,
) -> Result<(), RaftWalError> {
    let Some(core_writers) = core_writers.upgrade() else {
        return Ok(());
    };
    let slots = match &*core_writers
        .lock()
        .map_err(|_poisoned| RaftWalError::LockPoisoned)?
    {
        CoreWriterSlots::Running(slots) => slots.clone(),
        CoreWriterSlots::ShutDown => return Ok(()),
    };
    drop(core_writers);
    // Each pass is asked for while the writer's handle is held, and
    // the handle goes before the wait: holding the last one there would
    // make its drop wait for the writer and keep the journal locked
    // against a group that reopens meanwhile.
    let mut answers = Vec::with_capacity(slots.len());
    for (core, slot) in slots {
        // Opening a journal holds its slot through the replay, so a busy
        // slot is skipped rather than waited on.
        let slot = match slot.try_lock() {
            Ok(slot) => slot,
            Err(std::sync::TryLockError::WouldBlock) => continue,
            Err(std::sync::TryLockError::Poisoned(_poisoned)) => {
                return Err(RaftWalError::LockPoisoned);
            }
        };
        let Some(writer) = slot.writer.upgrade() else {
            continue;
        };
        drop(slot);
        let answer = writer.send_maintenance(now, max_dirty_age);
        drop(writer);
        answers.push((CoreId(core), answer));
    }
    let passes = join_all(answers.into_iter().map(|(core, answer)| async move {
        match answer {
            Ok(answer) => answer.ran().await,
            Err(source) => Err(source),
        }
        .map_err(|source| RaftWalError::MaintainJournal { core, source })
    }))
    .await;
    passes.into_iter().collect::<Result<Vec<()>, _>>().map(drop)
}

async fn maintain_meta(
    meta_writer: &Weak<Mutex<MetaWriterSlot>>,
    now: Instant,
    max_dirty_age: Duration,
) -> Result<(), RaftWalError> {
    let Some(meta_writer) = meta_writer.upgrade() else {
        return Ok(());
    };
    // Opening the journal holds the slot through its replay, so a busy slot
    // is skipped rather than waited on.
    let writer = match meta_writer.try_lock() {
        Ok(slot) => match &*slot {
            MetaWriterSlot::Open(writer) => writer.clone(),
            MetaWriterSlot::Unopened | MetaWriterSlot::ShutDown => return Ok(()),
        },
        Err(std::sync::TryLockError::WouldBlock) => return Ok(()),
        Err(std::sync::TryLockError::Poisoned(_poisoned)) => {
            return Err(RaftWalError::LockPoisoned);
        }
    };
    drop(meta_writer);
    let answer = writer.send_maintenance(now, max_dirty_age);
    drop(writer);
    match answer {
        Ok(answer) => answer.ran().await,
        Err(source) => Err(source),
    }
    .map_err(|source| RaftWalError::MaintainMetaJournal { source })
}

#[cfg(all(test, not(madsim)))]
mod tests {
    use ursula_shard::RaftGroupId;
    use ursula_shard::ShardId;

    use super::*;
    fn unique_test_dir(name: &str) -> PathBuf {
        static TEST_DIR_COUNTER: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(0);
        let ordinal = TEST_DIR_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "ursula-raft-{name}-{}-{}",
            std::process::id(),
            ordinal,
        ))
    }

    /// #741: a group that opens while the core's previous writer is still
    /// stopping, its last handle dropped and its worker not yet exited,
    /// waits for the worker instead of finding the journal locked.
    #[cfg(not(madsim))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_open_waits_for_the_previous_writer_to_stop() {
        let root = unique_test_dir("stopping-writer-open");
        let wal = RaftWal::start(
            &root,
            WalFsync::Never,
            &ursula_shard::StaticShardMap::new(1, 2).expect("valid topology"),
        )
        .expect("start");
        let placement = |group| ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(group),
        };
        let metrics = || ursula_runtime::RuntimeMetrics::new(1, 2).group_engine_metrics();
        // A slot whose writer is gone but whose lock is not yet released.
        let (stopping, released) = crate::rt::sync::watch::channel(());
        {
            let mut core_writers = wal.core_writers.lock().expect("slots");
            let CoreWriterSlots::Running(slots) = &mut *core_writers else {
                panic!("a started WAL is running");
            };
            *slots.entry(0).or_default().lock().expect("core 0") = CoreWriterState {
                writer: Weak::new(),
                held: HeldCore::Lazy,
                exited: None,
                released: Some(released),
            };
        }
        assert!(matches!(
            wal.open(placement(1), ursula_control::Admission::Create, metrics()),
            Err(RaftWalError::CoreWriterStopping {
                core: CoreId(0),
                ..
            })
        ));
        let opening = tokio::spawn({
            let wal = wal.clone();
            async move {
                wal.open_waiting(placement(1), ursula_control::Admission::Create, metrics())
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!opening.is_finished(), "the open waits for the writer");
        drop(stopping);
        let store = opening
            .await
            .expect("the open task")
            .expect("the open after the writer stopped");
        drop(store);
        wal.shutdown().await.expect("a clean shutdown");
        crate::tests::remove_test_path(&root);
    }

    /// A writer whose last handle is still draining it keeps a shutdown from
    /// recording the run as clean, so nothing writes the journal after that.
    #[cfg(not(madsim))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_waits_for_a_writer_whose_last_handle_is_dropping() {
        let root = unique_test_dir("draining-writer-shutdown");
        let wal = RaftWal::start(
            &root,
            WalFsync::Never,
            &ursula_shard::StaticShardMap::new(1, 1).expect("valid topology"),
        )
        .expect("start");
        let store = wal
            .open(
                ShardPlacement {
                    core_id: CoreId(0),
                    shard_id: ShardId(0),
                    raft_group_id: RaftGroupId(0),
                },
                ursula_control::Admission::Create,
                ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics(),
            )
            .expect("open a store");
        let exited = {
            let core_writers = wal.core_writers.lock().expect("slots");
            let CoreWriterSlots::Running(slots) = &*core_writers else {
                panic!("a started WAL is running");
            };
            slots[&0]
                .lock()
                .expect("core 0")
                .exited
                .clone()
                .expect("an opened writer")
        };
        // Dropping the last handle joins the worker, so it runs beside the
        // shutdown.
        let dropping = tokio::task::spawn_blocking(move || drop(store));
        wal.shutdown().await.expect("a clean shutdown");
        // The worker dropped its sender before the run was recorded clean.
        if let Ok(changed) = exited.has_changed() {
            panic!("the writer still runs after a clean shutdown (changed: {changed})");
        }
        dropping.await.expect("drop the last handle");
        crate::tests::remove_test_path(&root);
    }

    /// The meta journal's writer is held for the run: its group reopens on
    /// it once the previous store closed, and a shutdown closes it, with the
    /// store still held, before it records the run as clean.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn the_meta_writer_closes_before_the_clean_record() {
        let root = tempfile::tempdir().unwrap();
        let topology = ursula_shard::StaticShardMap::new(1, 1).unwrap();
        let wal = RaftWal::start(root.path(), WalFsync::Never, &topology).unwrap();
        let metrics = || ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics();
        let store = wal.open_meta(metrics()).unwrap();
        let exited = match &*wal.meta_writer.lock().unwrap() {
            MetaWriterSlot::Open(writer) => writer.exited(),
            other => panic!("an opened meta writer, not {other:?}"),
        };
        assert!(matches!(
            wal.open_meta(metrics()),
            Err(MetaOpenError::OpenGroup(
                CoreJournalError::GroupAlreadyOpen { .. }
            ))
        ));
        drop(store);
        let store = wal
            .open_meta(metrics())
            .expect("the meta group reopens on the held writer");
        wal.shutdown().await.unwrap();
        if let Ok(changed) = exited.has_changed() {
            panic!("the meta writer still runs after a clean shutdown (changed: {changed})");
        }
        let recorded = crate::log_store::state_file::read::<crate::log_store::RunState>(
            crate::log_store::StateFileKind::RunState,
            &root.path().join(crate::log_store::RUN_STATE_FILE),
        )
        .unwrap()
        .unwrap();
        assert_eq!(recorded.status, crate::log_store::RunStatus::Clean);
        assert!(matches!(
            wal.open_meta(metrics()),
            Err(MetaOpenError::ShutDown { .. })
        ));
        drop(store);
    }

    /// A core recovering its journal does not hold up another core's.
    #[test]
    fn cores_open_their_journals_independently() {
        let root = unique_test_dir("parallel-core-open");
        let factory = RaftWal::start(
            &root,
            WalFsync::Always,
            &ursula_shard::StaticShardMap::new(2, 2).expect("valid topology"),
        )
        .expect("start");
        let metrics = ursula_runtime::RuntimeMetrics::new(2, 2).group_engine_metrics();
        let placement = |core: u16, group: u32| ShardPlacement {
            core_id: CoreId(core),
            shard_id: ShardId(group),
            raft_group_id: RaftGroupId(group),
        };
        // Stand in for a long recovery of core 0 by holding its slot.
        let slot = {
            let mut core_writers = factory.core_writers.lock().expect("slots");
            let CoreWriterSlots::Running(slots) = &mut *core_writers else {
                panic!("a started WAL is running");
            };
            slots.entry(0).or_default().clone()
        };
        let held = slot.lock().expect("hold core 0");

        // Core 0's slot stays held on this thread, so this open would never
        // return if it waited for core 0.
        let store = factory
            .open(placement(1, 1), ursula_control::Admission::Create, metrics)
            .expect("core 1 opens while core 0 recovers");
        drop(store);
        drop(held);
        crate::tests::remove_test_path(&root);
    }
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn opening_a_group_preserves_typed_journal_and_shutdown_errors() {
        let root = tempfile::tempdir().unwrap();
        let topology = ursula_shard::StaticShardMap::new(1, 1).unwrap();
        let wal = RaftWal::start(root.path(), WalFsync::Always, &topology).unwrap();
        let placement = ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(0),
        };
        let metrics = ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics();
        let store = wal
            .open(
                placement,
                ursula_control::Admission::Create,
                metrics.clone(),
            )
            .unwrap();
        let duplicate = wal
            .open(
                placement,
                ursula_control::Admission::Create,
                metrics.clone(),
            )
            .unwrap_err();
        assert!(matches!(&duplicate, RaftWalError::OpenGroup {
            placement: actual,
            source: crate::log_store::CoreJournalError::GroupAlreadyOpen { raft_group_id, .. },
        } if *actual == placement && *raft_group_id == placement.raft_group_id));
        assert!(
            std::error::Error::source(&duplicate)
                .unwrap()
                .downcast_ref::<crate::log_store::CoreJournalError>()
                .is_some()
        );
        wal.shutdown().await.unwrap();
        assert!(matches!(
            wal.open(placement, ursula_control::Admission::Create, metrics),
            Err(RaftWalError::ShutDown { .. })
        ));
        drop(store);
    }

    /// A maintenance pass that is still answering holds no handle on the
    /// core's writer (#669): once the group's last store goes, the writer
    /// goes too, and the group opens again at once instead of finding its
    /// journal locked.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_waiting_maintenance_pass_keeps_no_writer() {
        let root = tempfile::tempdir().unwrap();
        let topology = ursula_shard::StaticShardMap::new(1, 1).unwrap();
        let wal = RaftWal::start(root.path(), WalFsync::Never, &topology).unwrap();
        let placement = ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(0),
        };
        let metrics = ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics();
        let store = wal
            .open(
                placement,
                ursula_control::Admission::Create,
                metrics.clone(),
            )
            .unwrap();
        let maintenance = wal.maintenance();
        let mut pass =
            std::pin::pin!(maintenance.maintain(Instant::now(), Duration::from_secs(60)));
        // One poll asks for the pass and leaves it waiting, or done.
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        let polled = pass.as_mut().poll(&mut cx);
        drop(store);
        let writer_gone = {
            let core_writers = wal.core_writers.lock().unwrap();
            let CoreWriterSlots::Running(slots) = &*core_writers else {
                panic!("a started WAL is running");
            };
            slots[&0].lock().unwrap().writer.upgrade().is_none()
        };
        assert!(
            writer_gone,
            "the pass kept the writer after its last store went ({polled:?})"
        );
        let reopened = wal
            .open(placement, ursula_control::Admission::Create, metrics)
            .expect("the group opens again");
        if polled.is_pending() {
            let ran = pass.await;
            assert!(
                matches!(
                    ran,
                    Ok(())
                        | Err(RaftWalError::MaintainJournal {
                            source: crate::log_store::CoreJournalError::WriterStopped { .. },
                            ..
                        })
                ),
                "{ran:?}"
            );
        }
        drop(reopened);
        wal.shutdown().await.unwrap();
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn opening_a_core_preserves_its_io_source() {
        let root = tempfile::tempdir().unwrap();
        let topology = ursula_shard::StaticShardMap::new(1, 1).unwrap();
        let wal = RaftWal::start(root.path(), WalFsync::Always, &topology).unwrap();
        let placement = ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(0),
        };
        std::fs::write(wal.core_dir(CoreId(0)), b"not a directory").unwrap();
        let metrics = ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics();
        let error = wal
            .open(placement, ursula_control::Admission::Create, metrics)
            .unwrap_err();
        assert!(matches!(&error, RaftWalError::OpenCore {
            core: CoreId(0), source: crate::log_store::CoreJournalError::Io { source, .. },
        } if source.kind() == std::io::ErrorKind::AlreadyExists));
        assert!(
            std::error::Error::source(&error)
                .unwrap()
                .downcast_ref::<crate::log_store::CoreJournalError>()
                .is_some()
        );
        wal.shutdown().await.unwrap();
    }

    fn leased_start(
        root: &Path,
        topology: &ursula_shard::StaticShardMap,
    ) -> Result<RaftWal, RaftWalError> {
        RaftWal::start_in_cluster(
            root,
            JournalTuning::new(WalFsync::Never),
            topology,
            NodeId::new(1),
            None,
            NodeFence::leased(Duration::from_secs(5)),
        )
    }

    /// A clean end keeps the root locked until the process lets go of it
    /// (`CleanExit`), whatever still holds the run's WAL, and the run's
    /// predecessor wait and its binding record nothing after it: a record
    /// after the end would rewrite a successor's identity file. Catches a
    /// `record_clean` that drops the lock, or a wait or binding record that
    /// skips the fence check.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_clean_end_keeps_the_root_locked_and_records_no_wait_after_it() {
        let dir = tempfile::tempdir().unwrap();
        let topology = ursula_shard::StaticShardMap::new(1, 1).unwrap();
        let wal = leased_start(dir.path(), &topology).unwrap();
        // A fresh root owes its wait from its binding.
        let wait = wal.predecessor_wait().expect("a fresh root owes a wait");
        let binding = wal.binding();
        let held = wal.clone();
        let exit = wal.shutdown().await.unwrap();
        drop(wal);
        drop(held);
        assert!(matches!(
            leased_start(dir.path(), &topology),
            Err(RaftWalError::Locked { .. })
        ));
        assert!(matches!(
            wait.finish().await,
            Err(RaftWalError::Fenced {
                status: ursula_runtime::node_fence::FenceStatus::Closed,
                ..
            })
        ));
        // Nor is a binding recorded after it.
        assert!(matches!(
            binding.record_bound().await,
            Err(RaftWalError::Fenced {
                status: ursula_runtime::node_fence::FenceStatus::Closed,
                ..
            })
        ));
        drop(exit);
        let next = leased_start(dir.path(), &topology).unwrap();
        assert_eq!(
            next.opening().previous_run,
            crate::wal::diagnostics::PreviousRun::Clean
        );
        drop(next.shutdown().await.unwrap());
    }

    /// The lease's probe file opens at the start, under the root's lock, and
    /// a start whose probe file does not open fails: the probe never opens
    /// it by path later, where another attachment of the volume may be.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_start_whose_probe_file_does_not_open_fails() {
        let dir = tempfile::tempdir().unwrap();
        let topology = ursula_shard::StaticShardMap::new(1, 1).unwrap();
        std::fs::create_dir(dir.path().join(super::super::lease::PROBE_FILE)).unwrap();
        assert!(matches!(
            leased_start(dir.path(), &topology),
            Err(RaftWalError::LeaseProbe { .. })
        ));
    }
}
