//! Node WAL lifecycle and per-core journal ownership.

use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;

use futures_util::future::join_all;
use ursula_config::WalFsync;
use ursula_runtime::GroupEngineMetrics;
use ursula_shard::CoreId;
use ursula_shard::ShardPlacement;

use crate::log_store::CoreFileLogWriter;
use crate::log_store::CoreJournalOptions;
use crate::log_store::JournalTuning;
use crate::log_store::LaggingGroups;
use crate::log_store::NodeWal;
use crate::log_store::RaftGroupFileLogStore;
use crate::log_store::RaftWalError;
use crate::log_store::RecoveryState;
use crate::log_store::WalOpening;
use crate::log_store::WriterExited;
use crate::log_store::core_dir;

/// The open writer of one core's journal, if any, and the signal that its
/// worker stopped, which outlives the writer's last handle.
type CoreWriterSlot = Arc<Mutex<CoreWriterState>>;

#[derive(Debug, Default)]
struct CoreWriterState {
    writer: Weak<CoreFileLogWriter>,
    exited: Option<WriterExited>,
}

/// The core writers of a running WAL, by core.
#[derive(Debug)]
enum CoreWriterSlots {
    Running(BTreeMap<u16, CoreWriterSlot>),
    /// [`RaftWal::shutdown`] closed every writer; no
    /// journal opens again in this run.
    ShutDown,
}

/// Opens each group's durable log store over its core's shared journal, for
/// one run of the node's Raft WAL.
///
/// [`RaftWal::start`] begins the run: it reads the run
/// state the previous run left, decides how the journals open
/// ([`WalOpening`]) and records this run before any journal write.
/// [`RaftWal::shutdown`] ends it cleanly.
#[derive(Debug, Clone)]
pub struct RaftWal {
    node: Arc<NodeWal>,
    tuning: JournalTuning,
    lagging: Arc<LaggingGroups>,
    /// One slot per core. Opening a journal holds only its core's slot, so
    /// cores recover their journals in parallel.
    core_writers: Arc<Mutex<CoreWriterSlots>>,
}

impl RaftWal {
    /// Starts a run of the Raft WAL under `root` with the `fsync` policy and
    /// the default segment size and entry cache.
    pub fn start(
        root: impl Into<PathBuf>,
        fsync: WalFsync,
        topology: &ursula_shard::StaticShardMap,
    ) -> Result<Self, RaftWalError> {
        Self::start_with(root, JournalTuning::new(fsync), topology)
    }

    /// Starts a run of the Raft WAL under `root` with `tuning`.
    pub fn start_with(
        root: impl Into<PathBuf>,
        tuning: JournalTuning,
        topology: &ursula_shard::StaticShardMap,
    ) -> Result<Self, RaftWalError> {
        Ok(Self {
            node: Arc::new(NodeWal::start(root.into(), tuning.fsync, topology)?),
            tuning,
            lagging: Arc::new(LaggingGroups::default()),
            core_writers: Arc::new(Mutex::new(CoreWriterSlots::Running(BTreeMap::new()))),
        })
    }

    pub fn root(&self) -> &Path {
        self.node.root()
    }

    pub fn fsync(&self) -> WalFsync {
        self.node.fsync()
    }

    /// The groups whose live records keep old journal segments alive, for
    /// the snapshot driver.
    pub fn lagging_groups(&self) -> Arc<LaggingGroups> {
        self.lagging.clone()
    }

    /// How this run opens the journals the previous run left.
    pub fn opening(&self) -> WalOpening {
        self.node.opening()
    }

    /// Whether this node's logs may be missing entries it acknowledged.
    pub fn recovery_state(&self) -> RecoveryState {
        self.node.opening().recovery
    }

    /// The journal directory of core `core_id`.
    pub fn core_dir(&self, core_id: CoreId) -> PathBuf {
        core_dir(self.root(), core_id.0)
    }

    pub(crate) fn snapshot_metadata_path(&self, placement: ShardPlacement) -> PathBuf {
        self.root()
            .join(format!("core-{}", placement.core_id.0))
            .join(format!("group-{}.snapshot.json", placement.raft_group_id.0))
    }

    fn core_writer(
        &self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
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
        if let Some(writer) = slot.writer.upgrade() {
            return Ok(writer);
        }

        let opening = self.node.opening();
        let writer =
            CoreFileLogWriter::open(self.core_dir(placement.core_id), CoreJournalOptions {
                previous_run: opening.previous_run,
                core: placement.core_id,
                tuning: self.tuning,
                recovery_epoch: opening.recovery_epoch,
                run_state: self.node.run_state().clone(),
                node_recovery: opening.recovery,
                lagging: self.lagging.clone(),
                metrics: Some((placement, metrics)),
            })
            .map_err(|source| RaftWalError::OpenCore {
                core: placement.core_id,
                source,
            })?;
        *slot = CoreWriterState {
            writer: Arc::downgrade(&writer),
            exited: Some(writer.exited()),
        };
        Ok(writer)
    }

    pub fn open(
        &self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> Result<Arc<RaftGroupFileLogStore>, RaftWalError> {
        let core_writer = self.core_writer(placement, metrics.clone())?;
        RaftGroupFileLogStore::open(placement, metrics, core_writer)
            .map_err(|source| RaftWalError::OpenGroup { placement, source })
    }

    /// Ends this run cleanly: closes every core writer, each of which
    /// `fsync`s its journal, and only then records a clean shutdown. Stop the
    /// Raft groups first; a write after this fails and no journal opens
    /// again. When a writer cannot close, the run is not recorded as clean.
    pub async fn shutdown(&self) -> Result<(), RaftWalError> {
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
                    core: *core,
                    source,
                })
        }))
        .await;
        closed.into_iter().collect::<Result<Vec<()>, _>>()?;
        // A writer whose last handle is being dropped still drains its queued
        // batch. Only once every worker stopped can no journal change.
        for mut exited in exits {
            while exited.changed().await.is_ok() {}
        }
        self.node.record_clean()?;
        tracing::info!(
            root = %self.root().display(),
            cores = writers.len(),
            "shut down the Raft WAL cleanly"
        );
        Ok(())
    }
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
            .open(placement(1, 1), metrics)
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
        let store = wal.open(placement, metrics.clone()).unwrap();
        let duplicate = wal.open(placement, metrics.clone()).unwrap_err();
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
            wal.open(placement, metrics),
            Err(RaftWalError::ShutDown { .. })
        ));
        drop(store);
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
        let error = wal.open(placement, metrics).unwrap_err();
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
}
