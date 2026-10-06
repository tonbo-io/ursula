//! Durable meta-group journal and snapshot storage.
//!
//! The control log uses the same checksummed frames as data logs. A snapshot
//! is fsynced and atomically installed before covered log entries can be
//! purged. The journal lock covers both files and survives journal replacement.

use std::fmt::Debug;
use std::fs;
use std::io;
use std::ops::RangeBounds;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

use openraft::OptionalSend;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;
use openraft::storage::LogState;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use serde::Deserialize;
use serde::Serialize;
use ursula_runtime::journal;

use super::JournalLock;
use super::MemoryRaftLogStoreInner;
use super::WireCodec;
use super::ensure_consecutive_entries;
#[cfg(not(madsim))]
use super::ensure_consecutive_log;
use super::ensure_log_append_boundary;
use super::spawn_log_store_blocking;
use crate::meta::MetaCurrentSnapshot;
use crate::meta::MetaRaftTypeConfig;

type MetaLog = MemoryRaftLogStoreInner<MetaRaftTypeConfig>;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum MetaLogRecord {
    SaveVote(VoteOf<MetaRaftTypeConfig>),
    SaveCommitted(Option<LogIdOf<MetaRaftTypeConfig>>),
    Append(Vec<EntryOf<MetaRaftTypeConfig>>),
    TruncateAfter(Option<LogIdOf<MetaRaftTypeConfig>>),
    Purge(LogIdOf<MetaRaftTypeConfig>),
}

struct MetaFileInner {
    log: MetaLog,
    writer: journal::JournalWriter,
    snapshot: Option<MetaCurrentSnapshot>,
    failed: bool,
}

/// One durable meta replica. Use a dedicated journal path, not a data group's
/// shared core journal. Opening the same path twice is rejected by its lock.
pub struct MetaRaftFileLogStore {
    path: PathBuf,
    snapshot_path: PathBuf,
    inner: Mutex<MetaFileInner>,
    _lock: JournalLock,
}

impl Debug for MetaRaftFileLogStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaRaftFileLogStore")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl MetaRaftFileLogStore {
    #[cfg(madsim)]
    pub fn open(_path: impl Into<PathBuf>) -> io::Result<Arc<Self>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "durable meta files are unavailable in simulation; use the simulated log store",
        ))
    }

    #[cfg(not(madsim))]
    pub fn open(path: impl Into<PathBuf>) -> io::Result<Arc<Self>> {
        let path = path.into();
        let path = if path.is_absolute() {
            path
        } else {
            std::env::current_dir()?.join(path)
        };
        let lock = JournalLock::acquire(&path)?;
        let mut snapshot_name = path.as_os_str().to_owned();
        snapshot_name.push(".snapshot");
        let snapshot_path = PathBuf::from(snapshot_name);
        let snapshot = if snapshot_path.exists() {
            let mut records =
                journal::replay::<WireCodec<MetaCurrentSnapshot>>(&snapshot_path)?.into_iter();
            let snapshot = records
                .next()
                .ok_or_else(|| invalid("durable meta snapshot has no complete checkpoint"))?;
            if records.next().is_some() {
                return Err(invalid("durable meta snapshot has multiple checkpoints"));
            }
            Some(snapshot)
        } else {
            None
        };
        if snapshot.is_some() && !path.exists() {
            return Err(invalid(
                "meta journal is missing beside an existing snapshot",
            ));
        }
        if path.exists() && fs::metadata(&path)?.len() == 0 {
            return Err(invalid("existing meta journal is empty"));
        }
        let mut log = MetaLog::default();
        journal::replay_each::<WireCodec<MetaLogRecord>>(&path, |record| {
            apply_record(&mut log, record)
        })?;
        ensure_consecutive_log::<MetaRaftTypeConfig>(&log.entries)?;
        if let Some(purged) = log.last_purged_log_id
            && snapshot.as_ref().and_then(|s| s.meta.last_log_id) < Some(purged)
        {
            return Err(invalid(
                "purged meta log is not covered by a durable snapshot",
            ));
        }
        let mut writer = journal::JournalWriter::new(!path.exists());
        writer.ensure_created(&path)?;
        writer.sync(&path)?;
        if let Some(parent) = path.parent() {
            fs::File::open(parent)?.sync_all()?;
        }
        Ok(Arc::new(Self {
            path,
            snapshot_path,
            inner: Mutex::new(MetaFileInner {
                log,
                writer,
                snapshot,
                failed: false,
            }),
            _lock: lock,
        }))
    }

    fn lock(&self) -> io::Result<MutexGuard<'_, MetaFileInner>> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| io::Error::other("durable meta storage mutex poisoned"))?;
        if inner.failed {
            return Err(io::Error::other(
                "durable meta storage failed; reopen to recover",
            ));
        }
        Ok(inner)
    }

    pub(crate) fn snapshot(&self) -> io::Result<Option<MetaCurrentSnapshot>> {
        Ok(self.lock()?.snapshot.clone())
    }

    pub(crate) async fn persist_snapshot(
        self: &Arc<Self>,
        snapshot: MetaCurrentSnapshot,
    ) -> io::Result<()> {
        let store = self.clone();
        spawn_log_store_blocking(None, move || {
            let mut inner = store.lock()?;
            if let Some(current) = &inner.snapshot
                && current.meta.last_log_id > snapshot.meta.last_log_id
            {
                return Ok(());
            }
            let result = replace_journal(&store.snapshot_path, std::iter::once(snapshot.clone()));
            if result.is_err() {
                inner.failed = true;
            }
            result?;
            inner.snapshot = Some(snapshot);
            Ok(())
        })
        .await
    }

    async fn write(self: &Arc<Self>, record: MetaLogRecord) -> io::Result<()> {
        let store = self.clone();
        spawn_log_store_blocking(None, move || {
            let mut inner = store.lock()?;
            if let MetaLogRecord::Purge(purged) = &record
                && inner.snapshot.as_ref().and_then(|s| s.meta.last_log_id) < Some(*purged)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "cannot purge meta entries without a durable covering snapshot",
                ));
            }
            let mut next = inner.log.clone();
            apply_record(&mut next, record.clone())?;
            let result = inner
                .writer
                .append::<WireCodec<MetaLogRecord>>(&store.path, &record)
                .and_then(|()| inner.writer.sync(&store.path));
            if result.is_err() {
                inner.failed = true;
            }
            result?;
            inner.log = next;
            if matches!(record, MetaLogRecord::Purge(_)) {
                // Close the old inode before replacement; all readers/mutations
                // remain serialized under this lock.
                inner.writer = journal::JournalWriter::new(false);
                let result = replace_journal(&store.path, compact_records(&inner.log));
                if result.is_err() {
                    inner.failed = true;
                }
                result?;
            }
            Ok(())
        })
        .await
    }
}

#[cfg(all(test, not(madsim)))]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::process::Command;
    use std::time::Duration;

    use futures_util::stream;
    use openraft::BasicNode;
    use openraft::Config;
    use openraft::EntryPayload;
    use openraft::LogId;
    use openraft::RaftTypeConfig;
    use openraft::alias::SnapshotMetaOf;
    use openraft::storage::RaftSnapshotBuilder;
    use openraft::storage::RaftStateMachine;
    use openraft::type_config::TypeConfigExt;
    use openraft::vote::RaftLeaderId;
    use ursula_control::ControlCommand;
    use ursula_control::ControlPlaneState;
    use ursula_control::ControlResponse;
    use ursula_shard::RaftGroupId;

    use super::*;
    use crate::MetaRaftHandle;
    use crate::MetaRaftStateMachine;
    use crate::SingleNodeRaftNetworkFactory;

    fn log_id(index: u64) -> LogIdOf<MetaRaftTypeConfig> {
        LogId {
            leader_id: <MetaRaftTypeConfig as RaftTypeConfig>::LeaderId::new(1, 1),
            index,
        }
    }

    fn blank(index: u64) -> EntryOf<MetaRaftTypeConfig> {
        EntryOf::<MetaRaftTypeConfig>::new(log_id(index), EntryPayload::Blank)
    }

    fn snapshot(index: u64) -> MetaCurrentSnapshot {
        MetaCurrentSnapshot {
            meta: SnapshotMetaOf::<MetaRaftTypeConfig> {
                last_log_id: Some(log_id(index)),
                last_membership: Default::default(),
                snapshot_id: format!("test-{index}"),
            },
            bytes: serde_json::to_vec(&ControlPlaneState::default()).unwrap(),
        }
    }

    #[tokio::test]
    async fn durable_meta_log_recovers_vote_committed_and_truncated_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.wal");
        let mut store = MetaRaftFileLogStore::open(&path).unwrap();
        let vote = openraft::Vote::new_committed(7, 1);
        store.save_vote(&vote).await.unwrap();
        let (tx, rx) = MetaRaftTypeConfig::oneshot();
        store
            .append([blank(1), blank(2), blank(3)], IOFlushed::signal(tx))
            .await
            .unwrap();
        rx.await.unwrap().unwrap();
        store.save_committed(Some(log_id(2))).await.unwrap();
        store.truncate_after(Some(log_id(2))).await.unwrap();
        assert!(store.truncate_after(Some(log_id(1))).await.is_err());
        drop(store);

        let mut store = MetaRaftFileLogStore::open(&path).unwrap();
        assert_eq!(store.read_vote().await.unwrap(), Some(vote));
        assert_eq!(store.read_committed().await.unwrap(), Some(log_id(2)));
        assert_eq!(
            store.get_log_state().await.unwrap().last_log_id,
            Some(log_id(2))
        );
        assert_eq!(store.try_get_log_entries(1..=9).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn durable_meta_purge_requires_snapshot_and_compaction_keeps_appending() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.wal");
        let mut store = MetaRaftFileLogStore::open(&path).unwrap();
        assert_eq!(
            MetaRaftFileLogStore::open(&path).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        store
            .append([blank(1), blank(2)], IOFlushed::noop())
            .await
            .unwrap();
        assert_eq!(
            store.purge(log_id(1)).await.unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(store.try_get_log_entries(..).await.unwrap().len(), 2);
        store.persist_snapshot(snapshot(1)).await.unwrap();
        store.purge(log_id(1)).await.unwrap();
        assert!(MetaRaftFileLogStore::open(&path).is_err());
        store.append([blank(3)], IOFlushed::noop()).await.unwrap();
        assert!(store.append([blank(5)], IOFlushed::noop()).await.is_err());
        drop(store);

        let mut store = MetaRaftFileLogStore::open(&path).unwrap();
        let logs = store.try_get_log_entries(..).await.unwrap();
        assert_eq!(logs.iter().map(|e| e.index()).collect::<Vec<_>>(), vec![
            2, 3
        ]);
        assert_eq!(
            store.get_log_state().await.unwrap().last_purged_log_id,
            Some(log_id(1))
        );
        assert_eq!(
            MetaRaftStateMachine::open_durable(store.clone())
                .unwrap()
                .applied_log_id(),
            Some(log_id(1))
        );
        drop(store);
        fs::remove_file(dir.path().join("meta.wal.snapshot")).unwrap();
        assert_eq!(
            MetaRaftFileLogStore::open(&path).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[tokio::test]
    async fn durable_meta_recovers_torn_journal_tail_and_rejects_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.wal");
        let mut store = MetaRaftFileLogStore::open(&path).unwrap();
        store.append([blank(1)], IOFlushed::noop()).await.unwrap();
        let original_len = fs::metadata(&path).unwrap().len();
        drop(store);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[10, 0, 0]).unwrap();
        file.sync_all().unwrap();
        drop(file);
        let mut store = MetaRaftFileLogStore::open(&path).unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), original_len);
        store.append([blank(2)], IOFlushed::noop()).await.unwrap();
        drop(store);
        let mut bytes = fs::read(&path).unwrap();
        bytes[24] ^= 1;
        fs::write(&path, bytes).unwrap();
        assert_eq!(
            MetaRaftFileLogStore::open(&path).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[tokio::test]
    async fn durable_meta_snapshots_keep_intent_and_do_not_regress() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.wal");
        let store = MetaRaftFileLogStore::open(&path).unwrap();
        let mut machine = MetaRaftStateMachine::open_durable(store.clone()).unwrap();
        let mut commands = (1..=4)
            .map(|node_id| ControlCommand::RegisterNode {
                node_id,
                client_url: format!("http://node-{node_id}:4437"),
                cluster_url: format!("http://node-{node_id}:4438"),
                labels: BTreeMap::new(),
                now_ms: 1,
            })
            .collect::<Vec<_>>();
        commands.push(ControlCommand::SeedPlacement {
            raft_group_id: RaftGroupId(0),
            voters: BTreeSet::from([1, 2, 3]),
            now_ms: 2,
        });
        commands.push(ControlCommand::BeginMigration {
            raft_group_id: RaftGroupId(0),
            target_voters: BTreeSet::from([1, 2, 4]),
            retain_removed: false,
            now_ms: 3,
        });
        for (index, command) in (1..).zip(commands) {
            machine
                .apply(stream::iter([Ok((
                    EntryOf::<MetaRaftTypeConfig>::new(
                        log_id(index),
                        EntryPayload::Normal(command),
                    ),
                    None,
                ))]))
                .await
                .unwrap();
        }
        let mut old = machine.get_snapshot_builder().await;
        machine
            .apply(stream::iter([Ok((
                EntryOf::<MetaRaftTypeConfig>::new(
                    log_id(7),
                    EntryPayload::Normal(ControlCommand::FinishMigration {
                        migration_id: 1,
                        success: false,
                        now_ms: 4,
                    }),
                ),
                None,
            ))]))
            .await
            .unwrap();
        machine
            .get_snapshot_builder()
            .await
            .build_snapshot()
            .await
            .unwrap();
        old.build_snapshot().await.unwrap();
        assert_eq!(
            machine
                .get_current_snapshot()
                .await
                .unwrap()
                .unwrap()
                .meta
                .last_log_id,
            Some(log_id(7))
        );
        drop(old);
        drop(machine);
        drop(store);
        let store = MetaRaftFileLogStore::open(&path).unwrap();
        let restored = MetaRaftStateMachine::open_durable(store).unwrap();
        assert_eq!(restored.applied_log_id(), Some(log_id(7)));
        assert_eq!(restored.state().active_migration, None);
        assert_eq!(restored.state().migrations.len(), 1);
        assert_eq!(restored.state().next_migration_id, 2);
    }

    #[tokio::test]
    async fn durable_meta_snapshot_write_failure_closes_storage_without_publishing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.wal");
        let mut store = MetaRaftFileLogStore::open(&path).unwrap();
        let mut machine = MetaRaftStateMachine::open_durable(store.clone()).unwrap();
        machine
            .apply(stream::iter([Ok((blank(1), None))]))
            .await
            .unwrap();
        let temporary = dir.path().join("meta.wal.snapshot.tmp");
        fs::create_dir(&temporary).unwrap();
        let mut builder = machine.get_snapshot_builder().await;
        assert!(builder.build_snapshot().await.is_err());
        assert!(machine.get_current_snapshot().await.unwrap().is_none());
        assert!(
            store
                .save_vote(&openraft::Vote::new_committed(1, 1))
                .await
                .is_err()
        );
        drop(builder);
        drop(machine);
        drop(store);
        fs::remove_dir(&temporary).unwrap();
        assert!(
            MetaRaftFileLogStore::open(&path)
                .unwrap()
                .snapshot()
                .unwrap()
                .is_none()
        );
    }

    fn crash_config() -> Arc<Config> {
        Arc::new(
            Config {
                cluster_name: "durable-meta-process-crash".to_owned(),
                heartbeat_interval: 10,
                election_timeout_min: 30,
                election_timeout_max: 60,
                max_in_snapshot_log_to_keep: 0,
                ..Config::default()
            }
            .validate()
            .unwrap(),
        )
    }

    // Invoked by the parent with an isolated path. Exit deliberately skips
    // Raft shutdown and all Rust destructors, simulating process loss after
    // durable publication or compaction/unsnapshotted writes.
    #[test]
    fn durable_meta_crash_child() {
        let Some(path) = std::env::var_os("URSULA_META_CRASH_TEST_PATH") else {
            return;
        };
        let after_purge = std::env::var_os("URSULA_META_CRASH_AFTER_PURGE").is_some();
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async move {
                let handle = MetaRaftHandle::new_durable_node_with_network(
                    1,
                    crash_config(),
                    SingleNodeRaftNetworkFactory,
                    PathBuf::from(path),
                )
                .await
                .unwrap();
                handle
                    .initialize_membership(BTreeMap::from([(1, BasicNode::new("meta-local"))]))
                    .await
                    .unwrap();
                handle
                    .wait_for_current_leader(1, Duration::from_secs(5))
                    .await
                    .unwrap();
                for id in 1..=4 {
                    assert_eq!(
                        handle
                            .register_node(
                                crate::MetaNodeRegistration::new(
                                    id,
                                    format!("http://node{id}:4437"),
                                    format!("http://node{id}:4438")
                                ),
                                1
                            )
                            .await
                            .unwrap(),
                        ControlResponse::Ok
                    );
                }
                handle
                    .write(ControlCommand::SeedPlacement {
                        raft_group_id: RaftGroupId(0),
                        voters: BTreeSet::from([1, 2, 3]),
                        now_ms: 2,
                    })
                    .await
                    .unwrap();
                assert_eq!(
                    handle
                        .write(ControlCommand::BeginMigration {
                            raft_group_id: RaftGroupId(0),
                            target_voters: BTreeSet::from([1, 2, 4]),
                            retain_removed: false,
                            now_ms: 3,
                        })
                        .await
                        .unwrap(),
                    ControlResponse::MigrationStarted { migration_id: 1 }
                );
                let applied = handle
                    .with_state_machine(|sm| Box::pin(async move { sm.applied_log_id().unwrap() }))
                    .await
                    .unwrap();
                let raft = handle.raft_handle();
                raft.trigger().snapshot().await.unwrap();
                raft.wait(Some(Duration::from_secs(5)))
                    .snapshot(applied, "crash checkpoint")
                    .await
                    .unwrap();
                if after_purge {
                    raft.trigger().purge_log(applied.index).await.unwrap();
                    raft.wait(Some(Duration::from_secs(5)))
                        .purged(Some(applied), "crash compaction")
                        .await
                        .unwrap();
                    assert_eq!(
                        handle.finish_migration(1, false, 4).await.unwrap(),
                        ControlResponse::Ok
                    );
                }
                std::process::exit(91);
            });
    }

    #[tokio::test]
    async fn durable_meta_process_crash_recovers_intent_and_post_snapshot_log() {
        for after_purge in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("meta.wal");
            let child_path = path.clone();
            let output = tokio::task::spawn_blocking(move || {
                let mut command = Command::new(std::env::current_exe().unwrap());
                command
                    .arg("--exact")
                    .arg("log_store::meta::tests::durable_meta_crash_child")
                    .arg("--nocapture")
                    .env("URSULA_META_CRASH_TEST_PATH", child_path)
                    .env_remove("URSULA_META_CRASH_AFTER_PURGE");
                if after_purge {
                    command.env("URSULA_META_CRASH_AFTER_PURGE", "1");
                }
                command.output().unwrap()
            })
            .await
            .unwrap();
            assert_eq!(
                output.status.code(),
                Some(91),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let handle = MetaRaftHandle::new_durable_node_with_network(
                1,
                crash_config(),
                SingleNodeRaftNetworkFactory,
                &path,
            )
            .await
            .unwrap();
            assert!(handle.raft_handle().is_initialized().await.unwrap());
            handle
                .wait_for_current_leader(1, Duration::from_secs(5))
                .await
                .unwrap();
            let state = handle.read_state(Clone::clone).await.unwrap();
            assert_eq!(state.nodes.len(), 4);
            assert_eq!(
                state.active_migration,
                if after_purge { None } else { Some(1) }
            );
            assert_eq!(state.migrations.len(), 1);
            assert_eq!(state.next_migration_id, 2);
            assert_eq!(
                state.placements[&RaftGroupId(0)].voters,
                BTreeSet::from([1, 2, 3])
            );
            handle.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn durable_meta_real_raft_restart_recovers_after_snapshot_and_purge() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.wal");
        let config = Arc::new(
            Config {
                cluster_name: "durable-meta-restart".to_owned(),
                heartbeat_interval: 10,
                election_timeout_min: 30,
                election_timeout_max: 60,
                max_in_snapshot_log_to_keep: 0,
                ..Config::default()
            }
            .validate()
            .unwrap(),
        );
        let handle = MetaRaftHandle::new_durable_node_with_network(
            1,
            config.clone(),
            SingleNodeRaftNetworkFactory,
            &path,
        )
        .await
        .unwrap();
        handle
            .initialize_membership(BTreeMap::from([(1, BasicNode::new("meta-local"))]))
            .await
            .unwrap();
        handle
            .wait_for_current_leader(1, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(
            handle
                .register_node(
                    crate::MetaNodeRegistration::new(7, "http://node7:4437", "http://node7:4438"),
                    1
                )
                .await
                .unwrap(),
            ControlResponse::Ok
        );
        let applied = handle
            .with_state_machine(|sm| Box::pin(async move { sm.applied_log_id().unwrap() }))
            .await
            .unwrap();
        let raft = handle.raft_handle();
        raft.trigger().snapshot().await.unwrap();
        raft.wait(Some(Duration::from_secs(5)))
            .snapshot(applied, "snapshot")
            .await
            .unwrap();
        raft.trigger().purge_log(applied.index).await.unwrap();
        raft.wait(Some(Duration::from_secs(5)))
            .purged(Some(applied), "purge")
            .await
            .unwrap();
        handle.shutdown().await.unwrap();
        drop(raft);
        drop(handle);

        let handle = MetaRaftHandle::new_durable_node_with_network(
            1,
            config,
            SingleNodeRaftNetworkFactory,
            &path,
        )
        .await
        .unwrap();
        assert!(handle.raft_handle().is_initialized().await.unwrap());
        handle
            .wait_for_current_leader(1, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(
            handle
                .read_state(|state| state.nodes.get(&7).unwrap().client_url.clone())
                .await
                .unwrap(),
            "http://node7:4437"
        );
        assert_eq!(
            handle
                .register_node(
                    crate::MetaNodeRegistration::new(8, "http://node8:4437", "http://node8:4438"),
                    2
                )
                .await
                .unwrap(),
            ControlResponse::Ok
        );
        handle.shutdown().await.unwrap();
    }
}

fn apply_record(log: &mut MetaLog, record: MetaLogRecord) -> io::Result<()> {
    match record {
        MetaLogRecord::SaveVote(vote) => log.vote = Some(vote),
        MetaLogRecord::SaveCommitted(committed) => log.committed = committed,
        MetaLogRecord::Append(entries) => {
            ensure_consecutive_entries::<MetaRaftTypeConfig>(&entries)?;
            ensure_log_append_boundary::<MetaRaftTypeConfig>(log, &entries)?;
            if let Some(first) = entries.first()
                && let Some(purged) = log.last_purged_log_id
            {
                if first.index() <= purged.index {
                    return Err(invalid("cannot append over purged meta entries"));
                }
                if log.entries.is_empty() && first.index() != purged.index.saturating_add(1) {
                    return Err(invalid("meta log has a hole after its purged prefix"));
                }
            }
            for entry in entries {
                log.entries.insert(entry.index(), entry);
            }
        }
        MetaLogRecord::TruncateAfter(last) => {
            if last < log.last_purged_log_id || last < log.committed {
                return Err(invalid(
                    "cannot truncate the durable committed/purged meta prefix",
                ));
            }
            log.entries
                .retain(|index, _| last.is_some_and(|last| *index <= last.index));
        }
        MetaLogRecord::Purge(purged) => {
            if Some(purged) < log.last_purged_log_id {
                return Err(invalid("cannot move the meta purged prefix backward"));
            }
            log.last_purged_log_id = Some(purged);
            log.entries.retain(|index, _| *index > purged.index);
        }
    }
    Ok(())
}

fn compact_records(log: &MetaLog) -> Vec<MetaLogRecord> {
    let mut records = Vec::new();
    if let Some(vote) = log.vote {
        records.push(MetaLogRecord::SaveVote(vote));
    }
    records.push(MetaLogRecord::SaveCommitted(log.committed));
    if let Some(purged) = log.last_purged_log_id {
        records.push(MetaLogRecord::Purge(purged));
    }
    let entries = log.entries.values().cloned().collect::<Vec<_>>();
    for chunk in entries.chunks(128) {
        records.push(MetaLogRecord::Append(chunk.to_vec()));
    }
    records
}

fn replace_journal<T: Serialize + serde::de::DeserializeOwned>(
    path: &Path,
    records: impl IntoIterator<Item = T>,
) -> io::Result<()> {
    let mut temporary_name = path.as_os_str().to_owned();
    temporary_name.push(".tmp");
    let temporary = PathBuf::from(temporary_name);
    match fs::remove_file(&temporary) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let mut writer = journal::JournalWriter::new(true);
    writer.ensure_created(&temporary)?;
    for record in records {
        writer.append::<WireCodec<T>>(&temporary, &record)?;
    }
    writer.sync(&temporary)?;
    drop(writer);
    fs::rename(&temporary, path)?;
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

impl RaftLogReader<MetaRaftTypeConfig> for Arc<MetaRaftFileLogStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> io::Result<Vec<EntryOf<MetaRaftTypeConfig>>> {
        Ok(self
            .lock()?
            .log
            .entries
            .range(range)
            .map(|(_, e)| e.clone())
            .collect())
    }

    async fn read_vote(&mut self) -> io::Result<Option<VoteOf<MetaRaftTypeConfig>>> {
        Ok(self.lock()?.log.vote)
    }
}

impl RaftLogStorage<MetaRaftTypeConfig> for Arc<MetaRaftFileLogStore> {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> io::Result<LogState<MetaRaftTypeConfig>> {
        let inner = self.lock()?;
        Ok(LogState {
            last_purged_log_id: inner.log.last_purged_log_id,
            last_log_id: inner
                .log
                .entries
                .last_key_value()
                .map(|(_, e)| e.log_id())
                .or(inner.log.last_purged_log_id),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VoteOf<MetaRaftTypeConfig>) -> io::Result<()> {
        self.write(MetaLogRecord::SaveVote(*vote)).await
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<MetaRaftTypeConfig>>,
    ) -> io::Result<()> {
        self.write(MetaLogRecord::SaveCommitted(committed)).await
    }

    async fn read_committed(&mut self) -> io::Result<Option<LogIdOf<MetaRaftTypeConfig>>> {
        Ok(self.lock()?.log.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<MetaRaftTypeConfig>,
    ) -> io::Result<()>
    where
        I: IntoIterator<Item = EntryOf<MetaRaftTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let result = self
            .write(MetaLogRecord::Append(entries.into_iter().collect()))
            .await;
        callback.io_completed(
            result
                .as_ref()
                .copied()
                .map_err(|error| io::Error::new(error.kind(), error.to_string())),
        );
        result
    }

    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<MetaRaftTypeConfig>>,
    ) -> io::Result<()> {
        self.write(MetaLogRecord::TruncateAfter(last_log_id)).await
    }

    async fn purge(&mut self, log_id: LogIdOf<MetaRaftTypeConfig>) -> io::Result<()> {
        self.write(MetaLogRecord::Purge(log_id)).await
    }
}
