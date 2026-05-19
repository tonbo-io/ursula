use openraft::rt::WatchReceiver;
use openraft::storage::RaftLogStorage;
use openraft::entry::RaftEntry;
use openraft::vote::RaftLeaderId;
use openraft::RaftNetworkV2;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs;
use std::future::Future;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use openraft::BasicNode;
use openraft::Config;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::OptionalSend;
use openraft::Raft;
use openraft::RaftNetworkFactory;
use openraft::alias::VoteOf;
use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::error::Unreachable;
use openraft::network::RPCOption;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::storage::IOFlushed;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use openraft::type_config::alias::SnapshotOf as TypeConfigSnapshotOf;
use prost::Message;
use ursula_proto as raft_app_proto;
use ursula_runtime::{
    AppendBatchRequest, AppendRequest, CloseStreamRequest, ColdWriteAdmission, CreateStreamRequest,
    GroupEngine, GroupWriteCommand, GroupWriteResponse, HeadStreamRequest, ReadStreamRequest,
};
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;

use super::*;
use crate::codec::*;
use crate::engine::*;
use crate::log_store::*;
use crate::registry::*;
use crate::types::*;

use futures_util::stream;
use openraft::{LogId, SnapshotPolicy};
use std::time::{SystemTime, UNIX_EPOCH};
use ursula_runtime::{ProducerRequest, RuntimeConfig, RuntimeThreading, ShardRuntime};

type CommittedLeaderId = <UrsulaRaftTypeConfig as openraft::RaftTypeConfig>::LeaderId;

fn placement() -> ShardPlacement {
    ShardPlacement {
        core_id: CoreId(0),
        shard_id: ShardId(0),
        raft_group_id: RaftGroupId(0),
    }
}

fn log_id(index: u64) -> LogId<CommittedLeaderId> {
    LogId {
        leader_id: CommittedLeaderId::new(1, 1),
        index,
    }
}

fn normal_entry(
    index: u64,
    command: GroupWriteCommand,
) -> <UrsulaRaftTypeConfig as openraft::RaftTypeConfig>::Entry {
    Entry::new(log_id(index), EntryPayload::Normal(command.into()))
}

fn create_stream_command(name: &str) -> GroupWriteCommand {
    GroupWriteCommand::from(CreateStreamRequest::new(
        ursula_shard::BucketStreamId::new("benchcmp", name),
        "application/octet-stream",
    ))
}

#[test]
fn raft_group_command_uses_shared_protobuf_log_schema() {
    let command = GroupWriteCommand::AppendBatch {
        stream_id: ursula_shard::BucketStreamId::new("benchcmp", "shared-proto-log"),
        content_type: "application/octet-stream".to_owned(),
        payloads: vec![b"ab".to_vec().into(), b"cd".to_vec().into()],
        producer: Some(ProducerRequest {
            producer_id: "writer-1".to_owned(),
            producer_epoch: 7,
            producer_seq: 42,
        }),
        now_ms: 123,
    };
    let raft_command = RaftGroupCommand::from(command.clone());

    let mut encoded = Vec::new();
    raft_command
        .0
        .encode(&mut encoded)
        .expect("encode shared proto command");
    let decoded = raft_app_proto::RaftGroupCommandV1::decode(encoded.as_slice())
        .expect("decode shared proto command");

    assert_eq!(decoded, raft_command.0);
    assert_eq!(
        group_write_command_from_proto(RaftGroupCommand(decoded)).expect("domain command"),
        command
    );
}

#[test]
fn raft_group_response_uses_shared_protobuf_log_schema() {
    let response = raft_write_applied_response(GroupWriteResponse::CreateStream(
        ursula_runtime::CreateStreamResponse {
            placement: placement(),
            next_offset: 5,
            closed: false,
            already_exists: false,
            group_commit_index: 11,
        },
    ));

    let mut encoded_proto = Vec::new();
    response
        .0
        .encode(&mut encoded_proto)
        .expect("encode shared proto response");
    let decoded_proto = raft_app_proto::RaftGroupResponseV1::decode(encoded_proto.as_slice())
        .expect("decode shared proto response");

    assert_eq!(decoded_proto, response.0);

    match group_write_result_from_raft_response(RaftGroupResponse(decoded_proto))
        .expect("domain response")
    {
        Ok(GroupWriteResponse::CreateStream(response)) => {
            assert_eq!(response.next_offset, 5);
            assert_eq!(response.group_commit_index, 11);
        }
        other => panic!("unexpected response: {other:?}"),
    }
}

fn temp_log_path(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time is after epoch")
        .as_nanos();
    std::env::temp_dir()
        .join("ursula-raft-tests")
        .join(format!("{name}-{}-{nonce}.bin", std::process::id()))
}

fn protobuf_frame_count<M: Message + Default>(path: &Path) -> usize {
    let bytes = fs::read(path).expect("read log file");
    read_protobuf_frames::<M>(&bytes)
        .expect("decode protobuf frames")
        .len()
}

#[derive(Debug, Clone, Default)]
struct InProcessRaftRegistry {
    nodes: Arc<Mutex<BTreeMap<u64, Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>>>>,
    full_snapshot_calls: Arc<Mutex<BTreeMap<u64, usize>>>,
}

impl InProcessRaftRegistry {
    fn register(&self, node_id: u64, raft: Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>) {
        self.nodes
            .lock()
            .expect("registry mutex")
            .insert(node_id, raft);
    }

    fn get(&self, node_id: u64) -> Option<Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>> {
        self.nodes
            .lock()
            .expect("registry mutex")
            .get(&node_id)
            .cloned()
    }

    fn record_full_snapshot(&self, node_id: u64) {
        *self
            .full_snapshot_calls
            .lock()
            .expect("full snapshot calls mutex")
            .entry(node_id)
            .or_insert(0) += 1;
    }

    fn full_snapshot_count(&self, node_id: u64) -> usize {
        self.full_snapshot_calls
            .lock()
            .expect("full snapshot calls mutex")
            .get(&node_id)
            .copied()
            .unwrap_or(0)
    }
}

#[derive(Debug, Clone)]
struct InProcessRaftNetworkFactory {
    registry: InProcessRaftRegistry,
}

impl RaftNetworkFactory<UrsulaRaftTypeConfig> for InProcessRaftNetworkFactory {
    type Network = InProcessRaftNetwork;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        InProcessRaftNetwork {
            target,
            registry: self.registry.clone(),
        }
    }
}

#[derive(Debug, Clone)]
struct InProcessRaftNetwork {
    target: u64,
    registry: InProcessRaftRegistry,
}

impl InProcessRaftNetwork {
    fn missing_target_error(&self) -> Unreachable<UrsulaRaftTypeConfig> {
        Unreachable::from_string(format!(
            "in-process raft node {} is not registered",
            self.target
        ))
    }
}

impl RaftNetworkV2<UrsulaRaftTypeConfig> for InProcessRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<UrsulaRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<UrsulaRaftTypeConfig>, RPCError<UrsulaRaftTypeConfig>> {
        let target = self
            .registry
            .get(self.target)
            .ok_or_else(|| RPCError::Unreachable(self.missing_target_error()))?;
        target.append_entries(rpc).await.map_err(|err| {
            RPCError::Network(NetworkError::from_string(format!(
                "remote AppendEntries on node {}: {err}",
                self.target
            )))
        })
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<UrsulaRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<UrsulaRaftTypeConfig>, RPCError<UrsulaRaftTypeConfig>> {
        let target = self
            .registry
            .get(self.target)
            .ok_or_else(|| RPCError::Unreachable(self.missing_target_error()))?;
        target.vote(rpc).await.map_err(|err| {
            RPCError::Network(NetworkError::from_string(format!(
                "remote Vote on node {}: {err}",
                self.target
            )))
        })
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<UrsulaRaftTypeConfig>,
        snapshot: TypeConfigSnapshotOf<UrsulaRaftTypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<UrsulaRaftTypeConfig>, StreamingError<UrsulaRaftTypeConfig>> {
        self.registry.record_full_snapshot(self.target);
        let target = self
            .registry
            .get(self.target)
            .ok_or_else(|| StreamingError::Unreachable(self.missing_target_error()))?;
        target
            .install_full_snapshot(vote, snapshot)
            .await
            .map_err(|err| {
                StreamingError::Network(NetworkError::from_string(format!(
                    "remote full snapshot on node {}: {err}",
                    self.target
                )))
            })
    }
}

#[tokio::test]
async fn raft_log_store_appends_reads_truncates_and_purges() {
    let mut store = RaftGroupLogStore::shared();
    store
        .append(
            vec![
                normal_entry(1, create_stream_command("log-1")),
                normal_entry(2, create_stream_command("log-2")),
                normal_entry(3, create_stream_command("log-3")),
            ],
            IOFlushed::noop(),
        )
        .await
        .expect("append raft log entries");

    let state = store.get_log_state().await.expect("log state");
    assert_eq!(state.last_purged_log_id, None);
    assert_eq!(state.last_log_id, Some(log_id(3)));

    let mut reader = store.get_log_reader().await;
    let entries = reader
        .try_get_log_entries(1..4)
        .await
        .expect("read entries");
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].log_id, log_id(1));
    assert_eq!(entries[2].log_id, log_id(3));

    store
        .truncate_after(Some(log_id(1)))
        .await
        .expect("truncate log");
    assert_eq!(
        store.get_log_state().await.expect("log state").last_log_id,
        Some(log_id(1))
    );

    store
        .append(
            vec![
                normal_entry(2, create_stream_command("log-2b")),
                normal_entry(3, create_stream_command("log-3b")),
            ],
            IOFlushed::noop(),
        )
        .await
        .expect("append after truncate");
    store.purge(log_id(2)).await.expect("purge log");

    let state = store.get_log_state().await.expect("log state after purge");
    assert_eq!(state.last_purged_log_id, Some(log_id(2)));
    assert_eq!(state.last_log_id, Some(log_id(3)));

    let entries = reader
        .try_get_log_entries(1..4)
        .await
        .expect("read after purge");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].log_id, log_id(3));
}

#[tokio::test]
async fn raft_log_store_persists_vote_and_committed_pointer() {
    let mut store = RaftGroupLogStore::shared();
    let vote: VoteOf<UrsulaRaftTypeConfig> = openraft::Vote::new_committed(7, 1);

    store.save_vote(&vote).await.expect("save vote");
    let mut reader = store.get_log_reader().await;
    assert_eq!(reader.read_vote().await.expect("read vote"), Some(vote));

    store
        .save_committed(Some(log_id(9)))
        .await
        .expect("save committed");
    assert_eq!(
        store.read_committed().await.expect("read committed"),
        Some(log_id(9))
    );
}

#[tokio::test]
async fn raft_log_store_rejects_holes() {
    let mut store = RaftGroupLogStore::shared();
    let err = store
        .append(
            vec![
                normal_entry(1, create_stream_command("hole-1")),
                normal_entry(3, create_stream_command("hole-3")),
            ],
            IOFlushed::noop(),
        )
        .await
        .expect_err("hole should be rejected");

    assert_eq!(err.kind(), io::ErrorKind::InvalidData);

    store
        .append(
            vec![normal_entry(1, create_stream_command("hole-boundary-1"))],
            IOFlushed::noop(),
        )
        .await
        .expect("append first entry");
    let err = store
        .append(
            vec![normal_entry(3, create_stream_command("hole-boundary-3"))],
            IOFlushed::noop(),
        )
        .await
        .expect_err("cross-append hole should be rejected");

    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn raft_file_log_store_recovers_vote_committed_and_entries() {
    let path = temp_log_path("recover");
    let vote: VoteOf<UrsulaRaftTypeConfig> = openraft::Vote::new_committed(7, 1);

    {
        let mut store = RaftGroupFileLogStore::shared(&path).expect("open file log store");
        store
            .append(
                vec![
                    normal_entry(1, create_stream_command("file-log-1")),
                    normal_entry(2, create_stream_command("file-log-2")),
                ],
                IOFlushed::noop(),
            )
            .await
            .expect("append file log entries");
        store.save_vote(&vote).await.expect("save vote");
        store
            .save_committed(Some(log_id(2)))
            .await
            .expect("save committed");
    }
    assert_eq!(protobuf_frame_count::<RaftGroupLogRecord>(&path), 3);

    let mut reopened = RaftGroupFileLogStore::shared(&path).expect("reopen file log store");
    let state = reopened.get_log_state().await.expect("log state");
    assert_eq!(state.last_log_id, Some(log_id(2)));
    assert_eq!(
        reopened.read_committed().await.expect("committed"),
        Some(log_id(2))
    );

    let mut reader = reopened.get_log_reader().await;
    assert_eq!(reader.read_vote().await.expect("vote"), Some(vote));
    let entries = reader
        .try_get_log_entries(1..3)
        .await
        .expect("read recovered entries");
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].log_id, log_id(1));
    assert_eq!(entries[1].log_id, log_id(2));

    let _ = fs::remove_file(&path);
}

#[tokio::test]
async fn raft_file_log_store_skips_duplicate_vote_and_committed_records() {
    let path = temp_log_path("duplicate-vote-committed");
    let vote: VoteOf<UrsulaRaftTypeConfig> = openraft::Vote::new_committed(7, 1);

    {
        let mut store = RaftGroupFileLogStore::shared(&path).expect("open file log store");
        store.save_vote(&vote).await.expect("save vote");
        store
            .save_committed(Some(log_id(2)))
            .await
            .expect("save committed");
        store.save_vote(&vote).await.expect("save duplicate vote");
        store
            .save_committed(Some(log_id(2)))
            .await
            .expect("save duplicate committed");
    }
    assert_eq!(protobuf_frame_count::<RaftGroupLogRecord>(&path), 2);

    let mut reopened = RaftGroupFileLogStore::shared(&path).expect("reopen file log store");
    assert_eq!(reopened.read_vote().await.expect("vote"), Some(vote));
    assert_eq!(
        reopened.read_committed().await.expect("committed"),
        Some(log_id(2))
    );

    let _ = fs::remove_file(&path);
}

#[tokio::test]
async fn raft_file_log_store_recovers_truncate_and_purge() {
    let path = temp_log_path("truncate-purge");

    {
        let mut store = RaftGroupFileLogStore::shared(&path).expect("open file log store");
        store
            .append(
                vec![
                    normal_entry(1, create_stream_command("file-log-1")),
                    normal_entry(2, create_stream_command("file-log-2")),
                    normal_entry(3, create_stream_command("file-log-3")),
                ],
                IOFlushed::noop(),
            )
            .await
            .expect("append initial entries");
        store
            .truncate_after(Some(log_id(1)))
            .await
            .expect("truncate file log");
        store
            .append(
                vec![
                    normal_entry(2, create_stream_command("file-log-2b")),
                    normal_entry(3, create_stream_command("file-log-3b")),
                ],
                IOFlushed::noop(),
            )
            .await
            .expect("append after truncate");
        store.purge(log_id(2)).await.expect("purge file log");
    }
    assert_eq!(protobuf_frame_count::<RaftGroupLogRecord>(&path), 4);

    let mut reopened = RaftGroupFileLogStore::shared(&path).expect("reopen file log store");
    let state = reopened.get_log_state().await.expect("log state");
    assert_eq!(state.last_purged_log_id, Some(log_id(2)));
    assert_eq!(state.last_log_id, Some(log_id(3)));

    let mut reader = reopened.get_log_reader().await;
    let entries = reader
        .try_get_log_entries(1..4)
        .await
        .expect("read recovered entries");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].log_id, log_id(3));

    let _ = fs::remove_file(&path);
}

#[tokio::test]
async fn single_node_openraft_group_applies_client_writes() {
    let config = Arc::new(
        Config {
            cluster_name: "ursula-single-node-test".to_owned(),
            heartbeat_interval: 10,
            election_timeout_min: 30,
            election_timeout_max: 60,
            ..Default::default()
        }
        .validate()
        .expect("valid raft config"),
    );
    let mut log_store = RaftGroupLogStore::shared();
    let state_machine = RaftGroupStateMachine::new(placement());
    let raft = Raft::<UrsulaRaftTypeConfig, RaftGroupStateMachine>::new(
        1,
        config,
        SingleNodeRaftNetworkFactory,
        log_store.clone(),
        state_machine,
    )
    .await
    .expect("create single-node raft group");

    let mut nodes = BTreeMap::new();
    nodes.insert(1, BasicNode::new("local"));
    raft.initialize(nodes)
        .await
        .expect("initialize single-node raft group");
    raft.wait(Some(Duration::from_secs(2)))
        .current_leader(1, "single-node raft group should elect itself")
        .await
        .expect("wait for leadership");

    let stream_id = ursula_shard::BucketStreamId::new("benchcmp", "raft-client-write");
    let created = raft
        .client_write(
            GroupWriteCommand::from(CreateStreamRequest::new(
                stream_id.clone(),
                "application/octet-stream",
            ))
            .into(),
        )
        .await
        .expect("create stream through openraft");
    assert!(matches!(
        group_write_result_from_raft_response(created.data).expect("decode create response"),
        Ok(GroupWriteResponse::CreateStream(_))
    ));

    let appended = raft
        .client_write(
            GroupWriteCommand::from(AppendRequest::from_bytes(stream_id, b"payload".to_vec()))
                .into(),
        )
        .await
        .expect("append through openraft");
    match group_write_result_from_raft_response(appended.data).expect("decode append response") {
        Ok(GroupWriteResponse::Append(response)) => {
            assert_eq!(response.start_offset, 0);
            assert_eq!(response.stream_append_count, 1);
        }
        other => panic!("unexpected append response: {other:?}"),
    }

    let state = log_store.get_log_state().await.expect("raft log state");
    assert!(state.last_log_id.is_some());
    raft.shutdown().await.expect("shutdown raft group");
}

#[tokio::test]
async fn three_node_openraft_group_replicates_group_writes() {
    let registry = InProcessRaftRegistry::default();
    let config = Arc::new(
        Config {
            cluster_name: "ursula-three-node-test".to_owned(),
            heartbeat_interval: 10,
            election_timeout_min: 50,
            election_timeout_max: 100,
            ..Default::default()
        }
        .validate()
        .expect("valid raft config"),
    );
    let mut nodes = BTreeMap::new();
    for node_id in 1..=3 {
        nodes.insert(node_id, BasicNode::new(format!("node-{node_id}")));
    }

    let mut engines = Vec::new();
    for node_id in 1..=3 {
        let engine = RaftGroupEngine::new_node_with_log_store_and_network(
            placement(),
            node_id,
            config.clone(),
            InProcessRaftNetworkFactory {
                registry: registry.clone(),
            },
            RaftGroupLogStore::shared(),
            None,
            None,
        )
        .await
        .expect("create cluster raft group node");
        registry.register(node_id, engine.raft.clone());
        engines.push(engine);
    }

    engines[0]
        .raft
        .initialize(nodes)
        .await
        .expect("initialize three-node raft group");
    let leader_metrics = engines[0]
        .raft
        .wait(Some(Duration::from_secs(5)))
        .metrics(|metrics| metrics.current_leader.is_some(), "leader elected")
        .await
        .expect("wait for leader");
    let leader_id = leader_metrics.current_leader.expect("leader id");
    for engine in &engines {
        engine
            .raft
            .wait(Some(Duration::from_secs(5)))
            .current_leader(leader_id, "all nodes observe the same leader")
            .await
            .expect("wait for shared leader");
    }
    for (index, engine) in engines.iter().enumerate() {
        let node_id = u64::try_from(index + 1).expect("node id fits u64");
        assert_eq!(engine.accepts_local_writes(), node_id == leader_id);
    }

    let leader_index = usize::try_from(leader_id - 1).expect("leader id fits usize");
    let stream_id = ursula_shard::BucketStreamId::new("benchcmp", "three-node-raft-group-engine");
    let created = engines[leader_index]
        .raft
        .client_write(
            GroupWriteCommand::from(CreateStreamRequest::new(
                stream_id.clone(),
                "application/octet-stream",
            ))
            .into(),
        )
        .await
        .expect("create stream through elected leader");
    assert!(matches!(
        group_write_result_from_raft_response(created.data).expect("decode create response"),
        Ok(GroupWriteResponse::CreateStream(_))
    ));

    let appended = engines[leader_index]
        .raft
        .client_write(
            GroupWriteCommand::from(AppendRequest::from_bytes(
                stream_id.clone(),
                b"replicated".to_vec(),
            ))
            .into(),
        )
        .await
        .expect("append through elected leader");
    let appended_log_index = appended.log_id.index();
    match group_write_result_from_raft_response(appended.data).expect("decode append response") {
        Ok(GroupWriteResponse::Append(response)) => {
            assert_eq!(response.start_offset, 0);
            assert_eq!(response.next_offset, 10);
        }
        other => panic!("unexpected append response: {other:?}"),
    }

    for engine in &engines {
        engine
            .raft
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(Some(appended_log_index), "append replicated")
            .await
            .expect("wait for append replication");

        let stream_id = stream_id.clone();
        let read = engine
            .with_state_machine(move |state_machine| {
                Box::pin(async move {
                    state_machine
                        .read_stream(
                            ReadStreamRequest {
                                stream_id,
                                offset: 0,
                                max_len: 16,
                                now_ms: 0,
                            },
                            placement(),
                        )
                        .await
                })
            })
            .await
            .expect("read follower state machine")
            .expect("replicated stream is readable");
        assert_eq!(read.payload, b"replicated");
    }

    for engine in &engines {
        engine
            .shutdown()
            .await
            .expect("shutdown cluster raft group node");
    }
}

#[tokio::test]
async fn openraft_installs_snapshot_for_lagging_learner() {
    let registry = InProcessRaftRegistry::default();
    let config = Arc::new(
        Config {
            cluster_name: "ursula-lagging-learner-snapshot-test".to_owned(),
            heartbeat_interval: 10,
            election_timeout_min: 50,
            election_timeout_max: 100,
            max_in_snapshot_log_to_keep: 0,
            purge_batch_size: 1,
            replication_lag_threshold: 0,
            snapshot_policy: SnapshotPolicy::Never,
            ..Default::default()
        }
        .validate()
        .expect("valid raft config"),
    );

    let mut engines = Vec::new();
    for node_id in 1..=3 {
        let engine = RaftGroupEngine::new_node_with_log_store_and_network(
            placement(),
            node_id,
            config.clone(),
            InProcessRaftNetworkFactory {
                registry: registry.clone(),
            },
            RaftGroupLogStore::shared(),
            None,
            None,
        )
        .await
        .expect("create cluster raft group node");
        if node_id != 3 {
            registry.register(node_id, engine.raft.clone());
        }
        engines.push(engine);
    }

    let mut initial_nodes = BTreeMap::new();
    for node_id in 1..=2 {
        initial_nodes.insert(node_id, BasicNode::new(format!("node-{node_id}")));
    }
    engines[0]
        .raft
        .initialize(initial_nodes)
        .await
        .expect("initialize two-node raft group");
    let leader_metrics = engines[0]
        .raft
        .wait(Some(Duration::from_secs(5)))
        .metrics(|metrics| metrics.current_leader.is_some(), "leader elected")
        .await
        .expect("wait for leader");
    let leader_id = leader_metrics.current_leader.expect("leader id");
    for engine in &engines[..2] {
        engine
            .raft
            .wait(Some(Duration::from_secs(5)))
            .current_leader(leader_id, "initial voters observe the same leader")
            .await
            .expect("wait for shared leader");
    }

    let leader_index = usize::try_from(leader_id - 1).expect("leader id fits usize");
    let stream_id = ursula_shard::BucketStreamId::new("benchcmp", "lagging-learner-snapshot");
    let _created = engines[leader_index]
        .raft
        .client_write(
            GroupWriteCommand::from(CreateStreamRequest::new(
                stream_id.clone(),
                "application/octet-stream",
            ))
            .into(),
        )
        .await
        .expect("create stream through elected leader");
    let appended = engines[leader_index]
        .raft
        .client_write(
            GroupWriteCommand::from(AppendRequest::from_bytes(
                stream_id.clone(),
                b"snapshot-transfer".to_vec(),
            ))
            .into(),
        )
        .await
        .expect("append through elected leader");
    let appended_log_id = appended.log_id;
    let appended_log_index = appended_log_id.index();
    assert!(matches!(
        group_write_result_from_raft_response(appended.data).expect("decode append response"),
        Ok(GroupWriteResponse::Append(_))
    ));

    for engine in &engines[..2] {
        engine
            .raft
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(Some(appended_log_index), "initial voters applied append")
            .await
            .expect("wait for initial voter apply");
    }

    engines[leader_index]
        .raft
        .trigger()
        .snapshot()
        .await
        .expect("trigger leader snapshot");
    engines[leader_index]
        .raft
        .wait(Some(Duration::from_secs(5)))
        .snapshot(appended_log_id, "leader snapshot includes append")
        .await
        .expect("wait for leader snapshot");
    engines[leader_index]
        .raft
        .trigger()
        .purge_log(appended_log_index)
        .await
        .expect("trigger leader log purge");
    engines[leader_index]
        .raft
        .wait(Some(Duration::from_secs(5)))
        .purged(Some(appended_log_id), "leader purged snapshotted logs")
        .await
        .expect("wait for leader purge");

    registry.register(3, engines[2].raft.clone());
    let learner_added = engines[leader_index]
        .raft
        .add_learner(3, BasicNode::new("node-3"), true)
        .await
        .expect("add lagging learner");
    for _ in 0..50 {
        if registry.full_snapshot_count(3) > 0 {
            break;
        }
        engines[leader_index]
            .raft
            .trigger()
            .heartbeat()
            .await
            .expect("trigger heartbeat while waiting for snapshot replication");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        registry.full_snapshot_count(3) >= 1,
        "lagging learner should catch up through full_snapshot"
    );

    engines[2]
        .raft
        .wait(Some(Duration::from_secs(5)))
        .applied_index_at_least(
            Some(learner_added.log_id.index()),
            "lagging learner applied learner membership",
        )
        .await
        .expect("wait for lagging learner catch-up");
    let installed_snapshot_log_id = engines[2]
        .with_state_machine(|state_machine| {
            Box::pin(async move {
                state_machine
                    .current_snapshot
                    .lock()
                    .expect("snapshot mutex")
                    .as_ref()
                    .and_then(|snapshot| snapshot.meta.last_log_id)
            })
        })
        .await
        .expect("inspect lagging learner state machine");
    assert_eq!(installed_snapshot_log_id, Some(appended_log_id));

    let read = engines[2]
        .with_state_machine({
            let stream_id = stream_id.clone();
            move |state_machine| {
                Box::pin(async move {
                    state_machine
                        .read_stream(
                            ReadStreamRequest {
                                stream_id,
                                offset: 0,
                                max_len: 64,
                                now_ms: 0,
                            },
                            placement(),
                        )
                        .await
                })
            }
        })
        .await
        .expect("read lagging learner state machine")
        .expect("stream restored from snapshot is readable");
    assert_eq!(read.payload, b"snapshot-transfer");

    for engine in &engines {
        engine
            .shutdown()
            .await
            .expect("shutdown cluster raft group node");
    }
}

#[tokio::test]
async fn raft_group_engine_implements_runtime_group_engine_over_openraft() {
    let mut engine = RaftGroupEngine::new_single_node(placement())
        .await
        .expect("create raft group engine");
    let stream_id = ursula_shard::BucketStreamId::new("benchcmp", "raft-group-engine");

    let created = engine
        .create_stream(
            CreateStreamRequest::new(stream_id.clone(), "application/octet-stream"),
            placement(),
        )
        .await
        .expect("create through group engine");
    assert_eq!(created.next_offset, 0);
    assert!(!created.already_exists);

    let appended = engine
        .append(
            AppendRequest::from_bytes(stream_id.clone(), b"payload".to_vec()),
            placement(),
        )
        .await
        .expect("append through group engine");
    assert_eq!(appended.start_offset, 0);
    assert_eq!(appended.next_offset, 7);

    let head = engine
        .head_stream(
            HeadStreamRequest {
                stream_id: stream_id.clone(),
                now_ms: 0,
            },
            placement(),
        )
        .await
        .expect("head through group engine");
    assert_eq!(head.tail_offset, 7);

    let read = engine
        .read_stream(
            ReadStreamRequest {
                stream_id,
                offset: 0,
                max_len: 16,
                now_ms: 0,
            },
            placement(),
        )
        .await
        .expect("read through group engine");
    assert_eq!(read.payload, b"payload");

    let snapshot = engine.snapshot(placement()).await.expect("snapshot");
    assert_eq!(snapshot.group_commit_index, 2);
    engine.shutdown().await.expect("shutdown raft group engine");
}

#[tokio::test]
async fn raft_group_engine_applies_batched_runtime_writes() {
    let mut engine = RaftGroupEngine::new_single_node(placement())
        .await
        .expect("create raft group engine");
    let stream_id = ursula_shard::BucketStreamId::new("benchcmp", "raft-group-engine-batch");

    let responses = engine
        .write_batch(
            vec![
                GroupWriteCommand::from(CreateStreamRequest::new(
                    stream_id.clone(),
                    "application/octet-stream",
                )),
                GroupWriteCommand::from(AppendBatchRequest::new(
                    stream_id.clone(),
                    vec![b"ab".to_vec(), b"cd".to_vec()],
                )),
            ],
            placement(),
        )
        .await
        .expect("write batch through group engine");

    assert_eq!(responses.len(), 2);
    assert!(matches!(
        &responses[0],
        Ok(GroupWriteResponse::CreateStream(response)) if response.group_commit_index == 1
    ));
    match &responses[1] {
        Ok(GroupWriteResponse::AppendBatch(response)) => {
            assert_eq!(response.items.len(), 2);
            assert_eq!(
                response.items[0].as_ref().expect("first item").start_offset,
                0
            );
            assert_eq!(
                response.items[1]
                    .as_ref()
                    .expect("second item")
                    .start_offset,
                2
            );
        }
        other => panic!("unexpected append batch response: {other:?}"),
    }

    let read = engine
        .read_stream(
            ReadStreamRequest {
                stream_id,
                offset: 0,
                max_len: 16,
                now_ms: 0,
            },
            placement(),
        )
        .await
        .expect("read batched write");
    assert_eq!(read.payload, b"abcd");
    engine.shutdown().await.expect("shutdown raft group engine");
}

#[tokio::test]
async fn raft_group_engine_cold_admission_coalesces_append_batch_many_into_one_raft_entry() {
    let mut engine = RaftGroupEngine::new_single_node(placement())
        .await
        .expect("create raft group engine");
    let stream_id =
        ursula_shard::BucketStreamId::new("benchcmp", "raft-group-engine-cold-batch-many");

    engine
        .create_stream(
            CreateStreamRequest::new(stream_id.clone(), "application/octet-stream"),
            placement(),
        )
        .await
        .expect("create stream");
    let before_batch_log_index = engine
        .raft_handle()
        .metrics()
        .borrow_watched()
        .last_log_index
        .expect("create stream should append a raft log entry");

    let responses = engine
        .append_batch_many_with_cold_admission(
            vec![
                AppendBatchRequest::new(stream_id.clone(), vec![b"ab".to_vec()]),
                AppendBatchRequest::new(stream_id.clone(), vec![b"cd".to_vec()]),
                AppendBatchRequest::new(stream_id.clone(), vec![b"ef".to_vec()]),
            ],
            placement(),
            ColdWriteAdmission {
                max_hot_bytes_per_group: Some(1024 * 1024),
            },
        )
        .await
        .expect("append batch many with cold admission");

    assert_eq!(responses.len(), 3);
    for (index, response) in responses.into_iter().enumerate() {
        match response.expect("append batch response") {
            GroupWriteResponse::AppendBatch(response) => {
                assert_eq!(response.items.len(), 1);
                let item = response.items[0].as_ref().expect("append batch item");
                assert_eq!(item.start_offset, u64::try_from(index * 2).unwrap());
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    let after_batch_log_index = engine
        .raft_handle()
        .metrics()
        .borrow_watched()
        .last_log_index
        .expect("append batch should append a raft log entry");
    assert_eq!(after_batch_log_index, before_batch_log_index + 1);

    let read = engine
        .read_stream(
            ReadStreamRequest {
                stream_id,
                offset: 0,
                max_len: 16,
                now_ms: 0,
            },
            placement(),
        )
        .await
        .expect("read coalesced append batches");
    assert_eq!(read.payload, b"abcdef");
    engine.shutdown().await.expect("shutdown raft group engine");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_metrics_count_logical_commands_inside_coalesced_batches() {
    let runtime = ShardRuntime::spawn_with_engine_factory(
        RuntimeConfig::new(1, 1).with_cold_max_hot_bytes_per_group(Some(1024 * 1024)),
        RaftGroupEngineFactory,
    )
    .expect("spawn raft runtime");
    let stream_id = ursula_shard::BucketStreamId::new("benchcmp", "raft-logical-command-metrics");

    runtime
        .create_stream(CreateStreamRequest::new(
            stream_id.clone(),
            "application/octet-stream",
        ))
        .await
        .expect("create stream");
    let before = runtime.metrics().snapshot();

    let first = {
        let runtime = runtime.clone();
        let stream_id = stream_id.clone();
        tokio::spawn(async move {
            runtime
                .append_batch(AppendBatchRequest::new(stream_id, vec![b"ab".to_vec()]))
                .await
                .expect("first append batch")
        })
    };
    let second = {
        let runtime = runtime.clone();
        let stream_id = stream_id.clone();
        tokio::spawn(async move {
            runtime
                .append_batch(AppendBatchRequest::new(stream_id, vec![b"cd".to_vec()]))
                .await
                .expect("second append batch")
        })
    };
    let third = {
        let runtime = runtime.clone();
        let stream_id = stream_id.clone();
        tokio::spawn(async move {
            runtime
                .append_batch(AppendBatchRequest::new(stream_id, vec![b"ef".to_vec()]))
                .await
                .expect("third append batch")
        })
    };

    first.await.expect("first task");
    second.await.expect("second task");
    third.await.expect("third task");

    let after = runtime.metrics().snapshot();
    assert_eq!(
        after.raft_write_many_commands - before.raft_write_many_commands,
        after.raft_write_many_batches - before.raft_write_many_batches
    );
    assert_eq!(
        after.raft_write_many_logical_commands - before.raft_write_many_logical_commands,
        3
    );
    assert!(
        after.raft_write_many_logical_commands >= after.raft_write_many_commands,
        "logical command count should include commands nested in Batch"
    );

    let read = runtime
        .read_stream(ReadStreamRequest {
            stream_id,
            offset: 0,
            max_len: 16,
            now_ms: 0,
        })
        .await
        .expect("read appended batches");
    let mut chunks = read
        .payload
        .chunks_exact(2)
        .map(Vec::from)
        .collect::<Vec<_>>();
    chunks.sort();
    assert_eq!(chunks, vec![b"ab".to_vec(), b"cd".to_vec(), b"ef".to_vec()]);
}

#[tokio::test]
async fn raft_group_engine_preserves_stream_error_next_offset() {
    let mut engine = RaftGroupEngine::new_single_node(placement())
        .await
        .expect("create raft group engine");
    let stream_id = ursula_shard::BucketStreamId::new("benchcmp", "raft-stream-error-offset");

    engine
        .create_stream(
            CreateStreamRequest::new(stream_id.clone(), "application/octet-stream"),
            placement(),
        )
        .await
        .expect("create through group engine");
    engine
        .append(
            AppendRequest::from_bytes(stream_id.clone(), b"payload".to_vec()),
            placement(),
        )
        .await
        .expect("append through group engine");
    engine
        .close_stream(
            CloseStreamRequest {
                stream_id: stream_id.clone(),
                stream_seq: None,
                producer: None,
                now_ms: 0,
            },
            placement(),
        )
        .await
        .expect("close through group engine");

    let err = engine
        .append(
            AppendRequest::from_bytes(stream_id, b"after-close".to_vec()),
            placement(),
        )
        .await
        .expect_err("append to closed stream should fail");
    assert_eq!(err.next_offset(), Some(7));

    engine.shutdown().await.expect("shutdown raft group engine");
}

#[tokio::test]
async fn raft_group_engine_recovers_client_writes_from_file_log() {
    let path = temp_log_path("raft-group-engine-recover");
    let stream_id = ursula_shard::BucketStreamId::new("benchcmp", "raft-engine-recover");

    {
        let mut engine = RaftGroupEngine::new_single_node_with_file_log(placement(), &path)
            .await
            .expect("create durable raft group engine");
        engine
            .create_stream(
                CreateStreamRequest::new(stream_id.clone(), "application/octet-stream"),
                placement(),
            )
            .await
            .expect("create through durable raft group engine");
        engine
            .append(
                AppendRequest::from_bytes(stream_id.clone(), b"payload".to_vec()),
                placement(),
            )
            .await
            .expect("append through durable raft group engine");
        engine.shutdown().await.expect("shutdown first engine");
    }

    let mut recovered = RaftGroupEngine::new_single_node_with_file_log(placement(), &path)
        .await
        .expect("reopen durable raft group engine");
    let read = recovered
        .read_stream(
            ReadStreamRequest {
                stream_id,
                offset: 0,
                max_len: 16,
                now_ms: 0,
            },
            placement(),
        )
        .await
        .expect("read recovered payload");
    assert_eq!(read.payload, b"payload");
    recovered
        .shutdown()
        .await
        .expect("shutdown recovered engine");

    let _ = fs::remove_file(&path);
}

#[tokio::test]
async fn shard_runtime_uses_raft_group_engine_factory_for_owned_group() {
    let mut config = RuntimeConfig::new(1, 1);
    config.threading = RuntimeThreading::HostedTokio;
    let runtime = ShardRuntime::spawn_with_engine_factory(config, RaftGroupEngineFactory)
        .expect("spawn runtime with raft group engine factory");
    let stream_id = ursula_shard::BucketStreamId::new("benchcmp", "runtime-raft-engine");

    runtime
        .create_stream(CreateStreamRequest::new(
            stream_id.clone(),
            "application/octet-stream",
        ))
        .await
        .expect("create through runtime-owned raft group");
    runtime
        .append(AppendRequest::from_bytes(
            stream_id.clone(),
            b"payload".to_vec(),
        ))
        .await
        .expect("append through runtime-owned raft group");

    let read = runtime
        .read_stream(ReadStreamRequest {
            stream_id,
            offset: 0,
            max_len: 16,
            now_ms: 0,
        })
        .await
        .expect("read through runtime-owned raft group");
    assert_eq!(read.payload, b"payload");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn warm_group_registers_runtime_owned_raft_handle() {
    let registry = RaftGroupHandleRegistry::default();
    let mut config = RuntimeConfig::new(2, 4);
    config.threading = RuntimeThreading::HostedTokio;
    let runtime = ShardRuntime::spawn_with_engine_factory(
        config,
        RegisteredRaftGroupEngineFactory::new(registry.clone()),
    )
    .expect("spawn runtime with registered raft group engine factory");

    assert!(registry.is_empty());
    let placement = runtime
        .warm_group(RaftGroupId(3))
        .await
        .expect("warm raft group");
    assert_eq!(placement.core_id, CoreId(1));
    assert_eq!(placement.raft_group_id, RaftGroupId(3));
    assert!(registry.contains_group(RaftGroupId(3)));
    assert_eq!(registry.len(), 1);

    let raft = registry
        .get(RaftGroupId(3))
        .expect("registered raft handle");
    raft.wait(Some(Duration::from_secs(2)))
        .current_leader(1, "registered single-node group should elect itself")
        .await
        .expect("wait for registered leader");
}

#[tokio::test]
async fn durable_raft_group_engine_records_file_log_metrics() {
    let root = temp_log_path("raft-file-log-metrics-root").with_extension("");
    let _ = fs::remove_dir_all(&root);

    let mut config = RuntimeConfig::new(1, 1);
    config.threading = RuntimeThreading::HostedTokio;
    let runtime =
        ShardRuntime::spawn_with_engine_factory(config, DurableRaftGroupEngineFactory::new(&root))
            .expect("spawn runtime with durable raft group engine factory");
    let placement = placement();
    let stream_id = ursula_shard::BucketStreamId::new("benchcmp", "runtime-raft-file-metrics");

    runtime
        .create_stream(CreateStreamRequest::new(
            stream_id.clone(),
            "application/octet-stream",
        ))
        .await
        .expect("create through durable runtime-owned raft group");
    runtime
        .append(AppendRequest::from_bytes(
            stream_id.clone(),
            b"payload".to_vec(),
        ))
        .await
        .expect("append through durable runtime-owned raft group");

    let metrics = runtime.metrics().snapshot();
    let core_index = usize::from(placement.core_id.0);
    let group_index = usize::try_from(placement.raft_group_id.0).expect("u32 fits usize");
    assert!(metrics.wal_batches >= 2);
    assert!(metrics.wal_records >= 2);
    assert_eq!(
        metrics.wal_batches,
        metrics.per_core_wal_batches[core_index]
    );
    assert_eq!(
        metrics.wal_records,
        metrics.per_group_wal_records[group_index]
    );
    assert!(metrics.wal_write_ns > 0);
    assert!(metrics.wal_sync_ns > 0);

    drop(runtime);
    let _ = fs::remove_dir_all(&root);
}

#[tokio::test]
async fn durable_raft_group_engine_recovers_from_core_journal() {
    let root = temp_log_path("raft-core-journal-recover-root").with_extension("");
    let _ = fs::remove_dir_all(&root);
    let stream_id = ursula_shard::BucketStreamId::new("benchcmp", "raft-core-journal-recover");

    {
        let mut config = RuntimeConfig::new(1, 1);
        config.threading = RuntimeThreading::HostedTokio;
        let runtime = ShardRuntime::spawn_with_engine_factory(
            config,
            DurableRaftGroupEngineFactory::new(&root),
        )
        .expect("spawn durable runtime");
        runtime
            .create_stream(CreateStreamRequest::new(
                stream_id.clone(),
                "application/octet-stream",
            ))
            .await
            .expect("create stream");
        runtime
            .append(AppendRequest::from_bytes(
                stream_id.clone(),
                b"journal-payload".to_vec(),
            ))
            .await
            .expect("append stream");
    }

    let journal_path = root.join("core-0").join("journal.bin");
    assert!(journal_path.exists(), "core journal should exist");
    assert!(
        fs::metadata(&journal_path)
            .expect("core journal metadata")
            .len()
            > 0,
        "core journal should contain records"
    );

    {
        let mut config = RuntimeConfig::new(1, 1);
        config.threading = RuntimeThreading::HostedTokio;
        let recovered = ShardRuntime::spawn_with_engine_factory(
            config,
            DurableRaftGroupEngineFactory::new(&root),
        )
        .expect("spawn recovered durable runtime");
        let read = recovered
            .read_stream(ReadStreamRequest {
                stream_id,
                offset: 0,
                max_len: 32,
                now_ms: 0,
            })
            .await
            .expect("read recovered stream");
        assert_eq!(read.payload, b"journal-payload");
    }

    let _ = fs::remove_dir_all(&root);
}

#[tokio::test]
async fn openraft_state_machine_applies_group_write_commands() {
    let stream_id = ursula_shard::BucketStreamId::new("benchcmp", "raft-apply");
    let mut sm = RaftGroupStateMachine::new(placement());
    let entries = vec![
        normal_entry(
            1,
            GroupWriteCommand::from(CreateStreamRequest::new(
                stream_id.clone(),
                "application/octet-stream",
            )),
        ),
        normal_entry(
            2,
            GroupWriteCommand::from(AppendRequest::from_bytes(
                stream_id.clone(),
                b"abc".to_vec(),
            )),
        ),
    ];

    sm.apply(stream::iter(
        entries.into_iter().map(|entry| Ok((entry, None))),
    ))
    .await
    .expect("apply raft entries");

    let snapshot = sm.group_snapshot().await.expect("snapshot");
    assert_eq!(snapshot.group_commit_index, 2);
    assert_eq!(snapshot.stream_append_counts.len(), 1);
    assert_eq!(snapshot.stream_append_counts[0].append_count, 1);
}

#[tokio::test]
async fn openraft_snapshot_round_trips_group_state() {
    let stream_id = ursula_shard::BucketStreamId::new("benchcmp", "raft-snapshot");
    let mut source = RaftGroupStateMachine::new(placement());
    let entries = vec![
        normal_entry(
            1,
            GroupWriteCommand::from(CreateStreamRequest::new(
                stream_id.clone(),
                "application/octet-stream",
            )),
        ),
        normal_entry(
            2,
            GroupWriteCommand::from(AppendRequest::from_bytes(stream_id, b"payload".to_vec())),
        ),
    ];
    source
        .apply(stream::iter(
            entries.into_iter().map(|entry| Ok((entry, None))),
        ))
        .await
        .expect("apply source");

    let mut builder = source.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await.expect("build snapshot");

    let mut target = RaftGroupStateMachine::new(placement());
    target
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .expect("install snapshot");

    let appended = target
        .engine
        .apply_committed_write(
            GroupWriteCommand::from(AppendRequest::from_bytes(
                ursula_shard::BucketStreamId::new("benchcmp", "raft-snapshot"),
                b"-next".to_vec(),
            )),
            placement(),
        )
        .expect("append after install");
    match appended {
        GroupWriteResponse::Append(response) => {
            assert_eq!(response.start_offset, 7);
            assert_eq!(response.stream_append_count, 2);
        }
        other => panic!("unexpected append response: {other:?}"),
    }
}
