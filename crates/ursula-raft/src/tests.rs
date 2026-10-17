use std::collections::BTreeMap;
use std::collections::BTreeSet;
#[cfg(not(madsim))]
use std::fs;
use std::io;
#[cfg(not(madsim))]
use std::path::Path;
#[cfg(not(madsim))]
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(madsim)]
use std::sync::Mutex;
use std::time::Duration;
#[cfg(not(madsim))]
use std::time::SystemTime;
#[cfg(not(madsim))]
use std::time::UNIX_EPOCH;

use futures_util::stream;
use openraft::BasicNode;
use openraft::Config;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::LogId;
#[cfg(not(madsim))]
use openraft::Raft;
#[cfg(not(madsim))]
use openraft::SnapshotPolicy;
#[cfg(not(madsim))]
use openraft::StorageError;
use openraft::alias::VoteOf;
use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
#[cfg(not(madsim))]
use openraft::testing::log::StoreBuilder;
#[cfg(not(madsim))]
use openraft::testing::log::Suite;
#[cfg(not(madsim))]
use openraft::type_config::TypeConfigExt;
use openraft::vote::RaftLeaderId;
use ursula_config::WalFsync;
use ursula_control::ControlCommand;
#[cfg(not(madsim))]
use ursula_runtime::AppendExternalRequest;
use ursula_runtime::AppendRequest;
#[cfg(not(madsim))]
use ursula_runtime::BootstrapStreamRequest;
#[cfg(not(madsim))]
use ursula_runtime::CloseStreamRequest;
use ursula_runtime::ColdWriteAdmission;
#[cfg(not(madsim))]
use ursula_runtime::CreateStreamExternalRequest;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::EarlyAnswerQuery;
#[cfg(not(madsim))]
use ursula_runtime::ExternalPayloadRef;
use ursula_runtime::FollowerRead;
use ursula_runtime::GroupEngine;
use ursula_runtime::GroupEngineError;
#[cfg(not(madsim))]
use ursula_runtime::GroupEngineFactory;
use ursula_runtime::GroupInfraError;
use ursula_runtime::GroupWriteCommand;
use ursula_runtime::GroupWriteResponse;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::ProducerEarlyAnswer;
use ursula_runtime::ProducerRequest;
#[cfg(not(madsim))]
use ursula_runtime::ReadSnapshotRequest;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::ReadStreamResponse;
use ursula_runtime::RouteWriteRequest;
#[cfg(not(madsim))]
use ursula_runtime::RuntimeConfig;
#[cfg(not(madsim))]
use ursula_runtime::RuntimeThreading;
#[cfg(not(madsim))]
use ursula_runtime::ShardRuntime;
use ursula_runtime::StreamErrorCode;
use ursula_runtime::StreamErrorContext;
use ursula_runtime::WriteRoute;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;

use super::*;
use crate::codec::*;
use crate::engine::*;
use crate::forward::write_result_from_raft_response;
use crate::log_store::*;
#[cfg(not(madsim))]
use crate::registry::*;
use crate::types::*;

type CommittedLeaderId = <UrsulaRaftTypeConfig as openraft::RaftTypeConfig>::LeaderId;
type MetaLeaderId = <MetaRaftTypeConfig as openraft::RaftTypeConfig>::LeaderId;

#[cfg(not(madsim))]
#[path = "rejoin_grpc_tests.rs"]
mod rejoin_grpc_tests;

#[cfg(not(madsim))]
#[path = "forward_tests.rs"]
mod forward_tests;

#[cfg(madsim)]
#[path = "sim_storage_tests.rs"]
mod sim_storage_tests;

#[cfg(not(madsim))]
struct FileLogStoreBuilder;

#[cfg(not(madsim))]
impl
    StoreBuilder<
        UrsulaRaftTypeConfig,
        Arc<RaftGroupFileLogStore>,
        RaftGroupStateMachine,
        tempfile::TempDir,
    > for FileLogStoreBuilder
{
    async fn build(
        &self,
    ) -> Result<
        (
            tempfile::TempDir,
            Arc<RaftGroupFileLogStore>,
            RaftGroupStateMachine,
        ),
        StorageError<UrsulaRaftTypeConfig>,
    > {
        let dir = tempfile::tempdir()
            .map_err(|err| StorageError::write(UrsulaRaftTypeConfig::err_from_error(&err)))?;
        let store = open_core_journal_store(dir.path())
            .map_err(|err| StorageError::write(UrsulaRaftTypeConfig::err_from_error(&err)))?;
        Ok((dir, store, RaftGroupStateMachine::new(placement())))
    }
}

/// Opens `placement()`'s store on a fresh per-core journal under `root`; the
/// store owns the core writer, so dropping it closes the journal.
#[cfg(not(madsim))]
fn open_core_journal_store(root: &Path) -> Result<Arc<RaftGroupFileLogStore>, crate::RaftWalError> {
    fn topology() -> ursula_shard::StaticShardMap {
        ursula_shard::StaticShardMap::new(1, 1).expect("valid test topology")
    }
    RaftWal::start(root, WalFsync::Always, &topology())?.open(
        placement(),
        ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics(),
    )
}

#[cfg(not(madsim))]
fn start_wal(root: &Path, cores: usize, groups: usize) -> RaftWal {
    RaftWal::start(
        root,
        WalFsync::Always,
        &ursula_shard::StaticShardMap::new(cores, groups).expect("valid topology"),
    )
    .expect("start the Raft WAL")
}

/// `placement()`'s log store on a fresh WAL in a new temporary directory,
/// which the caller keeps while the store is in use.
#[cfg(not(madsim))]
fn fresh_journal_store() -> (tempfile::TempDir, Arc<RaftGroupFileLogStore>) {
    let root = tempfile::tempdir().expect("WAL root");
    let store = RaftWal::start(
        root.path(),
        WalFsync::Never,
        &ursula_shard::StaticShardMap::new(1, 1).expect("valid topology"),
    )
    .expect("start the Raft WAL")
    .open(
        placement(),
        ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics(),
    )
    .expect("open the log store");
    (root, store)
}

/// `placement()`'s log store on a fresh WAL on the simulated disk.
#[cfg(madsim)]
fn sim_journal_store(name: &str) -> Arc<RaftGroupFileLogStore> {
    let root = SimDisk::provision_dir(name).expect("provision a simulated WAL");
    RaftWal::start(
        root,
        WalFsync::Never,
        &ursula_shard::StaticShardMap::new(1, 1).expect("valid topology"),
    )
    .expect("start the Raft WAL")
    .open(
        placement(),
        ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics(),
    )
    .expect("open the log store")
}

/// The frames of core 0's journal under the WAL `root`, all segments.
#[cfg(not(madsim))]
fn core_journal_frames(root: &Path) -> usize {
    crate::log_store::journal_segments(&root.join("core-0"))
        .expect("list the journal segments")
        .iter()
        .map(|(_, path)| wire_frame_count::<CoreJournalRecord>(path))
        .sum()
}

#[cfg(not(madsim))]
#[tokio::test]
async fn raft_file_log_store_passes_openraft_conformance_suite() {
    Suite::test_all(FileLogStoreBuilder)
        .await
        .expect("OpenRaft file log conformance");
}

#[test]
fn reads_that_may_hold_only_for_a_lagging_state() {
    for code in [
        StreamErrorCode::StreamNotFound,
        StreamErrorCode::OffsetOutOfRange,
        StreamErrorCode::StreamGone,
    ] {
        assert!(may_hold_only_for_a_lagging_state(
            &GroupEngineError::stream(code, "lagging")
        ));
    }
    for code in [
        StreamErrorCode::StreamClosed,
        StreamErrorCode::IncarnationMismatch,
    ] {
        assert!(!may_hold_only_for_a_lagging_state(
            &GroupEngineError::stream(code, "final")
        ));
    }
}

#[test]
fn group_engine_error_codec_round_trips_stream_context() {
    let err = GroupEngineError::stream_with_context(
        StreamErrorCode::ProducerSeqConflict,
        "producer conflict",
        Some(9),
        vec![StreamErrorContext::ProducerSeqConflict {
            expected_seq: 8,
            received_seq: 3,
        }],
    );

    let decoded: GroupEngineError =
        decode_wire(&encode_wire(&err), "group engine error").expect("decode group engine error");

    assert_eq!(decoded, err);
}

#[test]
fn group_engine_error_codec_round_trips_stale_cold_flush_context() {
    let err = GroupEngineError::stream_with_context(
        StreamErrorCode::InvalidColdFlush,
        "cold flush candidate is stale",
        Some(17),
        vec![StreamErrorContext::StaleColdFlushCandidate],
    );

    let decoded: GroupEngineError =
        decode_wire(&encode_wire(&err), "group engine error").expect("decode group engine error");

    assert_eq!(decoded, err);
}

#[test]
fn group_engine_error_codec_round_trips_cold_backpressure_kind() {
    let stream_id = bsid("cold-backpressure-codec");
    let err = GroupEngineError::cold_backpressure(stream_id.clone(), 4, 5, 4);

    let decoded: GroupEngineError =
        decode_wire(&encode_wire(&err), "group engine error").expect("decode group engine error");

    assert_eq!(decoded, err);
    assert!(matches!(
        decoded,
        GroupEngineError::Infra(GroupInfraError::ColdBackpressure {
            stream_id: decoded_stream_id,
            before_group_hot_bytes: 4,
            after_group_hot_bytes: 5,
            limit: 4,
            ..
        }) if decoded_stream_id == stream_id
    ));
}

#[test]
fn raft_wire_encoding_is_byte_stable_across_round_trips() {
    // The raft RPC envelope and the on-disk log records carry openraft's own
    // serde types as MessagePack. DST determinism requires the encoding of a
    // value to be byte-stable, so re-encode a decoded copy and compare bytes.
    let membership = openraft::Membership::new(
        vec![BTreeSet::from([1u64, 2, 3])],
        BTreeMap::from([
            (1u64, BasicNode {
                addr: "node-1:4437".to_owned(),
            }),
            (2u64, BasicNode {
                addr: "node-2:4437".to_owned(),
            }),
            (3u64, BasicNode {
                addr: "node-3:4437".to_owned(),
            }),
        ]),
    )
    .expect("valid membership");
    let request = UrsulaAppendEntriesRequest {
        vote: openraft::Vote::new_committed(7, 1),
        prev_log_id: Some(log_id(3)),
        entries: vec![
            Entry::new(log_id(4), EntryPayload::Blank),
            normal_entry(5, create_stream_command("wire-stable")),
            Entry::new(log_id(6), EntryPayload::Membership(membership)),
        ],
        leader_commit: Some(log_id(4)),
    };

    let encoded = encode_wire(&request);
    let decoded: UrsulaAppendEntriesRequest =
        decode_wire(&encoded, "append request").expect("decode append request");
    assert_eq!(encode_wire(&decoded), encoded);

    let record = RaftGroupLogRecord::Append(decoded.entries.clone());
    let encoded_record = encode_wire(&record);
    let decoded_record: RaftGroupLogRecord =
        decode_wire(&encoded_record, "raft group log record").expect("decode log record");
    assert_eq!(encode_wire(&decoded_record), encoded_record);
}

#[test]
fn required_missing_proto_field_returns_typed_proto_decode_error() {
    let err = required::<u64>(None, "group_engine_error.error")
        .expect_err("missing required field should decode to an error");

    assert!(matches!(
        err,
        GroupEngineError::Infra(GroupInfraError::ProtoDecode {
            field,
            ..
        }) if field == "group_engine_error.error"
    ));
}

#[test]
fn group_engine_error_codec_round_trips_proto_decode_kind() {
    let err = GroupEngineError::Infra(GroupInfraError::ProtoDecode {
        field: "group_engine_error.error".to_owned(),
    });

    let decoded: GroupEngineError =
        decode_wire(&encode_wire(&err), "group engine error").expect("decode group engine error");

    assert_eq!(decoded, err);
}

fn placement() -> ShardPlacement {
    ShardPlacement {
        core_id: CoreId(0),
        shard_id: ShardId(0),
        raft_group_id: RaftGroupId(0),
    }
}

fn bsid(name: &str) -> ursula_shard::BucketStreamId {
    ursula_shard::BucketStreamId::new("benchcmp", name)
}

fn read_req(stream_id: ursula_shard::BucketStreamId, max_len: usize) -> ReadStreamRequest {
    ReadStreamRequest {
        stream_id,
        offset: 0,
        max_len,
        now_ms: 0,
        leader_only: false,
        read_index: None,
        follower: FollowerRead::Prefix,
    }
}

fn raft_config(cluster_name: &str, election_min: u64, election_max: u64) -> Arc<Config> {
    Arc::new(
        Config {
            cluster_name: cluster_name.to_owned(),
            heartbeat_interval: 10,
            election_timeout_min: election_min,
            election_timeout_max: election_max,
            ..Default::default()
        }
        .validate()
        .expect("valid raft config"),
    )
}

fn create_req(stream_id: ursula_shard::BucketStreamId) -> CreateStreamRequest {
    CreateStreamRequest::new(stream_id, "application/octet-stream")
}

fn create_command(stream_id: ursula_shard::BucketStreamId) -> GroupWriteCommand {
    GroupWriteCommand::from(create_req(stream_id))
}

/// A group's genesis history event, for a test that builds a log or writes
/// to a raw Raft core: a group applies nothing before it (#581 section 4.3).
pub(crate) fn genesis_command() -> GroupWriteCommand {
    GroupWriteCommand::Stream(ursula_stream::StreamCommand::NewHistory {
        history: std::num::NonZeroU64::new(1).expect("non-zero"),
        now_ms: 0,
        previous: 0,
        kept: 0,
        leader: ursula_stream::HistoryLeader {
            term: 0,
            node_id: 0,
        },
    })
}

fn append_command(stream_id: ursula_shard::BucketStreamId, payload: &[u8]) -> GroupWriteCommand {
    GroupWriteCommand::from(AppendRequest::from_bytes(stream_id, payload.to_vec()))
}

#[cfg(not(madsim))]
fn hosted_config(core_count: usize, raft_group_count: usize) -> RuntimeConfig {
    let mut config = RuntimeConfig::new(core_count, raft_group_count);
    config.threading = RuntimeThreading::HostedTokio;
    config
}

async fn shutdown_all(engines: &[RaftGroupEngine]) {
    for engine in engines {
        engine.shutdown().await.expect("shutdown raft group node");
    }
}

/// Build a three-node in-process cluster, initialize it, and wait for a
/// leader. Returns the registry, the engines (index = node id - 1), the
/// elected leader id and the WAL directories, which must outlive the engines.
#[cfg(not(madsim))]
async fn build_three_node_cluster(
    cluster_name: &str,
    policy: Option<InProcessRaftNetworkPolicy>,
) -> (
    InProcessRaftRegistry,
    Vec<RaftGroupEngine>,
    u64,
    Vec<tempfile::TempDir>,
) {
    build_three_node_cluster_at(cluster_name, policy, |node_id| format!("node-{node_id}")).await
}

/// [`build_three_node_cluster`] with each node advertising `address(node_id)`.
/// The in-process network delivers Raft RPCs by node id, so only leader
/// forwarding (gRPC) dials these addresses.
#[cfg(not(madsim))]
async fn build_three_node_cluster_at(
    cluster_name: &str,
    policy: Option<InProcessRaftNetworkPolicy>,
    address: impl Fn(u64) -> String,
) -> (
    InProcessRaftRegistry,
    Vec<RaftGroupEngine>,
    u64,
    Vec<tempfile::TempDir>,
) {
    let registry = InProcessRaftRegistry::default();
    // OpenRaft also uses election_timeout_min as the vote RPC deadline.
    // Leave room for concurrent tests fsyncing votes on the same executor host.
    let config = raft_config(cluster_name, 500, 1_000);
    let mut nodes = BTreeMap::new();
    for node_id in 1..=3 {
        nodes.insert(node_id, BasicNode::new(address(node_id)));
    }

    let mut engines = Vec::new();
    let mut wal_roots = Vec::new();
    for node_id in 1..=3 {
        let mut network = InProcessRaftNetworkFactory::new(registry.clone()).with_source(node_id);
        if let Some(policy) = &policy {
            network = network.with_policy(policy.clone());
        }
        let (wal_root, log_store) = fresh_journal_store();
        let engine = RaftGroupEngine::new_node_with_log_store_and_network(
            placement(),
            node_id,
            config.clone(),
            network,
            log_store,
            None,
            None,
        )
        .await
        .expect("create cluster raft group node");
        registry.register(node_id, &engine);
        engines.push(engine);
        wal_roots.push(wal_root);
    }

    engines[0]
        .raft
        .initialize(nodes)
        .await
        .expect("initialize raft group");
    let leader_id = tokio::time::timeout(Duration::from_secs(5), async {
        let leader_metrics = engines[0]
            .raft
            .wait(None)
            .metrics(|metrics| metrics.current_leader.is_some(), "leader elected")
            .await
            .expect("wait for leader");
        let leader_id = leader_metrics.current_leader.expect("leader id");
        let leader_index = usize::try_from(leader_id).unwrap().checked_sub(1).unwrap();
        engines[leader_index]
            .raft
            .wait(None)
            .metrics(
                |metrics| {
                    metrics.state == openraft::ServerState::Leader
                        && metrics.current_leader == Some(leader_id)
                },
                "elected actor enters leader state",
            )
            .await
            .expect("wait for selected leader actor");
        let (vote, _) = registry
            .confirm_recovery_barrier(leader_id, placement().raft_group_id)
            .await
            .expect("selected leader confirms a fresh quorum before fixture writes");
        assert_eq!(*vote.leader_id().node_id(), leader_id);
        leader_id
    })
    .await
    .expect("election and quorum readiness share the existing five-second budget");
    (registry, engines, leader_id, wal_roots)
}

/// Read a stream through the engine's state machine; the outer result is the
/// state-machine access, the inner one the read itself.
async fn read_stream_via_state_machine(
    engine: &RaftGroupEngine,
    stream_id: ursula_shard::BucketStreamId,
    max_len: usize,
) -> Result<Result<ReadStreamResponse, GroupEngineError>, GroupEngineError> {
    engine
        .with_state_machine(move |state_machine| {
            Box::pin(async move {
                state_machine
                    .read_stream(read_req(stream_id, max_len), placement())
                    .await
            })
        })
        .await
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
    Entry::new(log_id(index), EntryPayload::Normal(command))
}

fn meta_log_id(index: u64) -> LogId<MetaLeaderId> {
    LogId {
        leader_id: MetaLeaderId::new(1, 1),
        index,
    }
}

fn meta_entry(
    index: u64,
    command: ControlCommand,
) -> <MetaRaftTypeConfig as openraft::RaftTypeConfig>::Entry {
    <MetaRaftTypeConfig as openraft::RaftTypeConfig>::Entry::new(
        meta_log_id(index),
        EntryPayload::Normal(command),
    )
}

fn register_node_command(node_id: u64) -> ControlCommand {
    ControlCommand::RegisterNode {
        node_id,
        client_url: format!("http://node{node_id}:4491"),
        cluster_url: format!("http://node{node_id}:4492"),
        labels: BTreeMap::new(),
        now_ms: 10,
    }
}

fn create_stream_command(name: &str) -> GroupWriteCommand {
    create_command(bsid(name))
}

#[test]
fn raft_group_command_round_trips_through_wire_codec() {
    let command = GroupWriteCommand::Stream(ursula_stream::StreamCommand::Append {
        stream_id: bsid("shared-wire-log"),
        content_type: Some("application/octet-stream".to_owned()),
        payload: b"abcd".to_vec().into(),
        close_after: false,
        stream_seq: None,
        producer: Some(ProducerRequest {
            producer_id: "writer-1".to_owned(),
            producer_epoch: 7,
            producer_seq: 42,
            digest: None,
            ..Default::default()
        }),
        now_ms: 123,
    });

    let encoded = encode_wire(&command);
    let decoded: GroupWriteCommand =
        decode_wire(&encoded, "group command").expect("decode wire command");
    assert_eq!(decoded, command);

    // Determinism: re-encoding the decoded value is byte-identical.
    assert_eq!(encode_wire(&decoded), encoded);
}

#[test]
fn raft_group_write_response_round_trips_through_wire_codec() {
    let response = GroupWriteResponse::CreateStream(ursula_runtime::CreateStreamResponse {
        placement: placement(),
        next_offset: 5,
        closed: false,
        already_exists: false,
        group_commit_index: 11,
        hot_backlog: None,
        incarnation: 7,
    });

    let encoded = encode_wire(&response);
    let decoded: GroupWriteResponse =
        decode_wire(&encoded, "group write response").expect("decode wire response");
    assert_eq!(decoded, response);

    match write_result_from_raft_response(RaftGroupResponse::Write(Ok(decoded)))
        .expect("domain response")
    {
        Ok(GroupWriteResponse::CreateStream(response)) => {
            assert_eq!(response.next_offset, 5);
            assert_eq!(response.group_commit_index, 11);
        }
        other => panic!("unexpected response: {other:?}"),
    }
}

#[cfg(not(madsim))]
fn temp_log_path(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time is after epoch")
        .as_nanos();
    std::env::temp_dir()
        .join("ursula-raft-tests")
        .join(format!("{name}-{}-{nonce}.bin", std::process::id()))
}

#[cfg(not(madsim))]
fn wire_frame_count<T: serde::Serialize + serde::de::DeserializeOwned>(path: &Path) -> usize {
    let bytes = fs::read(path).expect("read log file");
    read_wire_frames::<T>(&bytes)
        .expect("decode wire frames")
        .len()
}

#[cfg(not(madsim))]
#[tokio::test]
async fn raft_log_store_appends_reads_truncates_and_purges() {
    let (_root, mut store) = fresh_journal_store();
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

#[cfg(not(madsim))]
#[tokio::test]
async fn raft_log_store_persists_vote_and_committed_pointer() {
    let (_root, mut store) = fresh_journal_store();
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

#[cfg(not(madsim))]
#[tokio::test]
async fn raft_log_store_rejects_holes() {
    let (_root, mut store) = fresh_journal_store();
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
async fn meta_raft_log_store_appends_reads_truncates_and_purges() {
    let mut store = MetaTestLogStore::shared();
    store
        .append(
            vec![
                meta_entry(1, register_node_command(1)),
                meta_entry(2, register_node_command(2)),
                meta_entry(3, register_node_command(3)),
            ],
            IOFlushed::noop(),
        )
        .await
        .expect("append meta log entries");

    let state = store.get_log_state().await.expect("log state");
    assert_eq!(state.last_purged_log_id, None);
    assert_eq!(state.last_log_id, Some(meta_log_id(3)));

    let mut reader = store.get_log_reader().await;
    let entries = reader
        .try_get_log_entries(1..4)
        .await
        .expect("read meta entries");
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].log_id, meta_log_id(1));
    assert_eq!(entries[2].log_id, meta_log_id(3));

    store
        .truncate_after(Some(meta_log_id(1)))
        .await
        .expect("truncate meta log");
    assert_eq!(
        store.get_log_state().await.expect("log state").last_log_id,
        Some(meta_log_id(1))
    );

    store
        .append(
            vec![
                meta_entry(2, register_node_command(4)),
                meta_entry(3, register_node_command(5)),
            ],
            IOFlushed::noop(),
        )
        .await
        .expect("append after truncate");
    store.purge(meta_log_id(2)).await.expect("purge meta log");

    let state = store.get_log_state().await.expect("log state after purge");
    assert_eq!(state.last_purged_log_id, Some(meta_log_id(2)));
    assert_eq!(state.last_log_id, Some(meta_log_id(3)));

    let entries = reader
        .try_get_log_entries(1..4)
        .await
        .expect("read after purge");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].log_id, meta_log_id(3));
}

#[tokio::test]
async fn meta_raft_log_store_persists_vote_and_committed_pointer() {
    let mut store = MetaTestLogStore::shared();
    let vote: VoteOf<MetaRaftTypeConfig> = openraft::Vote::new_committed(7, 1);

    store.save_vote(&vote).await.expect("save vote");
    let mut reader = store.get_log_reader().await;
    assert_eq!(reader.read_vote().await.expect("read vote"), Some(vote));

    store
        .save_committed(Some(meta_log_id(9)))
        .await
        .expect("save committed");
    assert_eq!(
        store.read_committed().await.expect("read committed"),
        Some(meta_log_id(9))
    );
}

#[tokio::test]
async fn meta_raft_log_store_rejects_holes() {
    let mut store = MetaTestLogStore::shared();
    let err = store
        .append(
            vec![
                meta_entry(1, register_node_command(1)),
                meta_entry(3, register_node_command(3)),
            ],
            IOFlushed::noop(),
        )
        .await
        .expect_err("hole should be rejected");

    assert_eq!(err.kind(), io::ErrorKind::InvalidData);

    store
        .append(
            vec![meta_entry(1, register_node_command(1))],
            IOFlushed::noop(),
        )
        .await
        .expect("append first entry");
    let err = store
        .append(
            vec![meta_entry(3, register_node_command(3))],
            IOFlushed::noop(),
        )
        .await
        .expect_err("cross-append hole should be rejected");

    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[cfg(not(madsim))]
#[tokio::test]
async fn raft_file_log_store_recovers_vote_committed_and_entries() {
    let root = tempfile::tempdir().expect("journal root");
    let vote: VoteOf<UrsulaRaftTypeConfig> = openraft::Vote::new_committed(7, 1);

    {
        let mut store = open_core_journal_store(root.path()).expect("open file log store");
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
    assert_eq!(
        core_journal_frames(root.path()),
        2,
        "the entries and the committed marker; the vote is in the metadata file"
    );

    let mut reopened = open_core_journal_store(root.path()).expect("reopen file log store");
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
}

#[cfg(not(madsim))]
#[tokio::test]
async fn raft_file_log_store_skips_duplicate_vote_and_committed_records() {
    let root = tempfile::tempdir().expect("journal root");
    let vote: VoteOf<UrsulaRaftTypeConfig> = openraft::Vote::new_committed(7, 1);

    {
        let mut store = open_core_journal_store(root.path()).expect("open file log store");
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
    assert_eq!(
        core_journal_frames(root.path()),
        1,
        "one committed marker; the vote is in the metadata file"
    );

    let mut reopened = open_core_journal_store(root.path()).expect("reopen file log store");
    assert_eq!(reopened.read_vote().await.expect("vote"), Some(vote));
    assert_eq!(
        reopened.read_committed().await.expect("committed"),
        Some(log_id(2))
    );
}

#[cfg(not(madsim))]
#[tokio::test]
async fn raft_file_log_store_recovers_truncate_and_purge() {
    let root = tempfile::tempdir().expect("journal root");

    {
        let mut store = open_core_journal_store(root.path()).expect("open file log store");
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
    assert_eq!(core_journal_frames(root.path()), 4);

    let mut reopened = open_core_journal_store(root.path()).expect("reopen file log store");
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
}

#[cfg(not(madsim))]
#[tokio::test]
async fn raft_file_log_restart_rebuilds_only_through_the_committed_marker() {
    let root = tempfile::tempdir().expect("journal root");
    let stream_id = bsid("committed-boundary");

    {
        let mut store = open_core_journal_store(root.path()).expect("open file log store");
        store
            .append(
                [
                    normal_entry(1, genesis_command()),
                    normal_entry(2, create_command(stream_id.clone())),
                    normal_entry(3, append_command(stream_id.clone(), b"committed")),
                    normal_entry(4, append_command(stream_id.clone(), b"-uncommitted")),
                ],
                IOFlushed::noop(),
            )
            .await
            .expect("append committed and uncommitted tail");
        store
            .save_committed(Some(log_id(3)))
            .await
            .expect("persist committed marker");
    }

    let mut reopened = open_core_journal_store(root.path()).expect("reopen file log store");
    let committed = reopened
        .read_committed()
        .await
        .expect("read committed marker")
        .expect("committed marker exists");
    assert_eq!(committed, log_id(3));
    let mut reader = reopened.get_log_reader().await;
    let durable_entries = reader
        .try_get_log_entries(1..5)
        .await
        .expect("read durable entries");
    assert_eq!(
        durable_entries.len(),
        4,
        "uncommitted tail stays replicable"
    );
    let committed_entries = durable_entries
        .into_iter()
        .take_while(|entry| entry.log_id.index <= committed.index)
        .collect::<Vec<_>>();

    let mut rebuilt = RaftGroupStateMachine::new(placement());
    rebuilt
        .apply(stream::iter(
            committed_entries.into_iter().map(|entry| Ok((entry, None))),
        ))
        .await
        .expect("rebuild state machine through committed marker");
    let read = rebuilt
        .engine
        .read_stream(read_req(stream_id, 64), placement())
        .await
        .expect("read rebuilt stream");
    assert_eq!(read.payload, b"committed");
}

#[cfg(not(madsim))]
#[tokio::test]
async fn single_node_meta_raft_applies_node_registration() {
    single_node_meta_raft_applies_node_registration_body().await;
}
#[cfg(madsim)]
#[test]
fn single_node_meta_raft_applies_node_registration() {
    crate::tests::check_madsim_determinism(7, madsim::Config::default(), || async {
        crate::sim_runtime::MadsimOpenRaftRuntime::scope(
            7,
            single_node_meta_raft_applies_node_registration_body(),
        )
        .await
    });
}
async fn single_node_meta_raft_applies_node_registration_body() {
    let config = raft_config("ursula-meta-single-node-test", 30, 60);
    let mut log_store = MetaTestLogStore::shared();
    let handle = MetaRaftHandle::new_single_node_with_log_store(
        1,
        BasicNode::new("meta-local"),
        config,
        log_store.clone(),
    )
    .await
    .expect("create single-node meta raft handle");

    let registered = handle
        .write(ControlCommand::RegisterNode {
            node_id: 4,
            client_url: "http://node4:4491/".to_owned(),
            cluster_url: "http://node4:4492/".to_owned(),
            labels: BTreeMap::from([("rack".to_owned(), "r1".to_owned())]),
            now_ms: 10,
        })
        .await
        .expect("register node through meta raft");
    assert_eq!(registered, ursula_control::ControlResponse::Ok);

    let updated = handle
        .write(ControlCommand::SetNodeState {
            node_id: 4,
            state: ursula_control::NodeState::Draining,
            now_ms: 20,
        })
        .await
        .expect("update registered node through meta raft");
    assert_eq!(updated, ursula_control::ControlResponse::Ok);

    let (applied, node) = handle
        .with_state_machine(|state_machine| {
            Box::pin(async move {
                let node = state_machine
                    .state()
                    .nodes
                    .get(&4)
                    .expect("node registered through raft")
                    .clone();
                (state_machine.applied_log_id(), node)
            })
        })
        .await
        .expect("read meta state machine");
    assert_eq!(node.client_url, "http://node4:4491");
    assert_eq!(node.cluster_url, "http://node4:4492");
    assert_eq!(node.state, ursula_control::NodeState::Draining);
    assert_eq!(node.labels.get("rack").map(String::as_str), Some("r1"));
    assert!(applied.is_some());

    let log_state = log_store.get_log_state().await.expect("meta log state");
    assert!(log_state.last_log_id.is_some());
    handle
        .shutdown()
        .await
        .expect("shutdown single-node meta raft handle");
}

#[cfg(not(madsim))]
#[tokio::test]
async fn meta_raft_handle_registers_initial_data_nodes() {
    meta_raft_handle_registers_initial_data_nodes_body().await;
}
#[cfg(madsim)]
#[test]
fn meta_raft_handle_registers_initial_data_nodes() {
    crate::tests::check_madsim_determinism(7, madsim::Config::default(), || async {
        crate::sim_runtime::MadsimOpenRaftRuntime::scope(
            7,
            meta_raft_handle_registers_initial_data_nodes_body(),
        )
        .await
    });
}
async fn meta_raft_handle_registers_initial_data_nodes_body() {
    let config = raft_config("ursula-meta-initial-data-nodes-test", 30, 60);
    let handle = MetaRaftHandle::new_single_node_with_log_store(
        1,
        BasicNode::new("meta-local"),
        config,
        MetaTestLogStore::shared(),
    )
    .await
    .expect("create single-node meta raft handle");

    handle
        .register_initial_data_nodes(
            [
                MetaNodeRegistration::new(1, "http://node1:4491/", "http://node1:4492/"),
                MetaNodeRegistration::new(2, "http://node2:4491", "http://node2:4492"),
                MetaNodeRegistration::new(3, "http://node3:4491", "http://node3:4492"),
            ],
            10,
        )
        .await
        .expect("register initial data nodes");

    let nodes = handle
        .read_state(|state| state.nodes.clone())
        .await
        .expect("read meta state");
    assert_eq!(nodes.len(), 3);
    let node1 = nodes.get(&1).expect("node 1 registered");
    assert_eq!(node1.client_url, "http://node1:4491");
    assert_eq!(node1.cluster_url, "http://node1:4492");
    assert_eq!(node1.state, ursula_control::NodeState::Active);
    assert_eq!(
        nodes.get(&2).map(|node| node.cluster_url.as_str()),
        Some("http://node2:4492")
    );
    assert_eq!(
        nodes.get(&3).map(|node| node.client_url.as_str()),
        Some("http://node3:4491")
    );

    handle
        .shutdown()
        .await
        .expect("shutdown single-node meta raft handle");
}

#[cfg(not(madsim))]
#[tokio::test]
async fn meta_raft_handle_rejects_invalid_initial_data_nodes() {
    meta_raft_handle_rejects_invalid_initial_data_nodes_body().await;
}
#[cfg(madsim)]
#[test]
fn meta_raft_handle_rejects_invalid_initial_data_nodes() {
    crate::tests::check_madsim_determinism(7, madsim::Config::default(), || async {
        crate::sim_runtime::MadsimOpenRaftRuntime::scope(
            7,
            meta_raft_handle_rejects_invalid_initial_data_nodes_body(),
        )
        .await
    });
}
async fn meta_raft_handle_rejects_invalid_initial_data_nodes_body() {
    let config = raft_config("ursula-meta-invalid-initial-data-nodes-test", 30, 60);
    let handle = MetaRaftHandle::new_single_node_with_log_store(
        1,
        BasicNode::new("meta-local"),
        config,
        MetaTestLogStore::shared(),
    )
    .await
    .expect("create single-node meta raft handle");

    let err = handle
        .register_initial_data_nodes(
            [
                MetaNodeRegistration::new(1, "http://node1:4491", "http://node1:4492"),
                MetaNodeRegistration::new(2, "  ", "http://node2:4492"),
            ],
            10,
        )
        .await
        .expect_err("reject invalid initial data node");
    assert!(matches!(
        std::error::Error::source(&err)
            .and_then(|source| source.downcast_ref::<ursula_control::ControlError>()),
        Some(ursula_control::ControlError::EmptyAddress {
            node_id: 2,
            endpoint: ursula_control::NodeEndpoint::Client
        })
    ));

    handle
        .shutdown()
        .await
        .expect("shutdown single-node meta raft handle");
}

#[cfg(not(madsim))]
#[tokio::test]
async fn meta_raft_handle_rejects_registration_during_an_operation_with_a_typed_source() {
    meta_raft_handle_rejects_registration_during_an_operation_with_a_typed_source_body().await;
}
#[cfg(madsim)]
#[test]
fn meta_raft_handle_rejects_registration_during_an_operation_with_a_typed_source() {
    crate::tests::check_madsim_determinism(7, madsim::Config::default(), || async {
        crate::sim_runtime::MadsimOpenRaftRuntime::scope(
            7,
            meta_raft_handle_rejects_registration_during_an_operation_with_a_typed_source_body(),
        )
        .await
    });
}
async fn meta_raft_handle_rejects_registration_during_an_operation_with_a_typed_source_body() {
    use ursula_control::OperationCommand;
    use ursula_control::OperationOutcome;
    let config = raft_config("ursula-meta-operation-active-test", 30, 60);
    let handle = MetaRaftHandle::new_single_node_with_log_store(
        1,
        BasicNode::new("meta-local"),
        config,
        MetaTestLogStore::shared(),
    )
    .await
    .expect("create single-node meta raft handle");
    let registration = |node_id: u64, host: &str| {
        MetaNodeRegistration::new(
            node_id,
            format!("http://{host}:4491"),
            format!("http://{host}:4492"),
        )
    };
    handle
        .register_initial_data_nodes((1..=4).map(|id| registration(id, &format!("node{id}"))), 10)
        .await
        .expect("register data nodes");
    let operation = |command| ControlCommand::Operation {
        command,
        now_ms: 10,
    };
    assert_eq!(
        handle
            .write(ControlCommand::SeedPlacement {
                raft_group_id: RaftGroupId(0),
                voters: BTreeSet::from([1, 2, 3]),
                now_ms: 10,
            })
            .await
            .expect("seed placement"),
        ursula_control::ControlResponse::Ok
    );
    let mut participants = BTreeMap::new();
    for node_id in 1..=4_u64 {
        let incarnation = ursula_control::ProcessIncarnation::from_bits(u128::from(node_id));
        let ursula_control::ControlResponse::Operation(Ok(OperationOutcome::ProcessClaimed(
            process,
        ))) = handle
            .write(operation(OperationCommand::ClaimProcess {
                node_id,
                expected_epoch: 0,
                incarnation: incarnation.clone(),
            }))
            .await
            .expect("claim process")
        else {
            panic!("claim process");
        };
        handle
            .write(operation(OperationCommand::RegisterReplica {
                node_id,
                process: process.clone(),
                identity: ursula_control::ReplicaIdentity {
                    generation: 1,
                    incarnation,
                },
            }))
            .await
            .expect("register replica");
        participants.insert(node_id, process);
    }
    let ursula_control::ControlResponse::Operation(Ok(OperationOutcome::Acquired(token))) = handle
        .write(operation(OperationCommand::Begin {
            kind: ursula_control::OperationKind::MoveReplicas {
                source: 1,
                target: 4,
                groups: BTreeSet::from([RaftGroupId(0)]),
            },
            executor: ursula_control::ProcessIncarnation::from_bits(100),
            participants,
        }))
        .await
        .expect("begin operation")
    else {
        panic!("begin operation");
    };

    handle
        .register_initial_data_nodes([registration(2, "node2-restarted")], 20)
        .await
        .expect("an address refresh is accepted during an operation");
    let err = handle
        .register_initial_data_nodes([registration(5, "node5")], 20)
        .await
        .expect_err("a new node waits for the operation");
    assert_eq!(
        std::error::Error::source(&err)
            .and_then(|source| source.downcast_ref::<ursula_control::ControlError>()),
        Some(&ursula_control::ControlError::OperationActive {
            operation_id: token.operation_id
        })
    );

    handle
        .shutdown()
        .await
        .expect("shutdown single-node meta raft handle");
}

#[cfg(not(madsim))]
#[test]
fn dynamic_group_hosting_allows_non_voter_warmup() {
    let wal_root = tempfile::tempdir().expect("WAL root");
    let registry = RaftGroupHandleRegistry::default();
    let factory = StaticGrpcRaftGroupEngineFactory::new(
        4,
        [
            (1, "node1".to_owned()),
            (2, "node2".to_owned()),
            (3, "node3".to_owned()),
            (4, "node4".to_owned()),
        ],
        false,
        registry.clone(),
        start_wal(wal_root.path(), 1, 3),
    )
    .with_per_group_voters(BTreeMap::from([(
        RaftGroupId(2),
        BTreeSet::from([1, 2, 3]),
    )]));
    let placement = ShardPlacement {
        shard_id: ShardId(0),
        core_id: CoreId(0),
        raft_group_id: RaftGroupId(2),
    };

    assert!(!factory.hosts_group(placement));
    registry.allow_dynamic_group_hosting(RaftGroupId(2));
    assert!(factory.hosts_group(placement));
}

#[cfg(not(madsim))]
#[tokio::test]
async fn single_node_openraft_group_applies_client_writes() {
    let config = raft_config("ursula-single-node-test", 30, 60);
    let (_wal_root, mut log_store) = fresh_journal_store();
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
    raft.client_write(genesis_command())
        .await
        .expect("the group's genesis");

    let stream_id = bsid("raft-client-write");
    let created = raft
        .client_write(create_command(stream_id.clone()))
        .await
        .expect("create stream through openraft");
    assert!(matches!(
        write_result_from_raft_response(created.data).expect("decode create response"),
        Ok(GroupWriteResponse::CreateStream(_))
    ));

    let appended = raft
        .client_write(append_command(stream_id, b"payload"))
        .await
        .expect("append through openraft");
    match write_result_from_raft_response(appended.data).expect("decode append response") {
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

/// A leader that still leads a group this long after it submitted a
/// transfer knows the transfer did not move the leadership: the transfer
/// deadline of two maximum election timeouts, then one minimum election
/// timeout for a refused step-down to be retried.
#[cfg(not(madsim))]
#[tokio::test]
async fn a_handoff_settles_within_the_transfer_deadline_and_one_minimum_election_timeout() {
    let config = raft_config("ursula-handoff-settle-time", 300, 700);
    let (_wal_root, log_store) = fresh_journal_store();
    let raft = Raft::<UrsulaRaftTypeConfig, RaftGroupStateMachine>::new(
        1,
        config,
        SingleNodeRaftNetworkFactory,
        log_store,
        RaftGroupStateMachine::new(placement()),
    )
    .await
    .expect("create single-node raft group");
    let owner = crate::owner::OwnerRaftHandle::new(raft.clone());
    // 2 x 700 ms + 300 ms.
    assert_eq!(owner.handoff_settle_time(), Duration::from_millis(1_700));
    raft.shutdown().await.expect("shutdown raft group");
}

#[cfg(not(madsim))]
#[tokio::test]
async fn three_node_openraft_group_replicates_group_writes() {
    let (_registry, engines, leader_id, _wal_roots) =
        build_three_node_cluster("ursula-three-node-test", None).await;
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
    let stream_id = bsid("three-node-raft-group-engine");
    create_stream_via_raft(&engines[leader_index], stream_id.clone()).await;

    let appended = engines[leader_index]
        .raft
        .client_write(append_command(stream_id.clone(), b"replicated"))
        .await
        .expect("append through elected leader");
    let appended_log_index = appended.log_id.index();
    match write_result_from_raft_response(appended.data).expect("decode append response") {
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
        let read = read_stream_via_state_machine(engine, stream_id, 16)
            .await
            .expect("read follower state machine")
            .expect("replicated stream is readable");
        assert_eq!(read.payload, b"replicated");
    }

    shutdown_all(&engines).await;
}

/// D10: a leader cut off from the quorum keeps believing it leads, but it
/// must not answer a `consistency=leader` read, HEAD, bootstrap, snapshot
/// read or live-read registration from its own state: a new leader has
/// acknowledged a write it never saw. Each answers a forward or a
/// leader-unknown error (HTTP 307 or 503), never the stale view.
#[cfg(not(madsim))]
#[tokio::test]
async fn deposed_leader_refuses_linearizable_reads() {
    let policy = InProcessRaftNetworkPolicy::default();
    let (_registry, mut engines, old_leader, _wal_roots) =
        build_three_node_cluster("ursula-deposed-leader-read-test", Some(policy.clone())).await;
    let old_index = usize::try_from(old_leader - 1).expect("leader id fits usize");
    let stream_id = bsid("deposed-leader-read");
    create_stream_via_raft(&engines[old_index], stream_id.clone()).await;
    engines[old_index]
        .raft
        .client_write(append_command(stream_id.clone(), b"before"))
        .await
        .expect("append before the partition");
    let leader_read = ReadStreamRequest {
        leader_only: true,
        ..read_req(stream_id.clone(), 64)
    };
    let healthy = engines[old_index]
        .read_stream(leader_read.clone(), placement())
        .await
        .expect("linearizable read on a healthy leader");
    assert_eq!(healthy.payload, b"before");

    for node_id in (1..=3).filter(|node_id| *node_id != old_leader) {
        policy.partition_bidirectional(old_leader, node_id);
    }
    let survivor_index = usize::from(old_index == 0);
    let new_leader = engines[survivor_index]
        .raft
        .wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| {
                metrics
                    .current_leader
                    .is_some_and(|leader| leader != old_leader)
            },
            "majority elects a new leader",
        )
        .await
        .expect("wait for a new leader")
        .current_leader
        .expect("new leader id");
    let new_index = usize::try_from(new_leader - 1).expect("leader id fits usize");
    let acked = engines[new_index]
        .raft
        .client_write(append_command(stream_id.clone(), b"after"))
        .await
        .expect("append acknowledged by the new leader");
    match write_result_from_raft_response(acked.data).expect("decode append response") {
        Ok(GroupWriteResponse::Append(response)) => assert_eq!(response.next_offset, 11),
        other => panic!("unexpected append response: {other:?}"),
    }
    assert!(
        engines[old_index].raft.is_leader(),
        "the cut-off leader still believes it leads, so only the read barrier stops it"
    );

    let old = &mut engines[old_index];
    assert_refused(
        "consistency=leader read",
        old.read_stream(leader_read, placement()).await,
    );
    assert_refused(
        "HEAD",
        old.head_stream(
            HeadStreamRequest {
                stream_id: stream_id.clone(),
                now_ms: 0,
                linearizable: true,
                read_index: None,
            },
            placement(),
        )
        .await,
    );
    assert_refused(
        "bootstrap",
        old.bootstrap_stream(
            BootstrapStreamRequest {
                stream_id: stream_id.clone(),
                now_ms: 0,
                read_index: None,
            },
            placement(),
        )
        .await,
    );
    assert_refused(
        "snapshot read",
        old.read_snapshot(
            ReadSnapshotRequest {
                stream_id: stream_id.clone(),
                snapshot_offset: None,
                now_ms: 0,
                read_index: None,
            },
            placement(),
        )
        .await,
    );
    assert_refused(
        "live-read registration",
        old.open_live_read(
            HeadStreamRequest {
                stream_id: stream_id.clone(),
                now_ms: 0,
                linearizable: true,
                read_index: None,
            },
            placement(),
        )
        .await,
    );

    shutdown_all(&engines).await;
}

#[track_caller]
#[cfg(not(madsim))]
fn assert_refused<T>(what: &str, result: Result<T, GroupEngineError>) {
    match result {
        Ok(_) => panic!("{what}: the deposed leader served its stale view"),
        Err(err) => {
            assert!(
                err.leader_hint().is_some(),
                "{what}: expected a forward or leader-unknown error, got {err:?}"
            );
            // openraft's quorum error lists every member's address; the client
            // sees a fixed message instead.
            assert!(
                !err.to_string().contains("node-"),
                "{what}: refusal leaks member addresses: {err}"
            );
        }
    }
}

/// The engine's one leadership check, which runs right before commands go to
/// OpenRaft, reports its forward as raised before proposal: on its own,
/// inside `write`, and inside a batch, for every entry. A forward that
/// OpenRaft reports for a write handed to it stays ambiguous, since OpenRaft
/// also reports one when it drops an entry it appended before it lost
/// leadership, which may still commit
/// (`a_write_handed_to_openraft_is_not_refused_before_proposal_after_a_step_down`).
/// The HTTP layer answers the first kind `NO_LEADER` and the second
/// `OUTCOME_UNKNOWN`.
#[cfg(not(madsim))]
#[tokio::test]
async fn only_the_engines_own_leadership_check_is_before_proposal() {
    let (_registry, engines, leader_id, _wal_roots) =
        build_three_node_cluster("forward-before-proposal", None).await;
    let leader_index = usize::try_from(leader_id)
        .ok()
        .and_then(|id| id.checked_sub(1))
        .expect("node ids start at 1");
    let follower = &engines[(leader_index + 1) % 3];
    follower
        .raft
        .wait(Some(Duration::from_secs(5)))
        .current_leader(leader_id, "the follower learns the leader")
        .await
        .expect("the follower learns the leader");
    let stream_id = bsid("forward-before-proposal");
    let names_the_leader = |error: &GroupEngineError| {
        error.leader_hint().and_then(|hint| hint.node_id) == Some(leader_id)
    };

    let checked = follower
        .forward_write_to_leader_if_follower(append_command(stream_id.clone(), b"a"))
        .await
        .expect_err("the engine's check refuses a write on a follower");
    assert!(checked.is_forward_before_proposal(), "{checked:?}");
    assert!(names_the_leader(&checked), "{checked:?}");

    let written = follower
        .write(append_command(stream_id.clone(), b"a"))
        .await
        .expect_err("the engine's check refuses a write on a follower");
    assert!(written.is_forward_before_proposal(), "{written:?}");
    assert!(names_the_leader(&written), "{written:?}");

    let batch =
        crate::forward::write_commands_on_raft(follower.raft_handle(), &follower.history, vec![
            append_command(stream_id.clone(), b"a"),
            append_command(stream_id, b"b"),
        ])
        .await
        .expect("the batch is answered entry by entry");
    assert_eq!(batch.len(), 2);
    for entry in batch {
        let error = entry.expect_err("the engine's check refuses each batch entry on a follower");
        assert!(error.is_forward_before_proposal(), "{error:?}");
        assert!(names_the_leader(&error), "{error:?}");
    }

    shutdown_all(&engines).await;
}

/// A write that the engine hands to OpenRaft on a leader, after its own
/// leadership check.
#[cfg(not(madsim))]
#[derive(Debug, Clone, Copy)]
enum WriteToOpenRaft {
    /// An inline create, with cold admission on.
    Create,
    /// An external create.
    CreateExternal,
    Append,
    /// Two appends, which a leader hands to `client_write_many` together.
    AppendBatch,
    Close,
    AppendExternal,
}

#[cfg(not(madsim))]
impl WriteToOpenRaft {
    const ALL: [Self; 6] = [
        Self::Create,
        Self::CreateExternal,
        Self::Append,
        Self::AppendBatch,
        Self::Close,
        Self::AppendExternal,
    ];

    /// The entries the write appends to a leader's log.
    fn entries(self) -> u64 {
        match self {
            Self::AppendBatch => 2,
            Self::Create
            | Self::CreateExternal
            | Self::Append
            | Self::Close
            | Self::AppendExternal => 1,
        }
    }

    /// Runs the write through `engine`'s [`GroupEngine`] entry point and
    /// returns the outcome of each of its commands.
    async fn run(
        self,
        engine: &mut RaftGroupEngine,
        stream_id: &ursula_shard::BucketStreamId,
    ) -> Vec<Result<(), GroupEngineError>> {
        // Cold admission is on, as `storage.cold.max_hot_size_per_group` is
        // by default, so the engine checks leadership for the create too.
        let admission = ColdWriteAdmission {
            max_hot_bytes_per_group: Some(1 << 20),
        };
        let append = || AppendRequest::from_bytes(stream_id.clone(), b"abc".to_vec());
        match self {
            Self::Create => vec![
                engine
                    .create_stream(
                        create_req(bsid("created-on-a-deposed-leader")),
                        placement(),
                        admission,
                    )
                    .await
                    .map(drop),
            ],
            Self::CreateExternal => vec![
                engine
                    .create_stream_external(
                        CreateStreamExternalRequest::from_create_request(
                            create_req(bsid("created-external-on-a-deposed-leader")),
                            ExternalPayloadRef {
                                s3_path: "external/staged-before-the-step-down".to_owned(),
                                payload_len: 3,
                                object_size: 3,
                            },
                            Vec::new(),
                        ),
                        placement(),
                    )
                    .await
                    .map(drop),
            ],
            Self::Append => vec![
                engine
                    .append(append(), placement(), admission)
                    .await
                    .map(drop),
            ],
            Self::AppendBatch => engine
                .append_batch(vec![append(), append()], placement(), admission)
                .await
                .into_iter()
                .map(|outcome| outcome.map(drop))
                .collect(),
            Self::Close => vec![
                engine
                    .close_stream(
                        CloseStreamRequest {
                            stream_id: stream_id.clone(),
                            stream_seq: None,
                            producer: None,
                            now_ms: 0,
                            if_incarnation: None,
                        },
                        placement(),
                    )
                    .await
                    .map(drop),
            ],
            Self::AppendExternal => vec![
                engine
                    .append_external(
                        AppendExternalRequest {
                            stream_id: stream_id.clone(),
                            content_type: "application/octet-stream".to_owned(),
                            payload: ExternalPayloadRef {
                                s3_path: "external/staged-before-the-step-down".to_owned(),
                                payload_len: 3,
                                object_size: 3,
                            },
                            record_ends: Vec::new(),
                            close_after: false,
                            stream_seq: None,
                            producer: None,
                            now_ms: 0,
                            if_incarnation: None,
                        },
                        placement(),
                    )
                    .await
                    .map(drop),
            ],
        }
    }
}

/// A write the engine handed to OpenRaft is never refused before proposal,
/// even by a node that lost leadership since. Each write below reaches
/// OpenRaft on a leader cut off from its followers, after the engine's own
/// leadership check. The leader appends the
/// write's entries but cannot commit them. The followers
/// elect a new leader, and once the partition heals, the new leader's log
/// replaces the entries and OpenRaft answers the write with a forward to it.
/// Had a follower stored an entry first, the new leader could have committed
/// it instead, and the deposed node cannot tell the two apart. That node's
/// own leadership check now refuses before proposal, so running the check
/// again after the hand-off, or reporting OpenRaft's forward as raised
/// before proposal, would treat a write that may have committed as never
/// proposed. The HTTP layer deletes a staged external payload exactly after
/// the engine's errors that [`GroupEngineError::is_refused_before_proposal`]
/// reports, so no answer here may report one.
#[cfg(not(madsim))]
#[tokio::test]
async fn a_write_handed_to_openraft_is_not_refused_before_proposal_after_a_step_down() {
    let policy = InProcessRaftNetworkPolicy::default();
    let (_registry, mut engines, mut leader_id, _wal_roots) =
        build_three_node_cluster("handed-to-openraft", Some(policy.clone())).await;
    let rafts: Vec<_> = engines.iter().map(RaftGroupEngine::raft_handle).collect();
    let index = |node_id: u64| {
        usize::try_from(node_id)
            .ok()
            .and_then(|id| id.checked_sub(1))
            .expect("node ids start at 1")
    };
    let stream_id = bsid("handed-to-openraft");
    create_stream_via_raft(&engines[index(leader_id)], stream_id.clone()).await;

    for write in WriteToOpenRaft::ALL {
        for raft in &rafts {
            raft.wait(Some(Duration::from_secs(10)))
                .current_leader(leader_id, "every node follows the leader")
                .await
                .expect("every node follows the leader");
        }
        let followers: Vec<u64> = (1..=3).filter(|node_id| *node_id != leader_id).collect();
        let leader = &rafts[index(leader_id)];
        // The leader's log once it has applied all of it, the blank entry a
        // new leader appends included: metrics published before then can lag
        // the log, and the wait below would then end before the write is
        // appended.
        let settled = leader
            .wait(Some(Duration::from_secs(10)))
            .metrics(
                |metrics| {
                    metrics.current_leader == Some(leader_id)
                        && metrics.last_log_index.is_some()
                        && metrics.last_applied.as_ref().map(|applied| applied.index())
                            == metrics.last_log_index
                },
                "the leader applied its whole log",
            )
            .await
            .expect("the leader applied its whole log");
        let appended = settled
            .last_log_index
            .expect("the leader holds entries")
            .checked_add(write.entries());
        for follower in &followers {
            policy.partition_bidirectional(leader_id, *follower);
        }
        let depose = async {
            leader
                .wait(Some(Duration::from_secs(5)))
                .log_index_at_least(appended, "the cut-off leader appends the write")
                .await
                .expect("the cut-off leader appends the write");
            let new_leader = rafts[index(followers[0])]
                .wait(Some(Duration::from_secs(10)))
                .metrics(
                    |metrics| metrics.current_leader.is_some_and(|id| id != leader_id),
                    "the followers elect a new leader",
                )
                .await
                .expect("the followers elect a new leader")
                .current_leader
                .expect("the new leader's id");
            for follower in &followers {
                policy.heal_bidirectional(leader_id, *follower);
            }
            new_leader
        };
        let (outcomes, new_leader) = tokio::time::timeout(Duration::from_secs(30), async {
            tokio::join!(
                write.run(&mut engines[index(leader_id)], &stream_id),
                depose
            )
        })
        .await
        .unwrap_or_else(|_elapsed| panic!("{write:?}: the deposed leader never answered"));

        assert_eq!(
            u64::try_from(outcomes.len()).expect("outcome count fits u64"),
            write.entries(),
            "{write:?}"
        );
        for outcome in outcomes {
            let Err(error) = outcome else {
                panic!("{write:?}: an entry the new leader replaced was applied");
            };
            assert!(!error.is_refused_before_proposal(), "{write:?}: {error:?}");
            assert_eq!(
                error.leader_hint().and_then(|hint| hint.node_id),
                Some(new_leader),
                "{write:?}: {error:?}"
            );
        }
        // The deposed node now follows, so its own check refuses before
        // proposal. Running it after the hand-off would misclassify the
        // write.
        let checked = engines[index(leader_id)]
            .forward_write_to_leader_if_follower(append_command(stream_id.clone(), b"abc"))
            .await
            .expect_err("the deposed node follows the new leader");
        assert!(
            checked.is_forward_before_proposal(),
            "{write:?}: {checked:?}"
        );
        // The hint names the new leader with the term this node knows it
        // in, from one view, so the front door can redirect typed.
        let hint = checked.leader_hint().expect("a leader hint");
        assert_eq!(hint.node_id, Some(new_leader), "{write:?}: {checked:?}");
        assert!(
            hint.term.is_some_and(|term| term > 0),
            "{write:?}: {checked:?}"
        );
        leader_id = new_leader;
    }

    shutdown_all(&engines).await;
}

/// A write is routed before its body (#581 sections 2.5 and 6.1): the leader
/// answers a producer write early, and a follower names the leader and its
/// term instead.
#[cfg(not(madsim))]
#[tokio::test]
async fn writes_are_routed_before_their_body() {
    let (_registry, mut engines, leader_id, _wal_roots) =
        build_three_node_cluster("early-answer", None).await;
    let leader_index = usize::try_from(leader_id - 1).expect("leader id fits usize");
    let stream_id = bsid("early-answer");
    create_stream_via_raft(&engines[leader_index], stream_id.clone()).await;
    let producer = ProducerRequest {
        producer_id: "writer".to_owned(),
        producer_epoch: 0,
        producer_seq: 0,
        digest: None,
        ..Default::default()
    };
    let mut append = AppendRequest::from_bytes(stream_id.clone(), b"abc".to_vec());
    append.producer = Some(producer.clone());
    let appended = engines[leader_index]
        .raft
        .client_write(GroupWriteCommand::from(append))
        .await
        .expect("append through the leader");
    let index = appended.log_id.index();
    let incarnation = match write_result_from_raft_response(appended.data).expect("decode") {
        Ok(GroupWriteResponse::Append(response)) => response.incarnation,
        other => panic!("unexpected append response: {other:?}"),
    };
    for engine in &engines {
        engine
            .raft
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(Some(index), "append applied")
            .await
            .expect("wait for apply");
    }
    let request = RouteWriteRequest {
        stream_id,
        early_answer: Some(EarlyAnswerQuery {
            producer,
            incarnation,
            now_ms: 0,
        }),
    };
    for (index, engine) in engines.iter_mut().enumerate() {
        let route = engine
            .route_write(request.clone(), placement())
            .await
            .expect("route lookup");
        if index == leader_index {
            assert!(
                matches!(route, WriteRoute::Leader {
                    early_answer: Some(ProducerEarlyAnswer::Repeat(_))
                }),
                "{route:?}"
            );
        } else {
            // A follower answers nothing early: it names the leader.
            assert!(
                matches!(route, WriteRoute::Follower { leader_id: id, term, .. }
                    if id == leader_id && term > 0),
                "{route:?}"
            );
        }
    }
    shutdown_all(&engines).await;
}

// NOTE: tokio-level fault-injection tests for partition/heal replication and
// the "minority leader must not ack" invariant were removed; those scenarios
// are covered by ursula-sim DST scenarios PartitionHeal, SnapshotCatchUp,
// RuntimeRaftNetwork, and isolated_leader_pending_write_snapshot_purge.

#[cfg(madsim)]
static MADSIM_TEST_LOCK: Mutex<()> = Mutex::new(());

#[cfg(madsim)]
pub(crate) fn madsim_test_guard() -> std::sync::MutexGuard<'static, ()> {
    MADSIM_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(madsim)]
pub(crate) fn check_madsim_determinism<F>(
    seed: u64,
    config: madsim::Config,
    f: fn() -> F,
) -> F::Output
where
    F: std::future::Future + 'static,
    F::Output: Send,
{
    let _guard = madsim_test_guard();
    madsim::runtime::Runtime::check_determinism(seed, config, f)
}

/// The madsim OpenRaft runtime, its own watch channel included, behaves as
/// OpenRaft's runtime suite expects.
#[cfg(madsim)]
#[test]
fn madsim_openraft_runtime_passes_the_openraft_runtime_suite() {
    let _guard = madsim_test_guard();
    openraft_rt::testing::Suite::<crate::sim_runtime::MadsimRuntime>::test_all();
}

#[cfg(madsim)]
#[test]
fn madsim_three_node_openraft_group_replicates_group_writes_deterministically() {
    let _guard = madsim_test_guard();
    let first = run_madsim_three_node_raft_once(7);
    let second = run_madsim_three_node_raft_once(7);
    assert_eq!(first, second);
}

#[cfg(madsim)]
#[test]
fn madsim_in_process_raft_network_delay_policy_replicates_deterministically() {
    let _guard = madsim_test_guard();
    let first = run_madsim_three_node_raft_with_network_delay_once(7, Duration::from_millis(1));
    let second = run_madsim_three_node_raft_with_network_delay_once(7, Duration::from_millis(1));
    assert_eq!(first, second);
}

#[cfg(madsim)]
#[test]
fn madsim_fault_script_partitions_heals_and_replays_by_seed() {
    let _guard = madsim_test_guard();
    let first = run_madsim_three_node_raft_with_fault_script_once(11);
    let second = run_madsim_three_node_raft_with_fault_script_once(11);
    assert_eq!(first, second);
}

#[cfg(madsim)]
#[test]
#[ignore = "diagnostic probe; full append/read strict replay is the default smoke test"]
fn madsim_three_node_openraft_group_strict_replay_probe() {
    check_madsim_determinism(7, madsim::Config::default(), || async {
        crate::sim_runtime::MadsimOpenRaftRuntime::scope(7, async {
            let (_, engines, leader_id) = build_madsim_three_node_raft_cluster(7).await;
            let leader_index = usize::try_from(leader_id - 1).expect("leader id fits usize");
            let stream_id = bsid("madsim-strict-replay-probe");
            create_stream_via_raft(&engines[leader_index], stream_id.clone()).await;
            shutdown_all(&engines).await;
            assert!((1..=3).contains(&leader_id));
        })
        .await;
    });
}

#[cfg(madsim)]
#[test]
#[ignore = "diagnostic probe; full append/read strict replay is the default smoke test"]
fn madsim_three_node_openraft_group_strict_replay_append_enqueue_probe() {
    check_madsim_determinism(7, madsim::Config::default(), || async {
        crate::sim_runtime::MadsimOpenRaftRuntime::scope(7, async {
            let (_, engines, leader_id) = build_madsim_three_node_raft_cluster(7).await;
            let leader_index = usize::try_from(leader_id - 1).expect("leader id fits usize");
            let stream_id = bsid("madsim-strict-replay-enqueue");
            create_stream_via_raft(&engines[leader_index], stream_id.clone()).await;
            engines[leader_index]
                .raft
                .client_write_ff(append_command(stream_id, b"simulated"), None)
                .await
                .expect("enqueue append through simulated leader");
            assert!((1..=3).contains(&leader_id));
        })
        .await;
    });
}

#[cfg(madsim)]
#[test]
#[ignore = "diagnostic probe; full append/read strict replay is the default smoke test"]
fn madsim_three_node_openraft_group_strict_replay_append_commit_probe() {
    check_madsim_determinism(7, madsim::Config::default(), || async {
        crate::sim_runtime::MadsimOpenRaftRuntime::scope(7, async {
            let (_, engines, leader_id) = build_madsim_three_node_raft_cluster(7).await;
            let leader_index = usize::try_from(leader_id - 1).expect("leader id fits usize");
            let stream_id = bsid("madsim-strict-replay-commit");
            create_stream_via_raft(&engines[leader_index], stream_id.clone()).await;
            let (responder, commit_rx, _complete_rx) = openraft::impls::ProgressResponder::<
                UrsulaRaftTypeConfig,
                openraft::raft::ClientWriteResult<UrsulaRaftTypeConfig>,
            >::new();
            engines[leader_index]
                .raft
                .client_write_ff(append_command(stream_id, b"simulated"), Some(responder))
                .await
                .expect("enqueue append through simulated leader");
            let committed = commit_rx.await.expect("append commit notification");
            assert!(committed.index() > 0);
            assert!((1..=3).contains(&leader_id));
        })
        .await;
    });
}

#[cfg(madsim)]
#[test]
#[ignore = "diagnostic probe; full append/read strict replay is the default smoke test"]
fn madsim_three_node_openraft_group_strict_replay_append_complete_probe() {
    check_madsim_determinism(7, madsim::Config::default(), || async {
        crate::sim_runtime::MadsimOpenRaftRuntime::scope(7, async {
            let (_, engines, leader_id) = build_madsim_three_node_raft_cluster(7).await;
            let leader_index = usize::try_from(leader_id - 1).expect("leader id fits usize");
            let stream_id = bsid("madsim-strict-replay-complete");
            create_stream_via_raft(&engines[leader_index], stream_id.clone()).await;
            let (responder, commit_rx, complete_rx) = openraft::impls::ProgressResponder::<
                UrsulaRaftTypeConfig,
                openraft::raft::ClientWriteResult<UrsulaRaftTypeConfig>,
            >::new();
            engines[leader_index]
                .raft
                .client_write_ff(append_command(stream_id, b"simulated"), Some(responder))
                .await
                .expect("enqueue append through simulated leader");
            let committed = commit_rx.await.expect("append commit notification");
            assert!(committed.index() > 0);
            let completed = complete_rx.await.expect("append apply completion");
            let completed = completed.expect("append completed successfully");
            assert_eq!(completed.log_id, committed);
            assert!((1..=3).contains(&leader_id));
        })
        .await;
    });
}

#[cfg(madsim)]
#[test]
#[ignore = "diagnostic probe; full append/read strict replay is the default smoke test"]
fn madsim_three_node_openraft_group_strict_replay_append_response_probe() {
    check_madsim_determinism(7, madsim::Config::default(), || async {
        crate::sim_runtime::MadsimOpenRaftRuntime::scope(7, async {
            let (_, engines, leader_id) = build_madsim_three_node_raft_cluster(7).await;
            let leader_index = usize::try_from(leader_id - 1).expect("leader id fits usize");
            let stream_id = bsid("madsim-strict-replay-response");
            create_stream_via_raft(&engines[leader_index], stream_id.clone()).await;
            let appended = engines[leader_index]
                .raft
                .client_write(append_command(stream_id, b"simulated"))
                .await
                .expect("append through simulated leader");
            match write_result_from_raft_response(appended.data).expect("decode append response") {
                Ok(GroupWriteResponse::Append(response)) => {
                    assert_eq!(response.start_offset, 0);
                    assert_eq!(response.next_offset, 9);
                }
                other => panic!("unexpected append response: {other:?}"),
            }
            assert!(appended.log_id.index() > 0);
            assert!((1..=3).contains(&leader_id));
        })
        .await;
    });
}

#[cfg(madsim)]
#[test]
#[ignore = "diagnostic probe; full append/read strict replay is the default smoke test"]
fn madsim_three_node_openraft_group_strict_replay_append_leader_read_probe() {
    check_madsim_determinism(7, madsim::Config::default(), || async {
        crate::sim_runtime::MadsimOpenRaftRuntime::scope(7, async {
            let (_, engines, leader_id) = build_madsim_three_node_raft_cluster(7).await;
            let leader_index = usize::try_from(leader_id - 1).expect("leader id fits usize");
            let stream_id = bsid("madsim-strict-replay-leader-read");
            create_stream_via_raft(&engines[leader_index], stream_id.clone()).await;
            let appended = engines[leader_index]
                .raft
                .client_write(append_command(stream_id.clone(), b"simulated"))
                .await
                .expect("append through simulated leader");
            assert!(appended.log_id.index() > 0);
            let read = read_stream_via_state_machine(&engines[leader_index], stream_id, 16)
                .await
                .expect("read simulated leader state machine")
                .expect("simulated append is readable on leader");
            assert_eq!(read.payload, b"simulated");
            assert!((1..=3).contains(&leader_id));
        })
        .await;
    });
}

#[cfg(madsim)]
#[test]
#[ignore = "diagnostic probe; full append/read strict replay is the default smoke test"]
fn madsim_three_node_openraft_group_strict_replay_follower_log_probe() {
    check_madsim_determinism(7, madsim::Config::default(), || async {
        crate::sim_runtime::MadsimOpenRaftRuntime::scope(7, async {
            let (_, engines, leader_id) = build_madsim_three_node_raft_cluster(7).await;
            let leader_index = usize::try_from(leader_id - 1).expect("leader id fits usize");
            let (_, appended_log_index) =
                append_madsim_stream(&engines[leader_index], "madsim-strict-replay-follower-log")
                    .await;

            for (idx, engine) in engines.iter().enumerate() {
                if idx == leader_index {
                    continue;
                }
                engine
                    .raft
                    .wait(Some(Duration::from_secs(5)))
                    .log_index_at_least(Some(appended_log_index), "append reached follower log")
                    .await
                    .expect("wait for simulated follower log replication");
            }
            assert!((1..=3).contains(&leader_id));
        })
        .await;
    });
}

#[cfg(madsim)]
#[test]
#[ignore = "diagnostic probe; full append/read strict replay is the default smoke test"]
fn madsim_three_node_openraft_group_strict_replay_follower_apply_probe() {
    check_madsim_determinism(7, madsim::Config::default(), || async {
        crate::sim_runtime::MadsimOpenRaftRuntime::scope(7, async {
            let (_, engines, leader_id) = build_madsim_three_node_raft_cluster(7).await;
            let leader_index = usize::try_from(leader_id - 1).expect("leader id fits usize");
            let (_, appended_log_index) = append_madsim_stream(
                &engines[leader_index],
                "madsim-strict-replay-follower-apply",
            )
            .await;

            for (idx, engine) in engines.iter().enumerate() {
                if idx == leader_index {
                    continue;
                }
                engine
                    .raft
                    .wait(Some(Duration::from_secs(5)))
                    .applied_index_at_least(Some(appended_log_index), "append applied on follower")
                    .await
                    .expect("wait for simulated follower apply");
            }
            assert!((1..=3).contains(&leader_id));
        })
        .await;
    });
}

#[cfg(madsim)]
#[test]
#[ignore = "diagnostic probe; full append/read strict replay is the default smoke test"]
fn madsim_three_node_openraft_group_strict_replay_follower_read_probe() {
    check_madsim_determinism(7, madsim::Config::default(), || async {
        crate::sim_runtime::MadsimOpenRaftRuntime::scope(7, async {
            let (_, engines, leader_id) = build_madsim_three_node_raft_cluster(7).await;
            let leader_index = usize::try_from(leader_id - 1).expect("leader id fits usize");
            let (stream_id, appended_log_index) =
                append_madsim_stream(&engines[leader_index], "madsim-strict-replay-follower-read")
                    .await;
            let follower_index = (0..engines.len())
                .find(|idx| *idx != leader_index)
                .expect("one follower exists");

            engines[follower_index]
                .raft
                .wait(Some(Duration::from_secs(5)))
                .applied_index_at_least(Some(appended_log_index), "append applied on follower")
                .await
                .expect("wait for simulated follower apply");
            let read = read_stream_via_state_machine(&engines[follower_index], stream_id, 16)
                .await
                .expect("read simulated follower state machine")
                .expect("simulated append is readable on follower");
            assert_eq!(read.payload, b"simulated");
            assert!((1..=3).contains(&leader_id));
        })
        .await;
    });
}

#[cfg(madsim)]
#[test]
fn madsim_three_node_openraft_group_strict_replay_append_probe() {
    check_madsim_determinism(7, madsim::Config::default(), || async {
        crate::sim_runtime::MadsimOpenRaftRuntime::scope(7, async {
            let (_, engines, leader_id) = build_madsim_three_node_raft_cluster(7).await;
            let leader_index = usize::try_from(leader_id - 1).expect("leader id fits usize");
            let (stream_id, appended_log_index) =
                append_madsim_stream(&engines[leader_index], "madsim-strict-replay-append-probe")
                    .await;
            for engine in &engines {
                engine
                    .raft
                    .wait(Some(Duration::from_secs(5)))
                    .applied_index_at_least(Some(appended_log_index), "append replicated")
                    .await
                    .expect("wait for simulated append replication");

                let stream_id = stream_id.clone();
                let read = read_stream_via_state_machine(engine, stream_id, 16)
                    .await
                    .expect("read simulated follower state machine")
                    .expect("simulated replicated stream is readable");
                assert_eq!(read.payload, b"simulated");
            }
            assert!((1..=3).contains(&leader_id));
        })
        .await;
    });
}

#[cfg(madsim)]
async fn append_madsim_stream(
    engine: &RaftGroupEngine,
    name: &str,
) -> (ursula_shard::BucketStreamId, u64) {
    let stream_id = bsid(name);
    create_stream_via_raft(engine, stream_id.clone()).await;
    let appended = engine
        .raft
        .client_write(append_command(stream_id.clone(), b"simulated"))
        .await
        .expect("append through simulated leader");
    match write_result_from_raft_response(appended.data).expect("decode append response") {
        Ok(GroupWriteResponse::Append(response)) => {
            assert_eq!(response.start_offset, 0);
            assert_eq!(response.next_offset, 9);
        }
        other => panic!("unexpected append response: {other:?}"),
    }
    (stream_id, appended.log_id.index())
}

async fn create_stream_via_raft(engine: &RaftGroupEngine, stream_id: ursula_shard::BucketStreamId) {
    wait_history(engine).await;
    let created = engine
        .raft
        .client_write(create_command(stream_id))
        .await
        .expect("create stream through leader");
    assert!(matches!(
        write_result_from_raft_response(created.data).expect("decode create response"),
        Ok(GroupWriteResponse::CreateStream(_))
    ));
}

#[cfg(madsim)]
fn run_madsim_three_node_raft_once(seed: u64) -> (u64, u64) {
    run_madsim_three_node_raft_with_policy_once(seed, InProcessRaftNetworkPolicy::default())
}

#[cfg(madsim)]
fn run_madsim_three_node_raft_with_network_delay_once(seed: u64, delay: Duration) -> (u64, u64) {
    let policy = InProcessRaftNetworkPolicy::default();
    policy.set_delay(Some(delay));
    run_madsim_three_node_raft_with_policy_once(seed, policy)
}

#[cfg(madsim)]
fn run_madsim_three_node_raft_with_policy_once(
    seed: u64,
    policy: InProcessRaftNetworkPolicy,
) -> (u64, u64) {
    crate::sim_runtime::run_seeded(seed, Some(Duration::from_secs(10)), move || {
        crate::sim_runtime::MadsimOpenRaftRuntime::scope(seed, async move {
            let (_, engines, leader_id) =
                build_madsim_three_node_raft_cluster_with_policy(seed, policy).await;
            let leader_index = usize::try_from(leader_id - 1).expect("leader id fits usize");

            let stream_id = bsid("madsim-three-node-raft");
            create_stream_via_raft(&engines[leader_index], stream_id.clone()).await;

            let appended = engines[leader_index]
                .raft
                .client_write(append_command(stream_id.clone(), b"simulated"))
                .await
                .expect("append through simulated leader");
            let appended_log_index = appended.log_id.index();
            match write_result_from_raft_response(appended.data).expect("decode append response") {
                Ok(GroupWriteResponse::Append(response)) => {
                    assert_eq!(response.start_offset, 0);
                    assert_eq!(response.next_offset, 9);
                }
                other => panic!("unexpected append response: {other:?}"),
            }

            for engine in &engines {
                engine
                    .raft
                    .wait(Some(Duration::from_secs(5)))
                    .applied_index_at_least(Some(appended_log_index), "append replicated")
                    .await
                    .expect("wait for simulated append replication");

                let stream_id = stream_id.clone();
                let read = read_stream_via_state_machine(engine, stream_id, 16)
                    .await
                    .expect("read simulated follower state machine")
                    .expect("simulated replicated stream is readable");
                assert_eq!(read.payload, b"simulated");
            }

            let result = (leader_id, appended_log_index);
            shutdown_all(&engines).await;
            result
        })
    })
}

#[cfg(madsim)]
fn run_madsim_three_node_raft_with_fault_script_once(seed: u64) -> (u64, u64, u64) {
    crate::sim_runtime::run_seeded(seed, Some(Duration::from_secs(10)), move || {
        crate::sim_runtime::MadsimOpenRaftRuntime::scope(seed, async move {
            let policy = InProcessRaftNetworkPolicy::default();
            let (_, engines, leader_id) =
                build_madsim_three_node_raft_cluster_with_policy(seed, policy.clone()).await;
            let leader_index = usize::try_from(leader_id - 1).expect("leader id fits usize");
            let isolated_id = seeded_follower_id(seed, leader_id);
            let connected_id = (1..=3)
                .find(|node_id| *node_id != leader_id && *node_id != isolated_id)
                .expect("connected follower exists");
            let isolated_index = usize::try_from(isolated_id - 1).expect("node id fits usize");
            let connected_index = usize::try_from(connected_id - 1).expect("node id fits usize");
            let mut script = InProcessRaftFaultScript::new(seed);
            script.push(
                "before_append",
                InProcessRaftFaultAction::PartitionBidirectional {
                    a: leader_id,
                    b: isolated_id,
                },
            );
            script.push(
                "after_isolated_lag",
                InProcessRaftFaultAction::HealBidirectional {
                    a: leader_id,
                    b: isolated_id,
                },
            );

            script.apply_phase("before_append", &policy);
            let (stream_id, appended_log_index) =
                append_madsim_stream(&engines[leader_index], "madsim-fault-script-partition-heal")
                    .await;
            for index in [leader_index, connected_index] {
                engines[index]
                    .raft
                    .wait(Some(Duration::from_secs(5)))
                    .applied_index_at_least(Some(appended_log_index), "append applied on majority")
                    .await
                    .expect("wait for majority apply");
            }
            let isolated_wait = engines[isolated_index]
                .raft
                .wait(Some(Duration::from_millis(50)))
                .applied_index_at_least(Some(appended_log_index), "isolated follower should lag")
                .await;
            assert!(
                isolated_wait.is_err(),
                "isolated follower should not apply before heal"
            );

            script.apply_phase("after_isolated_lag", &policy);
            engines[isolated_index]
                .raft
                .wait(Some(Duration::from_secs(5)))
                .applied_index_at_least(Some(appended_log_index), "healed follower catches up")
                .await
                .expect("wait for healed follower apply");
            let read = read_stream_via_state_machine(&engines[isolated_index], stream_id, 16)
                .await
                .expect("read healed follower state machine")
                .expect("simulated append is readable on healed follower");
            assert_eq!(read.payload, b"simulated");
            assert_eq!(script.seed(), seed);
            (leader_id, isolated_id, appended_log_index)
        })
    })
}

#[cfg(madsim)]
fn seeded_follower_id(seed: u64, leader_id: u64) -> u64 {
    let mut followers: Vec<u64> = (1..=3).filter(|node_id| *node_id != leader_id).collect();
    followers.sort_unstable();
    let mixed = seed ^ seed.rotate_left(17) ^ 0x9e37_79b9_7f4a_7c15;
    followers[usize::try_from(mixed % followers.len() as u64).expect("index fits usize")]
}

#[cfg(madsim)]
async fn build_madsim_three_node_raft_cluster(
    seed: u64,
) -> (InProcessRaftRegistry, Vec<RaftGroupEngine>, u64) {
    build_madsim_three_node_raft_cluster_with_policy(seed, InProcessRaftNetworkPolicy::default())
        .await
}

#[cfg(madsim)]
async fn build_madsim_three_node_raft_cluster_with_policy(
    seed: u64,
    policy: InProcessRaftNetworkPolicy,
) -> (InProcessRaftRegistry, Vec<RaftGroupEngine>, u64) {
    let registry = InProcessRaftRegistry::default();
    let config = raft_config("ursula-madsim-three-node-test", 50, 100);
    let mut nodes = BTreeMap::new();
    for node_id in 1..=3 {
        nodes.insert(node_id, BasicNode::new(format!("node-{node_id}")));
    }

    let mut engines = Vec::new();
    for node_id in 1..=3 {
        let node = madsim::runtime::Handle::current()
            .create_node()
            .name(format!("raft-node-{node_id}"))
            .build();
        let (tx, rx) = sim_tokio::sync::oneshot::channel();
        let registry_for_node = registry.clone();
        let config_for_node = config.clone();
        let policy_for_node = policy.clone();
        let node_seed = seed.wrapping_add(node_id);
        node.spawn(crate::sim_runtime::MadsimOpenRaftRuntime::scope(
            node_seed,
            async move {
                let engine = RaftGroupEngine::new_node_with_log_store_and_network(
                    placement(),
                    node_id,
                    config_for_node,
                    InProcessRaftNetworkFactory::new(registry_for_node)
                        .with_source(node_id)
                        .with_policy(policy_for_node),
                    sim_journal_store(&format!("raft-node-{node_id}")),
                    None,
                    None,
                )
                .await
                .expect("create simulated raft group node");
                assert!(tx.send(engine).is_ok(), "send simulated raft group node");
            },
        ));
        let engine = rx.await.expect("receive simulated raft group node");
        registry.register(node_id, &engine);
        engines.push(engine);
    }

    engines[0]
        .raft
        .initialize(nodes)
        .await
        .expect("initialize simulated raft group");
    let leader_metrics = engines[0]
        .raft
        .wait(Some(Duration::from_secs(5)))
        .metrics(|metrics| metrics.current_leader.is_some(), "leader elected")
        .await
        .expect("wait for simulated leader");
    let leader_id = leader_metrics.current_leader.expect("leader id");
    (registry, engines, leader_id)
}

#[cfg(not(madsim))]
#[tokio::test]
async fn openraft_installs_snapshot_for_lagging_learner() {
    let registry = InProcessRaftRegistry::default();
    let config = Arc::new(
        Config {
            cluster_name: "ursula-lagging-learner-snapshot-test".to_owned(),
            heartbeat_interval: 10,
            // Vote RPCs include a metadata fsync, also under parallel test load.
            election_timeout_min: 500,
            election_timeout_max: 1_000,
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
    let mut wal_roots = Vec::new();
    for node_id in 1..=3 {
        let (wal_root, log_store) = fresh_journal_store();
        wal_roots.push(wal_root);
        let engine = RaftGroupEngine::new_node_with_log_store_and_network(
            placement(),
            node_id,
            config.clone(),
            InProcessRaftNetworkFactory::new(registry.clone()).with_source(node_id),
            log_store,
            None,
            None,
        )
        .await
        .expect("create cluster raft group node");
        if node_id != 3 {
            registry.register(node_id, &engine);
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
    wait_history(&engines[leader_index]).await;
    let stream_id = bsid("lagging-learner-snapshot");
    let _created = engines[leader_index]
        .raft
        .client_write(create_command(stream_id.clone()))
        .await
        .expect("create stream through elected leader");
    let appended = engines[leader_index]
        .raft
        .client_write(append_command(stream_id.clone(), b"snapshot-transfer"))
        .await
        .expect("append through elected leader");
    let appended_log_id = appended.log_id;
    let appended_log_index = appended_log_id.index();
    assert!(matches!(
        write_result_from_raft_response(appended.data).expect("decode append response"),
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

    registry.register(3, &engines[2]);
    let learner_added = tokio::time::timeout(
        Duration::from_secs(5),
        engines[leader_index]
            .raft
            .add_learner(3, BasicNode::new("node-3"), true),
    )
    .await
    .expect("lagging learner catch-up finishes within the existing apply budget")
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

    let read = read_stream_via_state_machine(&engines[2], stream_id.clone(), 64)
        .await
        .expect("read lagging learner state machine")
        .expect("stream restored from snapshot is readable");
    assert_eq!(read.payload, b"snapshot-transfer");
    // M3 (#581 review): the snapshot covers the group's history event, and
    // installing it publishes that history on the learner, as an apply does,
    // so the learner's group actor wakes its watchers.
    assert_eq!(
        engines[2].history().applied(),
        engines[leader_index].history().applied(),
        "the installed snapshot carries the group's history"
    );
    assert!(engines[2].history().applied().is_some());

    shutdown_all(&engines).await;
}

#[cfg(not(madsim))]
#[tokio::test]
async fn raft_group_engine_implements_runtime_group_engine_over_openraft() {
    let (_wal_root, log_store) = fresh_journal_store();
    let mut engine = RaftGroupEngine::new_single_node_on_log_store(placement(), log_store, None)
        .await
        .expect("create raft group engine");
    let stream_id = bsid("raft-group-engine");

    let created = engine
        .create_stream(
            CreateStreamRequest::new(stream_id.clone(), "application/octet-stream"),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("create through group engine");
    assert_eq!(created.next_offset, 0);
    assert!(!created.already_exists);

    let appended = engine
        .append(
            AppendRequest::from_bytes(stream_id.clone(), b"payload".to_vec()),
            placement(),
            ColdWriteAdmission::default(),
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
                linearizable: true,
                read_index: None,
            },
            placement(),
        )
        .await
        .expect("head through group engine");
    assert_eq!(head.tail_offset, 7);

    let read = engine
        .read_stream(read_req(stream_id, 16), placement())
        .await
        .expect("read through group engine");
    assert_eq!(read.payload, b"payload");

    let snapshot = engine.snapshot(placement()).await.expect("snapshot");
    // The genesis history event, the create and the append.
    assert_eq!(snapshot.group_commit_index, 3);
    engine.shutdown().await.expect("shutdown raft group engine");
}

#[cfg(not(madsim))]
#[tokio::test]
async fn raft_group_engine_preserves_stream_error_next_offset() {
    let (_wal_root, log_store) = fresh_journal_store();
    let mut engine = RaftGroupEngine::new_single_node_on_log_store(placement(), log_store, None)
        .await
        .expect("create raft group engine");
    let stream_id = bsid("raft-stream-error-offset");

    engine
        .create_stream(
            CreateStreamRequest::new(stream_id.clone(), "application/octet-stream"),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("create through group engine");
    engine
        .append(
            AppendRequest::from_bytes(stream_id.clone(), b"payload".to_vec()),
            placement(),
            ColdWriteAdmission::default(),
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
                if_incarnation: None,
            },
            placement(),
        )
        .await
        .expect("close through group engine");

    let err = engine
        .append(
            AppendRequest::from_bytes(stream_id, b"after-close".to_vec()),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect_err("append to closed stream should fail");
    assert_eq!(err.next_offset(), Some(7));

    engine.shutdown().await.expect("shutdown raft group engine");
}

#[cfg(not(madsim))]
#[tokio::test]
async fn raft_group_engine_recovers_client_writes_from_file_log() {
    let root = tempfile::tempdir().expect("journal root");
    let stream_id = bsid("raft-engine-recover");
    let single_node_config = || raft_config("ursula-group-0", 30, 60);

    {
        let mut engine = RaftGroupEngine::new_single_node_with_log_store(
            placement(),
            1,
            BasicNode::new("local"),
            single_node_config(),
            open_core_journal_store(root.path()).expect("open durable log store"),
        )
        .await
        .expect("create durable raft group engine");
        engine
            .create_stream(
                CreateStreamRequest::new(stream_id.clone(), "application/octet-stream"),
                placement(),
                ColdWriteAdmission::default(),
            )
            .await
            .expect("create through durable raft group engine");
        engine
            .append(
                AppendRequest::from_bytes(stream_id.clone(), b"payload".to_vec()),
                placement(),
                ColdWriteAdmission::default(),
            )
            .await
            .expect("append through durable raft group engine");
        engine.shutdown().await.expect("shutdown first engine");
    }

    let mut recovered = RaftGroupEngine::new_single_node_with_log_store(
        placement(),
        1,
        BasicNode::new("local"),
        single_node_config(),
        open_core_journal_store(root.path()).expect("reopen durable log store"),
    )
    .await
    .expect("reopen durable raft group engine");
    let read = recovered
        .read_stream(read_req(stream_id, 16), placement())
        .await
        .expect("read recovered payload");
    assert_eq!(read.payload, b"payload");
    recovered
        .shutdown()
        .await
        .expect("shutdown recovered engine");
}

/// Waits until `engine`'s replica applied a history event.
async fn wait_history(engine: &RaftGroupEngine) -> ursula_runtime::HistoryEvent {
    let started = crate::rt::time::Instant::now();
    loop {
        if let Some(event) = engine.history().applied() {
            return event;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the group's genesis never applied"
        );
        crate::rt::time::sleep(Duration::from_millis(5)).await;
    }
}

/// W2 (#581 section 4.3): a new group's first leader proposes one genesis
/// history event, with a random id, before anything else applies; a
/// restart replays it and proposes no second one.
#[cfg(not(madsim))]
#[tokio::test]
async fn a_new_group_starts_with_one_genesis_history_event() {
    let root = tempfile::tempdir().expect("journal root");
    let single_node_config = || raft_config("ursula-group-0", 30, 60);
    let stream_id = bsid("genesis");
    let genesis = {
        let mut engine = RaftGroupEngine::new_single_node_with_log_store(
            placement(),
            1,
            BasicNode::new("local"),
            single_node_config(),
            open_core_journal_store(root.path()).expect("open durable log store"),
        )
        .await
        .expect("create durable raft group engine");
        // A create racing the genesis waits for it: its token is never
        // derived from a group without a history.
        engine
            .create_stream(
                CreateStreamRequest::new(stream_id.clone(), "application/octet-stream"),
                placement(),
                ColdWriteAdmission::default(),
            )
            .await
            .expect("create after the genesis");
        let genesis = wait_history(&engine).await;
        assert_eq!(genesis.imported, None);
        let snapshot = engine.snapshot(placement()).await.expect("snapshot");
        assert_eq!(snapshot.stream_snapshot.history, Some(genesis));
        let created_at_ms = snapshot.stream_snapshot.streams[0].metadata.created_at_ms;
        let head = engine
            .head_stream(
                HeadStreamRequest {
                    stream_id: stream_id.clone(),
                    now_ms: 0,
                    linearizable: true,
                    read_index: None,
                },
                placement(),
            )
            .await
            .expect("head");
        assert_eq!(
            head.incarnation,
            ursula_stream::incarnation_token(created_at_ms, genesis.id.get())
        );
        assert_eq!(head.inherited, None);
        engine.shutdown().await.expect("shutdown first engine");
        genesis
    };
    let recovered = RaftGroupEngine::new_single_node_with_log_store(
        placement(),
        1,
        BasicNode::new("local"),
        single_node_config(),
        open_core_journal_store(root.path()).expect("reopen durable log store"),
    )
    .await
    .expect("reopen durable raft group engine");
    // The leader applied its blank: an earlier genesis is in its state, so
    // it proposes none.
    recovered
        .raft_handle()
        .wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| {
                metrics
                    .last_applied
                    .is_some_and(|applied| applied.leader_id.term == metrics.current_term)
            },
            "the restarted leader applied its blank entry",
        )
        .await
        .expect("blank applied");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(recovered.history().applied(), Some(genesis));
    recovered
        .shutdown()
        .await
        .expect("shutdown recovered engine");
}

/// A leader whose recovery plan is not satisfied serves nothing (#581
/// section 4.5): a write and a read wait for the history event and are
/// refused with `RecoveryPending` (503) when it does not come, and nothing
/// was proposed.
#[cfg(not(madsim))]
#[tokio::test]
async fn a_leader_with_an_unsatisfied_plan_serves_nothing() {
    let (_root, log_store) = fresh_journal_store();
    let mut engine = RaftGroupEngine::new_single_node_on_log_store(placement(), log_store, None)
        .await
        .expect("single-node engine");
    wait_history(&engine).await;
    let stream_id = bsid("fenced");
    engine
        .create_stream(
            CreateStreamRequest::new(stream_id.clone(), "application/octet-stream"),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("create before the plan");
    // A plan whose point this leader's history event cannot pass.
    engine
        .history()
        .set_plan(Some(crate::log_store::RecoveryRecord {
            leader: 1,
            point: u64::MAX,
            leader_term: 0,
            cut: crate::log_store::PlanCut::NoCut,
        }));
    let before = engine.snapshot(placement()).await;
    assert!(
        matches!(&before, Err(error) if error.is_history_pending()),
        "{before:?}"
    );
    let write = engine
        .append(
            AppendRequest::from_bytes(stream_id.clone(), b"fenced".to_vec()),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect_err("a fenced leader admits no write");
    assert!(write.is_history_pending(), "{write:?}");
    assert!(write.is_refused_before_proposal());
    let read = engine
        .read_stream(read_req(stream_id.clone(), 16), placement())
        .await
        .expect_err("a fenced leader serves no read");
    assert!(read.is_history_pending(), "{read:?}");
    // The plan is satisfied: the fence lifts, and the refused write never
    // applied.
    engine.history().set_plan(None);
    let read = engine
        .read_stream(read_req(stream_id, 16), placement())
        .await
        .expect("read after the plan");
    assert!(read.payload.is_empty());
    engine.shutdown().await.expect("shutdown");
}

/// The leader's fence (#581 section 4.5): while its group's history is
/// pending it admits nothing but the history event and serves no read, and
/// it refuses once the wait runs out; a follower is never fenced.
#[test]
fn a_pending_history_fences_only_its_leader() {
    use crate::history::history_pending;
    use crate::history::plan_satisfied;
    use crate::log_store::RecoveryRecord;

    let event = |event_index| ursula_runtime::HistoryEvent {
        id: std::num::NonZeroU64::new(7).expect("non-zero"),
        event_index,
        previous: 1,
        kept: 10,
        leader: ursula_stream::HistoryLeader {
            term: 0,
            node_id: 0,
        },
        imported: None,
    };
    let plan = RecoveryRecord {
        leader: 1,
        point: 10,
        leader_term: 0,
        cut: crate::log_store::PlanCut::NoCut,
    };
    // Genesis: no history yet.
    assert!(history_pending(None, None));
    assert!(!history_pending(Some(event(1)), None));
    // A plan is satisfied only by an event above its point: an old event at
    // or below the point never satisfies it (#577 B2).
    assert!(history_pending(Some(event(10)), Some(&plan)));
    assert!(!plan_satisfied(&plan, Some(event(10))));
    assert!(plan_satisfied(&plan, Some(event(11))));
    // M1 (#581 review): a satisfied plan keeps the history pending until its
    // record is removed from disk.
    assert!(history_pending(Some(event(11)), Some(&plan)));
}

/// A recovery plan's cut restarts one group's engine in the process (#581
/// section 4.5): its log store closes and opens again with what it held.
#[cfg(not(madsim))]
#[tokio::test]
async fn shard_runtime_restarts_one_group_engine_from_its_log_store() {
    let wal_root = tempfile::tempdir().expect("WAL root");
    let runtime = ShardRuntime::spawn_with_engine_factory(
        hosted_config(1, 1),
        DurableRaftGroupEngineFactory::new(start_wal(wal_root.path(), 1, 1)),
    )
    .expect("spawn runtime with raft group engine factory");
    let stream_id = bsid("restarted-group");
    runtime
        .create_stream(create_req(stream_id.clone()))
        .await
        .expect("create");
    runtime
        .append(AppendRequest::from_bytes(
            stream_id.clone(),
            b"kept".to_vec(),
        ))
        .await
        .expect("append");
    let raft_group_id = runtime.locate(&stream_id).raft_group_id;
    runtime
        .restart_group_engine(raft_group_id)
        .await
        .expect("restart the group's engine");
    let read = runtime
        .read_stream(read_req(stream_id.clone(), 16))
        .await
        .expect("read after the restart");
    assert_eq!(read.payload, b"kept");
    runtime
        .append(AppendRequest::from_bytes(
            stream_id.clone(),
            b"+more".to_vec(),
        ))
        .await
        .expect("append after the restart");
    runtime
        .restart_group_engine(raft_group_id)
        .await
        .expect("restart again");
    let read = runtime
        .read_stream(read_req(stream_id, 16))
        .await
        .expect("read after the second restart");
    assert_eq!(read.payload, b"kept+more");
}

#[cfg(not(madsim))]
#[tokio::test]
async fn shard_runtime_uses_raft_group_engine_factory_for_owned_group() {
    let wal_root = tempfile::tempdir().expect("WAL root");
    let config = hosted_config(1, 1);
    let runtime = ShardRuntime::spawn_with_engine_factory(
        config,
        DurableRaftGroupEngineFactory::new(start_wal(wal_root.path(), 1, 1)),
    )
    .expect("spawn runtime with raft group engine factory");
    let stream_id = bsid("runtime-raft-engine");

    runtime
        .create_stream(create_req(stream_id.clone()))
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
        .read_stream(read_req(stream_id, 16))
        .await
        .expect("read through runtime-owned raft group");
    assert_eq!(read.payload, b"payload");
}

#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn warm_group_registers_runtime_owned_raft_handle() {
    let wal_root = tempfile::tempdir().expect("WAL root");
    let registry = RaftGroupHandleRegistry::default();
    let config = hosted_config(2, 4);
    let runtime = ShardRuntime::spawn_with_engine_factory(
        config,
        DurableRaftGroupEngineFactory::new(start_wal(wal_root.path(), 2, 4))
            .with_registry(registry.clone()),
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

/// M3 (#581 review): a history event ends a live read pinned to the old
/// token, which re-reads and finds the stream under its new token (#581
/// section 4.7); long-poll answers `412` from that and SSE ends.
#[cfg(not(madsim))]
#[tokio::test]
async fn a_history_event_ends_a_live_read_pinned_to_the_old_token() {
    let wal_root = tempfile::tempdir().expect("WAL root");
    let registry = RaftGroupHandleRegistry::default();
    let runtime = ShardRuntime::spawn_with_engine_factory(
        hosted_config(1, 1),
        DurableRaftGroupEngineFactory::new(start_wal(wal_root.path(), 1, 1))
            .with_registry(registry.clone()),
    )
    .expect("spawn runtime");
    let stream_id = bsid("pinned-live-read");
    runtime
        .create_stream(create_req(stream_id.clone()))
        .await
        .expect("create");
    runtime
        .append(AppendRequest::from_bytes(
            stream_id.clone(),
            b"abc".to_vec(),
        ))
        .await
        .expect("append");
    let before = runtime
        .read_stream(read_req(stream_id.clone(), 16))
        .await
        .expect("read");
    let waiter = {
        let runtime = runtime.clone();
        let request = ReadStreamRequest {
            stream_id: stream_id.clone(),
            offset: 3,
            max_len: 16,
            now_ms: 0,
            leader_only: false,
            read_index: None,
            follower: FollowerRead::Prefix,
        };
        let pinned = before.incarnation;
        tokio::spawn(async move { runtime.wait_read_stream_pinned(request, Some(pinned)).await })
    };
    let started = std::time::Instant::now();
    while runtime.metrics().snapshot().live_read_waiters == 0 {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the read never parked"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // A plan whose leader is this replica: its history driver proposes the
    // group's next history event.
    let raft_group_id = runtime.locate(&stream_id).raft_group_id;
    let entry = registry.entry(raft_group_id).expect("registered group");
    let point = openraft::rt::WatchReceiver::borrow_watched(&entry.raft().metrics())
        .last_log_index
        .expect("a log");
    entry.history.set_plan(Some(RecoveryRecord {
        leader: 1,
        point,
        leader_term: 0,
        cut: crate::log_store::PlanCut::NoCut,
    }));
    let woken = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("the history event ends the wait")
        .expect("waiter task")
        .expect("the re-read");
    assert_ne!(woken.incarnation, before.incarnation, "a new token");
    assert!(woken.payload.is_empty());
}

#[cfg(not(madsim))]
#[tokio::test]
async fn durable_raft_group_engine_records_file_log_metrics() {
    let root = temp_log_path("raft-file-log-metrics-root").with_extension("");
    remove_test_path(&root);

    let config = hosted_config(1, 1);
    let runtime = ShardRuntime::spawn_with_engine_factory(
        config,
        DurableRaftGroupEngineFactory::new(start_wal(&root, 1, 1)),
    )
    .expect("spawn runtime with durable raft group engine factory");
    let placement = placement();
    let stream_id = bsid("runtime-raft-file-metrics");

    runtime
        .create_stream(create_req(stream_id.clone()))
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
    assert!(metrics.wal_fsyncs > 0);
    // Votes and the `initialized` flag replace the metadata file, which
    // `fsync`s the file and its directory, so a batch may cost two.
    assert!(metrics.wal_fsyncs <= metrics.wal_batches.saturating_mul(3));
    assert!(metrics.wal_fsync_records > 0);
    assert!(metrics.wal_physical_bytes > 0);

    drop(runtime);
    remove_test_path(&root);
}

#[cfg(not(madsim))]
#[tokio::test]
async fn durable_raft_group_engine_recovers_from_core_journal() {
    let root = temp_log_path("raft-core-journal-recover-root").with_extension("");
    remove_test_path(&root);
    let stream_id = bsid("raft-core-journal-recover");

    {
        let config = hosted_config(1, 1);
        let log_stores = start_wal(&root, 1, 1);
        let runtime = ShardRuntime::spawn_with_engine_factory(
            config,
            DurableRaftGroupEngineFactory::new(log_stores.clone()),
        )
        .expect("spawn durable runtime");
        runtime
            .create_stream(create_req(stream_id.clone()))
            .await
            .expect("create stream");
        runtime
            .append(AppendRequest::from_bytes(
                stream_id.clone(),
                b"journal-payload".to_vec(),
            ))
            .await
            .expect("append stream");
        runtime
            .shutdown_group_engine(placement())
            .await
            .expect("shutdown durable group before in-process restart");
        log_stores
            .shutdown()
            .await
            .expect("shut down the Raft WAL cleanly");
    }

    let segments =
        crate::log_store::journal_segments(&root.join("core-0")).expect("journal segments");
    assert!(!segments.is_empty(), "core journal should exist");
    assert!(
        segments
            .iter()
            .any(|(_, segment)| fs::metadata(segment).expect("segment metadata").len() > 32),
        "core journal should contain records"
    );

    {
        let config = hosted_config(1, 1);
        let log_stores = start_wal(&root, 1, 1);
        assert_eq!(log_stores.opening().previous_run, PreviousRun::Clean);
        let recovered = ShardRuntime::spawn_with_engine_factory(
            config,
            DurableRaftGroupEngineFactory::new(log_stores),
        )
        .expect("spawn recovered durable runtime");
        let read = recovered
            .read_stream(read_req(stream_id, 32))
            .await
            .expect("read recovered stream");
        assert_eq!(read.payload, b"journal-payload");
    }

    remove_test_path(&root);
}

#[tokio::test]
async fn openraft_state_machine_applies_group_write_commands() {
    let stream_id = bsid("raft-apply");
    let mut sm = RaftGroupStateMachine::new(placement());
    let entries = vec![
        normal_entry(1, genesis_command()),
        normal_entry(2, create_command(stream_id.clone())),
        normal_entry(3, append_command(stream_id.clone(), b"abc")),
    ];

    sm.apply(stream::iter(
        entries.into_iter().map(|entry| Ok((entry, None))),
    ))
    .await
    .expect("apply raft entries");

    let snapshot = sm.group_snapshot().await.expect("snapshot");
    assert_eq!(snapshot.group_commit_index, 3);
    assert_eq!(snapshot.stream_append_counts.len(), 1);
    assert_eq!(snapshot.stream_append_counts[0].append_count, 1);
}

/// #669: an applied entry that removes streams records what they held in a
/// group snapshot, their hot bytes and the per-command overhead each, for a
/// deletion and a bucket purge alike. Other entries record nothing.
#[cfg(not(madsim))]
#[tokio::test]
async fn applied_removals_record_what_the_snapshot_held() {
    use crate::snapshot_cadence::RemovalProgress;

    let overhead = ursula_stream::COMMAND_LOG_OVERHEAD_BYTES;
    let kept = bsid("removal-kept");
    let deleted = bsid("removal-deleted");
    let purged = ursula_shard::BucketStreamId::new("removal-purged", "a");
    let empty = ursula_shard::BucketStreamId::new("removal-purged", "b");
    let mut sm = RaftGroupStateMachine::new(placement());
    apply_entries(&mut sm, vec![
        normal_entry(1, genesis_command()),
        normal_entry(2, create_command(kept.clone())),
        normal_entry(3, create_command(deleted.clone())),
        normal_entry(4, append_command(deleted.clone(), b"abcd")),
        normal_entry(5, create_command(purged.clone())),
        normal_entry(6, append_command(purged, b"xyz")),
        normal_entry(7, create_command(empty)),
    ])
    .await;
    assert_eq!(sm.log_progress().removal, RemovalProgress::Clear);

    let owed = |sm: &RaftGroupStateMachine| match sm.log_progress().removal {
        RemovalProgress::Owed { bytes, .. } => bytes,
        other => panic!("a removal is owed: {other:?}"),
    };
    apply_entries(&mut sm, vec![normal_entry(
        8,
        GroupWriteCommand::Stream(ursula_stream::StreamCommand::DeleteStream {
            stream_id: deleted,
        }),
    )])
    .await;
    let deleted_bytes = overhead.saturating_add(4);
    assert_eq!(owed(&sm), deleted_bytes);
    apply_entries(&mut sm, vec![normal_entry(
        9,
        GroupWriteCommand::Stream(ursula_stream::StreamCommand::PurgeBucket {
            bucket_id: "removal-purged".to_owned(),
        }),
    )])
    .await;
    let purged_bytes = deleted_bytes
        .saturating_add(overhead.saturating_add(3))
        .saturating_add(overhead);
    assert_eq!(owed(&sm), purged_bytes);
    apply_entries(&mut sm, vec![normal_entry(
        10,
        append_command(kept, b"more"),
    )])
    .await;
    assert_eq!(owed(&sm), purged_bytes);
}

/// Applies `entries` to `sm` in one batch.
#[cfg(not(madsim))]
async fn apply_entries(
    sm: &mut RaftGroupStateMachine,
    entries: Vec<<UrsulaRaftTypeConfig as openraft::RaftTypeConfig>::Entry>,
) {
    sm.apply(stream::iter(
        entries.into_iter().map(|entry| Ok((entry, None))),
    ))
    .await
    .expect("apply raft entries");
}

#[tokio::test]
async fn openraft_snapshot_round_trips_group_state() {
    let stream_id = bsid("raft-snapshot");
    let mut source = RaftGroupStateMachine::new(placement());
    let entries = vec![
        normal_entry(1, genesis_command()),
        normal_entry(2, create_command(stream_id.clone())),
        normal_entry(3, append_command(stream_id, b"payload")),
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
        .apply_committed_write(append_command(bsid("raft-snapshot"), b"-next"), placement())
        .expect("append after install");
    match appended {
        GroupWriteResponse::Append(response) => {
            assert_eq!(response.start_offset, 7);
            assert_eq!(response.stream_append_count, 2);
        }
        other => panic!("unexpected append response: {other:?}"),
    }
}

/// A forwarded read carries `leader_only` (field 6) to the leader over gRPC,
/// so the leader linearizes a forwarded `consistency=leader` read, and its
/// follower policy (field 7), so a receiver that no longer leads still
/// serves an immutable chunk only when it can mark it. So does
/// `HeadStreamReadV1.applied_state_only` (field 1), so the leader
/// linearizes a forwarded client HEAD and not an internal one.
#[test]
fn forwarded_reads_carry_linearizability_over_grpc() {
    use prost::Message;

    let request = ursula_runtime::ReadStreamRequest {
        stream_id: bsid("leader-forward"),
        offset: 0,
        max_len: 4096,
        now_ms: 77,
        leader_only: true,
        read_index: None,
        follower: FollowerRead::Prefix,
    };
    for request in [request.clone(), ursula_runtime::ReadStreamRequest {
        leader_only: false,
        follower: FollowerRead::Immutable,
        ..request.clone()
    }] {
        let wire = crate::forward::read_stream_read_v1(&request)
            .expect("wire read")
            .encode_to_vec();
        let decoded =
            crate::raft_internal_proto::ReadStreamReadV1::decode(wire.as_slice()).expect("decode");
        let served =
            crate::grpc::read_stream_request_from_v1(request.stream_id.clone(), 77, decoded)
                .expect("served request");
        assert_eq!(served, request);
    }
    let unknown = crate::raft_internal_proto::ReadStreamReadV1 {
        offset: 0,
        max_len: 1,
        leader_only: false,
        follower: 9,
    };
    assert_eq!(
        crate::grpc::read_stream_request_from_v1(request.stream_id.clone(), 77, unknown),
        Err("group_read.read_stream.follower unknown")
    );

    // A forwarded client HEAD stays linearizable on the leader; an internal
    // one does not.
    for linearizable in [true, false] {
        let head = HeadStreamRequest {
            stream_id: request.stream_id.clone(),
            now_ms: 77,
            linearizable,
            read_index: None,
        };
        let wire = crate::forward::head_stream_read_v1(&head).encode_to_vec();
        let decoded = crate::raft_internal_proto::HeadStreamReadV1::decode(wire.as_slice())
            .expect("decode head");
        let served = crate::grpc::head_stream_request_from_v1(head.stream_id.clone(), 77, decoded);
        assert_eq!(served, head);
    }
}

/// Remove a temporary test file or directory, tolerating its absence.
#[cfg(not(madsim))]
pub(crate) fn remove_test_path(path: impl AsRef<std::path::Path>) {
    let path = path.as_ref();
    let removed = if path.is_dir() {
        std::fs::remove_dir_all(path)
    } else {
        std::fs::remove_file(path)
    };
    if let Err(err) = removed
        && err.kind() != std::io::ErrorKind::NotFound
    {
        panic!("remove test path {}: {err}", path.display());
    }
}

#[cfg(not(madsim))]
#[tokio::test]
async fn registry_handoff_rejects_reverted_follower_and_transfers_to_healthy_voter() {
    use openraft::rt::WatchReceiver;
    let (_network, engines, leader, _roots) =
        build_three_node_cluster("handoff-policy", None).await;
    let engine = engines
        .iter()
        .find(|engine| engine.raft.metrics().borrow_watched().id == leader)
        .unwrap();
    let raft = engine.raft_handle();
    let target = (1..=3).find(|id| *id != leader).unwrap();
    let healthy = (1..=3).find(|id| *id != leader && *id != target).unwrap();
    raft.wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| {
                metrics
                    .replication
                    .as_ref()
                    .and_then(|progress| progress.get(&target))
                    .is_some_and(Option::is_some)
            },
            "follower acknowledged a log entry",
        )
        .await
        .unwrap();
    let registry = RaftGroupHandleRegistry::default();
    let (_root, store) = fresh_journal_store();
    let gate = Arc::new(
        GroupRejoin::durable(leader, placement().raft_group_id, &store)
            .await
            .expect("open the gate"),
    );
    engine.publish_recovery(gate.clone(), &registry).unwrap();
    let metrics = raft.metrics().borrow_watched().clone();
    let matched = metrics.replication.as_ref().unwrap()[&target];
    assert!(gate.follower_lost_log(
        target,
        &metrics.vote,
        matched.as_ref(),
        None,
        &UrsulaAppendEntriesResponse::Conflict
    ));
    assert!(
        matches!(registry.transfer_leader(placement().raft_group_id, target).await,
        Err(LeadershipTransferError::RecoveringTarget { target: rejected, .. }) if rejected == target)
    );
    assert!(matches!(
        registry
            .transfer_leader(placement().raft_group_id, leader)
            .await,
        Err(LeadershipTransferError::InvalidTarget { .. })
    ));
    assert!(matches!(
        registry
            .transfer_leader(placement().raft_group_id, 99)
            .await,
        Err(LeadershipTransferError::InvalidTarget { .. })
    ));
    // Rejection leaves the leader able to commit, and a healthy handoff still works.
    raft.client_write(create_command(ursula_shard::BucketStreamId::new(
        "default", "handoff",
    )))
    .await
    .unwrap();
    wait_matched_committed(&raft, healthy).await;
    registry
        .transfer_leader(placement().raft_group_id, healthy)
        .await
        .unwrap();
    raft.wait(Some(Duration::from_secs(5)))
        .current_leader(healthy, "healthy voter becomes leader")
        .await
        .unwrap();
    shutdown_all(&engines).await;
}

/// Waits until the leader's replication metrics show `target` holding every
/// entry the leader committed.
#[cfg(not(madsim))]
async fn wait_matched_committed(raft: &crate::RaftGroupHandle, target: u64) {
    raft.wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| {
                let matched = metrics
                    .replication
                    .as_ref()
                    .and_then(|replication| replication.get(&target))
                    .and_then(|matched| matched.as_ref())
                    .map(|log_id| log_id.index);
                matched >= metrics.local_committed.as_ref().map(|log_id| log_id.index)
            },
            "target matched the committed log",
        )
        .await
        .unwrap();
}

/// OpenRaft parks a leader whose transfer cannot complete: it forwards every
/// write to the target, stops heartbeats and never campaigns. The handoff
/// policy refuses a target that lacks committed entries or has not answered
/// recently, and the leader keeps leading.
#[cfg(not(madsim))]
#[tokio::test]
async fn registry_handoff_refuses_a_lagging_or_unreachable_target() {
    use openraft::rt::WatchReceiver;
    let policy = InProcessRaftNetworkPolicy::default();
    let (_network, engines, leader, _roots) =
        build_three_node_cluster("handoff-unready", Some(policy.clone())).await;
    let leader_engine = engines
        .iter()
        .find(|engine| engine.raft.metrics().borrow_watched().id == leader)
        .unwrap();
    let raft = leader_engine.raft_handle();
    let target = (1..=3).find(|id| *id != leader).unwrap();
    let write = |name: &'static str| {
        raft.client_write(create_command(ursula_shard::BucketStreamId::new(
            "default", name,
        )))
    };
    write("first").await.unwrap();
    wait_matched_committed(&raft, target).await;
    let registry = RaftGroupHandleRegistry::default();
    registry.register_engine(leader_engine);
    let group = placement().raft_group_id;

    // Cut off, the target misses an entry the other voter commits.
    policy.partition_bidirectional(leader, target);
    write("lagging").await.unwrap();
    assert!(matches!(
        registry.check_handoff(group, target),
        Err(LeadershipTransferError::LaggingTarget { target: refused, .. }) if refused == target
    ));
    // Silent for longer than the minimum election timeout, it may be down.
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(matches!(
        registry.transfer_leader(group, target).await,
        Err(LeadershipTransferError::UnreachableTarget { target: refused, .. }) if refused == target
    ));
    // The refused handoff left the leader writable.
    write("still-leading").await.unwrap();
    assert_eq!(raft.metrics().borrow_watched().current_leader, Some(leader));

    // Once it answers again and holds the committed log, it takes over.
    policy.clear();
    raft.wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| {
                metrics
                    .heartbeat
                    .as_ref()
                    .and_then(|acks| acks.get(&target))
                    .and_then(|acked| acked.as_ref())
                    .is_some_and(|acked| {
                        openraft::Instant::elapsed(&**acked) < Duration::from_millis(100)
                    })
            },
            "the target answers again",
        )
        .await
        .unwrap();
    wait_matched_committed(&raft, target).await;
    registry.transfer_leader(group, target).await.unwrap();
    raft.wait(Some(Duration::from_secs(5)))
        .current_leader(target, "the target takes over")
        .await
        .unwrap();
    shutdown_all(&engines).await;
}

/// A handoff the target never takes over (here it is cut off right after
/// it answered, and the other voter does not campaign) is abandoned: the
/// parked leader steps down and campaigns, shed or not, and leads again.
#[cfg(not(madsim))]
#[tokio::test]
async fn registry_handoff_abandons_a_transfer_nobody_takes_over() {
    use openraft::rt::WatchReceiver;
    let policy = InProcessRaftNetworkPolicy::default();
    let (_network, engines, leader, _roots) =
        build_three_node_cluster("handoff-parked", Some(policy.clone())).await;
    let leader_engine = engines
        .iter()
        .find(|engine| engine.raft.metrics().borrow_watched().id == leader)
        .unwrap();
    let raft = leader_engine.raft_handle();
    let target = (1..=3).find(|id| *id != leader).unwrap();
    let other = (1..=3).find(|id| *id != leader && *id != target).unwrap();
    let write = |name: &'static str| {
        raft.client_write(create_command(ursula_shard::BucketStreamId::new(
            "default", name,
        )))
    };
    write("first").await.unwrap();
    wait_matched_committed(&raft, target).await;
    let registry = RaftGroupHandleRegistry::default();
    registry.register_engine(leader_engine);
    for engine in &engines {
        engine.raft_handle().runtime_config().elect(false);
    }
    let vote = raft.metrics().borrow_watched().vote;
    policy.partition_bidirectional(leader, target);
    policy.partition_bidirectional(other, target);
    registry
        .transfer_leader(placement().raft_group_id, target)
        .await
        .unwrap();
    let forwarded = write("forwarded").await;
    assert!(matches!(
        forwarded,
        Err(openraft::error::RaftError::APIError(
            openraft::error::ClientWriteError::ForwardToLeader(ref forward)
        )) if forward.leader_id == Some(target)
    ));
    raft.wait(Some(Duration::from_secs(10)))
        .metrics(
            |metrics| {
                metrics.state == openraft::ServerState::Leader
                    && metrics.vote.leader_id().term() > vote.leader_id().term()
            },
            "the leader abandons the parked transfer",
        )
        .await
        .unwrap();
    write("resumed").await.unwrap();
    shutdown_all(&engines).await;
}

/// A leader steps down from a stalled transfer through a vote it grants
/// itself. The vote is marked a leadership transfer, so the leader's own
/// lease, which runs for a maximum election timeout after its election,
/// cannot refuse it.
#[cfg(not(madsim))]
#[tokio::test]
async fn a_transfer_step_down_is_not_refused_by_the_leaders_own_lease() {
    use openraft::rt::WatchReceiver;
    let (_network, engines, first, _roots) =
        build_three_node_cluster("step-down-lease", None).await;
    let first_raft = engines
        .iter()
        .find(|engine| engine.raft.metrics().borrow_watched().id == first)
        .unwrap()
        .raft_handle();
    // A leader elected a moment ago: its lease is still running.
    let leader = (1..=3).find(|id| *id != first).unwrap();
    first_raft.trigger().transfer_leader(leader).await.unwrap();
    first_raft
        .wait(Some(Duration::from_secs(5)))
        .current_leader(leader, "a fresh leader")
        .await
        .unwrap();
    let raft = engines
        .iter()
        .find(|engine| engine.raft.metrics().borrow_watched().id == leader)
        .unwrap()
        .raft_handle();
    raft.wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| metrics.state == openraft::ServerState::Leader,
            "the fresh leader leads",
        )
        .await
        .unwrap();
    let vote = raft.metrics().borrow_watched().vote;
    let last_log_id = raft.data_metrics().borrow_watched().last_log;
    let target = first;

    let mut plain = crate::owner::transfer_step_down(vote, target, last_log_id);
    plain.leadership_transfer = false;
    let refused = raft.vote(plain).await.unwrap();
    assert!(
        !refused.vote_granted,
        "the lease refuses a plain vote request"
    );
    assert_eq!(raft.metrics().borrow_watched().vote, vote);

    let stepped_down = raft
        .vote(crate::owner::transfer_step_down(vote, target, last_log_id))
        .await
        .unwrap();
    assert!(stepped_down.vote_granted);
    raft.wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| metrics.state != openraft::ServerState::Leader && metrics.vote != vote,
            "the leader stepped down",
        )
        .await
        .unwrap();
    shutdown_all(&engines).await;
}

/// The receive-side transfer policy decides from the request alone. A
/// transfer to another node only drops this node's lease, so it passes even
/// while this node sheds leadership or its recovery gate is closed. A
/// transfer to this node is refused when it carries this node's committed
/// vote, because this node already leads, or when this node may not
/// campaign. Its own uncommitted vote, or another node's committed vote,
/// passes.
#[cfg(not(madsim))]
#[tokio::test]
async fn incoming_transfer_policy_refuses_only_transfers_to_this_node() {
    let group = placement().raft_group_id;
    let node = 1;
    let transfer =
        |vote: UrsulaVote, to: u64| openraft::raft::TransferLeaderRequest::new(vote, to, None);
    let leader_vote = UrsulaVote::new_committed(5, 2);
    let own_vote = UrsulaVote::new_committed(5, node);
    let candidacy = UrsulaVote::new(6, node);

    let policy = ElectionPolicy::default();
    assert!(matches!(
        policy.validate_incoming_transfer(group, node, &transfer(leader_vote, node), None),
        Ok(())
    ));
    assert!(matches!(
        policy.validate_incoming_transfer(group, node, &transfer(candidacy, node), None),
        Ok(())
    ));
    assert!(matches!(
        policy.validate_incoming_transfer(group, node, &transfer(own_vote, node), None),
        Err(IncomingTransferError::OwnCommittedVote { vote, .. }) if vote == own_vote
    ));

    let (_root, store) = fresh_journal_store();
    let gate = GroupRejoin::durable(node, group, &store).await.unwrap();
    assert!(!gate.vote_gate_open());
    assert!(matches!(
        policy.validate_incoming_transfer(group, node, &transfer(leader_vote, node), Some(&gate)),
        Err(IncomingTransferError::MayNotCampaign { .. })
    ));
    let registry = RaftGroupHandleRegistry::default();
    registry.mark_leadership_shed(LeadershipShedReason::MaintenanceDrain);
    let shed = registry.election_policy();
    assert!(matches!(
        shed.validate_incoming_transfer(group, node, &transfer(leader_vote, node), None),
        Err(IncomingTransferError::MayNotCampaign { .. })
    ));
    assert!(matches!(
        shed.validate_incoming_transfer(group, node, &transfer(leader_vote, 3), Some(&gate)),
        Ok(())
    ));
}

/// Self-election as the admin endpoint requests it. The leader is refused
/// before OpenRaft could start an election under its leader state, and it
/// keeps its vote and commits. A stale term, a follower whose recovery gate
/// is closed and a follower that sheds leadership are refused. Any other
/// follower campaigns with the vote it observes and leads in a newer term.
#[cfg(not(madsim))]
#[tokio::test]
async fn registry_self_election_refuses_the_leader_and_elects_an_eligible_follower() {
    use openraft::rt::WatchReceiver;
    let (_network, engines, leader, _roots) = build_three_node_cluster("self-election", None).await;
    let group = placement().raft_group_id;
    let engine = |id: u64| {
        engines
            .iter()
            .find(|engine| engine.raft.metrics().borrow_watched().id == id)
            .unwrap()
    };
    let registry = |id: u64| {
        let registry = RaftGroupHandleRegistry::default();
        registry.register_engine(engine(id));
        registry
    };
    let term = |id: u64| engine(id).raft.metrics().borrow_watched().current_term;
    let gated = (1..=3).find(|id| *id != leader).unwrap();
    let eligible = (1..=3).find(|id| *id != leader && *id != gated).unwrap();

    let leader_raft = engine(leader).raft_handle();
    let vote = leader_raft.metrics().borrow_watched().vote;
    assert!(matches!(
        registry(leader).request_self_election(group, term(leader)).await,
        Err(SelfElectionError::Transfer(IncomingTransferError::OwnCommittedVote {
            vote: refused,
            ..
        })) if refused == vote
    ));
    leader_raft
        .client_write(create_command(ursula_shard::BucketStreamId::new(
            "default",
            "self-election",
        )))
        .await
        .unwrap();
    assert_eq!(leader_raft.metrics().borrow_watched().vote, vote);

    let (_gate_root, gate_store) = fresh_journal_store();
    let gate = Arc::new(
        GroupRejoin::durable(gated, group, &gate_store)
            .await
            .unwrap()
            .over_foreign_store(),
    );
    assert!(!gate.vote_gate_open());
    let gated_registry = RaftGroupHandleRegistry::default();
    engine(gated)
        .publish_recovery(gate, &gated_registry)
        .unwrap();
    assert!(matches!(
        gated_registry
            .request_self_election(group, term(gated))
            .await,
        Err(SelfElectionError::Transfer(
            IncomingTransferError::MayNotCampaign { .. }
        ))
    ));

    let eligible_registry = registry(eligible);
    let stale = term(eligible).checked_sub(1).unwrap();
    assert!(matches!(
        eligible_registry.request_self_election(group, stale).await,
        Err(SelfElectionError::TermChanged { requested, observed, .. })
            if requested == stale && observed == term(eligible)
    ));
    eligible_registry.mark_leadership_shed(LeadershipShedReason::MaintenanceDrain);
    assert!(matches!(
        eligible_registry
            .request_self_election(group, term(eligible))
            .await,
        Err(SelfElectionError::Transfer(
            IncomingTransferError::MayNotCampaign { .. }
        ))
    ));
    eligible_registry.clear_leadership_shed(LeadershipShedReason::MaintenanceDrain);
    assert_eq!(leader_raft.metrics().borrow_watched().vote, vote);

    wait_matched_committed(&leader_raft, eligible).await;
    eligible_registry
        .request_self_election(group, term(eligible))
        .await
        .unwrap();
    engine(eligible)
        .raft
        .wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| {
                metrics.state == openraft::ServerState::Leader
                    && metrics.vote.leader_id().term() > vote.leader_id().term()
            },
            "the follower leads in a newer term",
        )
        .await
        .unwrap();
    shutdown_all(&engines).await;
}

/// A handoff reaches its target through the in-process network, which
/// applies the receiver's transfer policy as the gRPC service does. A target
/// whose recovery gate is closed refuses it and never campaigns. The
/// transfer stalls, and a voter other than the target leads in a newer term.
#[cfg(not(madsim))]
#[tokio::test]
async fn registry_handoff_to_a_gated_follower_is_refused_by_the_follower() {
    use openraft::rt::WatchReceiver;
    let (network, engines, leader, _roots) =
        build_three_node_cluster("handoff-gated-target", None).await;
    let group = placement().raft_group_id;
    let engine = |id: u64| {
        engines
            .iter()
            .find(|engine| engine.raft.metrics().borrow_watched().id == id)
            .unwrap()
    };
    let gated = (1..=3).find(|id| *id != leader).unwrap();
    let leader_raft = engine(leader).raft_handle();
    let vote = leader_raft.metrics().borrow_watched().vote;

    // The gate keeps the target out of elections. Its vote floor lets it
    // keep taking the leader's appends, so the leader's handoff policy
    // still accepts it as a target.
    let (_gate_root, gate_store) = fresh_journal_store();
    let gate = Arc::new(
        GroupRejoin::durable(gated, group, &gate_store)
            .await
            .unwrap()
            .over_foreign_store(),
    );
    gate.establish_vote_floor(vote).await.unwrap();
    assert!(!gate.vote_gate_open());
    engine(gated)
        .publish_recovery(gate, &RaftGroupHandleRegistry::default())
        .unwrap();
    network.register(gated, engine(gated));
    leader_raft
        .client_write(create_command(ursula_shard::BucketStreamId::new(
            "default",
            "handoff-gated-target",
        )))
        .await
        .unwrap();
    wait_matched_committed(&leader_raft, gated).await;

    let registry = RaftGroupHandleRegistry::default();
    registry.register_engine(engine(leader));
    registry.transfer_leader(group, gated).await.unwrap();
    leader_raft
        .wait(Some(Duration::from_secs(10)))
        .metrics(
            |metrics| {
                metrics.vote.is_committed()
                    && metrics.vote.leader_id().term() > vote.leader_id().term()
                    && metrics.current_leader.is_some_and(|id| id != gated)
            },
            "a voter other than the gated target leads in a newer term",
        )
        .await
        .unwrap();
    let target = engine(gated).raft.metrics().borrow_watched().clone();
    assert_ne!(target.state, openraft::ServerState::Leader);
    assert_ne!(*target.vote.leader_id().node_id(), gated);
    shutdown_all(&engines).await;
}

#[cfg(not(madsim))]
#[tokio::test]
async fn recovery_proof_rejects_a_vote_change_after_read_index() {
    struct VoteChangingBarrier {
        raft: crate::RaftGroupHandle,
        barrier: Arc<crate::read_index::ReadIndexBarrier>,
    }
    impl ursula_runtime::LinearizableReadBarrier for VoteChangingBarrier {
        fn confirm(&self) -> ursula_runtime::ReadIndexFuture {
            let raft = self.raft.clone();
            let round = self.barrier.round();
            Box::pin(async move {
                let outcome = round.await?;
                raft.runtime_config().elect(false);
                raft.append_entries(UrsulaAppendEntriesRequest {
                    vote: UrsulaVote::new_committed(99, 2),
                    prev_log_id: None,
                    entries: Vec::new(),
                    leader_commit: None,
                })
                .await
                .unwrap();
                raft.wait(Some(Duration::from_secs(2)))
                    .metrics(|m| m.current_term == 99, "new vote published")
                    .await
                    .unwrap();
                Ok(outcome)
            })
        }
    }
    let (_root, store) = fresh_journal_store();
    let engine = RaftGroupEngine::new_single_node(
        placement(),
        1,
        BasicNode::new("local"),
        raft_config("changed-proof", 30, 60),
        store,
        Default::default(),
    )
    .await
    .unwrap();
    engine
        .raft
        .wait(Some(Duration::from_secs(2)))
        .current_leader(1, "leader elected")
        .await
        .unwrap();
    let barrier = VoteChangingBarrier {
        raft: engine.raft.clone(),
        barrier: engine.read_barrier.clone(),
    };
    let result = crate::registry::confirm_recovery_barrier(
        placement().raft_group_id,
        engine.read_barrier.owner(),
        &barrier,
    )
    .await;
    assert!(matches!(
        result,
        Err(crate::QuorumProofError::LeadershipChanged { .. })
    ));
}

#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registry_commands_execute_on_the_owner_and_waiters_do_not_block_rpc() {
    let wal_root = tempfile::tempdir().unwrap();
    let registry = RaftGroupHandleRegistry::default();
    let runtime = ShardRuntime::spawn_with_engine_factory(
        {
            let mut config = RuntimeConfig::new(1, 2);
            config.cpu_affinity = Some(true);
            config
        },
        DurableRaftGroupEngineFactory::new(start_wal(wal_root.path(), 1, 2))
            .with_registry(registry.clone()),
    )
    .unwrap();
    for group in [RaftGroupId(0), RaftGroupId(1)] {
        runtime.warm_group(group).await.unwrap();
        let handle = registry.get(group).unwrap();
        let name = handle
            .call(|_| async {
                #[cfg(target_os = "linux")]
                {
                    let status = std::fs::read_to_string("/proc/thread-self/status").unwrap();
                    let allowed = status
                        .lines()
                        .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
                        .unwrap()
                        .trim();
                    assert!(
                        !allowed.contains(','),
                        "owner must be pinned to one CPU: {allowed}"
                    );
                    assert!(
                        !allowed.contains('-'),
                        "owner must be pinned to one CPU: {allowed}"
                    );
                }
                std::thread::current().name().unwrap().to_owned()
            })
            .await
            .unwrap();
        assert_eq!(name, "ursula-core-0");
        let (release, wait) = tokio::sync::oneshot::channel::<()>();
        let (entered, entry) = tokio::sync::oneshot::channel();
        let slow = handle.clone();
        let waiting = tokio::spawn(async move {
            slow.call(move |_| async move {
                entered.send(()).unwrap();
                wait.await.unwrap();
            })
            .await
            .unwrap();
        });
        entry.await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), handle.with_raft_state(|_| ()))
            .await
            .unwrap()
            .unwrap();
        release.send(()).unwrap();
        waiting.await.unwrap();
        let (entered, entry) = tokio::sync::oneshot::channel();
        let (held, released) = tokio::sync::oneshot::channel::<()>();
        let caller = handle.clone();
        let task = tokio::spawn(async move {
            caller
                .call(move |_| async move {
                    let _held = held;
                    entered.send(()).unwrap();
                    std::future::pending::<()>().await;
                })
                .await
        });
        entry.await.unwrap();
        task.abort();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), released).await,
            Ok(Err(_))
        ));
        handle.shutdown().await.unwrap();
    }
}

#[cfg(not(madsim))]
#[tokio::test]
async fn replicated_append_batch_reserves_hot_capacity_across_unapplied_entries() {
    let dir = tempfile::tempdir().unwrap();
    let placement = placement();
    let factory = DurableRaftGroupEngineFactory::new(start_wal(dir.path(), 1, 1));
    let metrics = ursula_runtime::RuntimeMetrics::new(1, 1);
    let mut engine = factory
        .create(placement, metrics.group_engine_metrics())
        .await
        .unwrap();
    assert_append_batch_reserves_hot_capacity(&mut *engine, placement).await;
    engine.shutdown().await.unwrap();
}

async fn assert_append_batch_reserves_hot_capacity(
    engine: &mut dyn GroupEngine,
    placement: ShardPlacement,
) {
    let stream = bsid("batch-capacity");
    engine
        .create_stream(
            create_req(stream.clone()),
            placement,
            ColdWriteAdmission::default(),
        )
        .await
        .unwrap();
    let requests = (0..3)
        .map(|_| AppendRequest::from_bytes(stream.clone(), vec![7; 4]))
        .collect();
    let results = engine
        .append_batch(requests, placement, ColdWriteAdmission {
            max_hot_bytes_per_group: Some(8),
        })
        .await;
    assert_eq!(results.len(), 3);
    assert!(results[0].as_ref().is_ok_and(|r| r.group_hot_bytes == 4));
    assert!(results[1].as_ref().is_ok_and(|r| r.group_hot_bytes == 8));
    assert!(matches!(
        &results[2],
        Err(ursula_runtime::GroupEngineError::Infra(
            ursula_runtime::GroupInfraError::ColdBackpressure {
                limit: 8,
                after_group_hot_bytes: 12,
                ..
            }
        ))
    ));
}

#[cfg(madsim)]
#[test]
fn madsim_apply_failure_survives_real_leader_re_election() {
    use openraft::rt::WatchReceiver;
    for seed in [7, 19, 42] {
        let _guard = madsim_test_guard();
        crate::sim_runtime::run_seeded(seed, None, move || {
            crate::sim_runtime::MadsimOpenRaftRuntime::scope(seed, async move {
                let (_network, engines, leader_id) =
                    build_madsim_three_node_raft_cluster(seed).await;
                let leader = engines
                    .iter()
                    .find(|engine| engine.raft.metrics().borrow_watched().id == leader_id)
                    .unwrap();
                let stream = bsid("apply-re-election");
                create_stream_via_raft(leader, stream.clone()).await;
                let before = leader
                    .with_state_machine(|state| {
                        Box::pin(async move { state.last_applied_log_id.unwrap() })
                    })
                    .await
                    .unwrap();
                let index = before.index.checked_add(1).unwrap();
                leader
                    .with_state_machine(move |state| {
                        Box::pin(async move {
                            state.apply_fault = Some(if seed == 19 {
                                crate::apply_failure::ApplyFault::InvariantAfterMutation { index }
                            } else {
                                crate::apply_failure::ApplyFault::PanicAfterMutation { index }
                            });
                        })
                    })
                    .await
                    .unwrap();
                let error = leader
                    .write(append_command(stream.clone(), b"committed-poison"))
                    .await
                    .unwrap_err();
                assert!(matches!(
                    error,
                    GroupEngineError::Infra(ursula_runtime::GroupInfraError::OutcomeUnknown)
                ));
                let refused = leader
                    .write(append_command(stream.clone(), b"after-stop"))
                    .await
                    .unwrap_err();
                assert!(
                    matches!(refused, GroupEngineError::Infra(ursula_runtime::GroupInfraError::ApplyStopped { index: failed, .. }) if failed == index)
                );
                let candidates = engines
                    .iter()
                    .filter(|engine| engine.raft.metrics().borrow_watched().id != leader_id)
                    .map(|engine| {
                        Box::pin(async move {
                            engine
                                .raft
                                .wait(Some(Duration::from_secs(5)))
                                .metrics(
                                    |m| m.state == openraft::ServerState::Leader,
                                    "replacement elected",
                                )
                                .await
                                .map(|_| engine)
                        })
                    })
                    .collect::<Vec<_>>();
                let replacement = futures_util::future::select_all(candidates)
                    .await
                    .0
                    .unwrap();
                replacement
                    .raft
                    .ensure_linearizable(openraft::ReadPolicy::ReadIndex)
                    .await
                    .unwrap();
                replacement
                    .write(append_command(stream, b"after-re-election"))
                    .await
                    .unwrap();
                assert_eq!(
                    leader.raft.metrics().borrow_watched().last_applied,
                    Some(before)
                );
                assert_eq!(leader.apply_health.failure().unwrap().index, index);
                shutdown_all(&engines).await;
            })
        });
    }
}

/// A create of `stream_id` that expires at `expires_at_ms`.
#[cfg(not(madsim))]
fn expiring_create(
    stream_id: ursula_shard::BucketStreamId,
    expires_at_ms: u64,
) -> GroupWriteCommand {
    let mut request = create_req(stream_id);
    request.stream_expires_at_ms = Some(expires_at_ms);
    request.now_ms = 1;
    GroupWriteCommand::from(request)
}

/// #669: a group's leader sweeps the streams that expired in it in its
/// tidy pass, and the only entries the sweep appends are
/// `TouchStreamAccess { renew_ttl: false }` at its clock less the grace:
/// the command a read of an expired stream already proposed in 0.8.0, which
/// a 0.8.0 replica applies the same way.
#[cfg(not(madsim))]
#[tokio::test]
async fn a_leader_sweeps_expired_streams_with_the_0_8_0_touch_only() {
    let (_wal_root, log_store) = fresh_journal_store();
    let mut engine =
        RaftGroupEngine::new_single_node_on_log_store(placement(), log_store.clone(), None)
            .await
            .expect("create raft group engine");
    wait_history(&engine).await;
    for index in 0..300 {
        engine
            .raft
            .client_write(expiring_create(
                bsid(&format!("expiring-{index:03}")),
                5_000,
            ))
            .await
            .expect("create an expiring stream");
    }
    engine
        .raft
        .client_write(create_command(bsid("live")))
        .await
        .expect("create a live stream");
    // The log's last index: an answered write was appended before it
    // applied.
    let last_index = |store: Arc<RaftGroupFileLogStore>| async move {
        let mut store = store;
        store
            .get_log_state()
            .await
            .expect("log state")
            .last_log_id
            .map_or(0, |log_id| log_id.index)
    };
    let before = last_index(log_store.clone()).await;

    let report = engine
        .tidy_streams(
            ursula_runtime::TidyStreamsRequest {
                max_streams: 0,
                max_expiry_sweeps: 16,
                now_ms: 20_000,
            },
            placement(),
        )
        .await
        .expect("a tidy pass");
    // 257 streams, then the other 43.
    assert_eq!(report.expiry_sweeps, 2, "{report:?}");
    assert_eq!(report.expiry_due_groups, 0, "{report:?}");
    let after = last_index(log_store.clone()).await;
    assert_eq!(after, before.saturating_add(2));

    let mut store = log_store.clone();
    let mut reader = store.get_log_reader().await;
    let entries = reader
        .try_get_log_entries(before.saturating_add(1)..=after)
        .await
        .expect("read the sweep's entries");
    assert_eq!(entries.len(), 2);
    for entry in &entries {
        assert!(
            matches!(
                &entry.payload,
                EntryPayload::Normal(GroupWriteCommand::Stream(
                    ursula_stream::StreamCommand::TouchStreamAccess {
                        now_ms: 19_000,
                        renew_ttl: false,
                        ..
                    }
                ))
            ),
            "the sweep appended {:?}",
            entry.payload
        );
    }
    let gauges = engine.state_gauges(placement()).await.expect("gauges");
    assert_eq!(gauges.streams, 1);

    // Nothing left: the next pass appends nothing.
    let report = engine
        .tidy_streams(
            ursula_runtime::TidyStreamsRequest {
                max_streams: 0,
                max_expiry_sweeps: 16,
                now_ms: 20_000,
            },
            placement(),
        )
        .await
        .expect("an idle pass");
    assert_eq!(report.expiry_sweeps, 0);
    engine.shutdown().await.expect("shutdown raft group engine");
}

/// #669: only a group's leader sweeps: a follower's tidy pass proposes
/// nothing, and the leader's removes the streams on every replica.
#[cfg(not(madsim))]
#[tokio::test]
async fn a_follower_proposes_no_expiry_sweep() {
    let (_registry, mut engines, leader_id, _wal_roots) =
        build_three_node_cluster("ursula-expiry-sweep-follower-test", None).await;
    let leader_index = usize::try_from(leader_id.saturating_sub(1)).expect("leader id fits usize");
    let follower_index = if leader_index == 0 { 1 } else { 0 };
    wait_history(&engines[leader_index]).await;
    let created = engines[leader_index]
        .raft
        .client_write(expiring_create(bsid("expiring"), 5_000))
        .await
        .expect("create an expiring stream");
    let created_index = created.log_id.index();
    for engine in &engines {
        engine
            .raft
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(Some(created_index), "create replicated")
            .await
            .expect("wait for the create");
    }
    let request = ursula_runtime::TidyStreamsRequest {
        max_streams: 0,
        max_expiry_sweeps: 16,
        now_ms: 20_000,
    };
    let report = engines[follower_index]
        .tidy_streams(request, placement())
        .await
        .expect("a follower's pass");
    assert_eq!(report, ursula_runtime::TidyStreamsResponse::default());
    let report = engines[leader_index]
        .tidy_streams(request, placement())
        .await
        .expect("the leader's pass");
    assert_eq!(report.expiry_sweeps, 1, "{report:?}");
    let swept = created_index.saturating_add(1);
    for engine in &engines {
        engine
            .raft
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(Some(swept), "sweep replicated")
            .await
            .expect("wait for the sweep");
        let read = read_stream_via_state_machine(engine, bsid("expiring"), 16)
            .await
            .expect("access the state machine");
        assert!(
            matches!(&read, Err(err) if err.code() == Some(StreamErrorCode::StreamNotFound)),
            "the replica still holds the expired stream: {read:?}"
        );
    }
    shutdown_all(&engines).await;
}

/// OpenRaft runs a snapshot build in a task of its own that its shutdown
/// neither stops nor waits for. A build still running when its group engine
/// shuts down publishes no snapshot metadata afterwards, so nothing writes
/// the WAL directory once the WAL closes, and a restarted replica of the
/// group owns its metadata file alone (#489).
#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_snapshot_build_that_outlives_its_engine_publishes_nothing() {
    let root = tempfile::tempdir().unwrap();
    let wal = start_wal(root.path(), 1, 1);
    let metadata_path = wal.snapshot_metadata_path(placement());
    let store = wal
        .open(
            placement(),
            ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics(),
        )
        .unwrap();
    let engine = RaftGroupEngine::new_single_node(
        placement(),
        1,
        BasicNode::new("local"),
        raft_config("build-outlives-engine", 30, 60),
        store,
        RaftGroupEngineOptions {
            snapshot_metadata_path: Some(metadata_path.clone()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    // Stand in for the build OpenRaft detached: its builder, taken while the
    // group runs, builds only after the engine shut down.
    let mut builder = engine
        .with_state_machine(|state_machine| {
            Box::pin(async move { state_machine.get_snapshot_builder().await })
        })
        .await
        .unwrap();
    engine.shutdown().await.unwrap();
    wal.shutdown().await.unwrap();

    let error = builder.build_snapshot().await.unwrap_err();
    assert!(matches!(
        error.get_ref().and_then(
            |source| source.downcast_ref::<crate::state_machine::SnapshotMetadataStopped>()
        ),
        Some(crate::state_machine::SnapshotMetadataStopped::EngineShutDown)
    ));
    assert!(
        !metadata_path.exists() && !metadata_path.with_extension("json.tmp").exists(),
        "a build published snapshot metadata after its engine shut down"
    );
    drop(builder);
    fs::remove_dir_all(root.path()).expect("nothing writes the closed WAL directory");
}

/// A single-node engine of `placement()` over `wal` whose snapshot metadata
/// lives at the WAL's own path, as the factory builds it. The previous
/// engine's store may still be open for a moment after its shutdown: the
/// OpenRaft state-machine worker it leaves running holds it until it exits
/// (#709).
#[cfg(not(madsim))]
async fn reopen_journal_engine(wal: &RaftWal, cluster_name: &str) -> RaftGroupEngine {
    let store = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match wal.open(
                placement(),
                ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics(),
            ) {
                Ok(store) => break store,
                Err(crate::log_store::RaftWalError::OpenGroup {
                    source: crate::log_store::CoreJournalError::GroupAlreadyOpen { .. },
                    ..
                }) => tokio::time::sleep(Duration::from_millis(10)).await,
                Err(err) => panic!("open the group's log store: {err}"),
            }
        }
    })
    .await
    .expect("the stopped engine's log store closes");
    RaftGroupEngine::new_single_node(
        placement(),
        1,
        BasicNode::new("local"),
        raft_config(cluster_name, 30, 60),
        store,
        RaftGroupEngineOptions {
            snapshot_metadata_path: Some(wal.snapshot_metadata_path(placement())),
            ..Default::default()
        },
    )
    .await
    .unwrap()
}

/// A stream created through `engine`, which returns once the group applied
/// it, then a builder of the group, taken as OpenRaft's state-machine worker
/// takes one before it spawns the build.
#[cfg(not(madsim))]
async fn snapshot_builder_after_write(
    engine: &mut RaftGroupEngine,
    stream: &str,
) -> crate::state_machine::RaftGroupSnapshotBuilder {
    engine
        .create_stream(
            create_req(bsid(stream)),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .unwrap();
    engine
        .with_state_machine(|state_machine| {
            Box::pin(async move { state_machine.get_snapshot_builder().await })
        })
        .await
        .unwrap()
}

/// The restart of a recovery plan's cut (#489): engine A of a group stops
/// with a snapshot build still running, and engine B of the same group opens
/// on the same WAL and metadata path and publishes its own snapshot. A's
/// build, ending only then, publishes nothing over B's pointer, and B, whose
/// publication is its own, builds and publishes again.
#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_build_of_a_stopped_engine_leaves_its_restarted_replica_alone() {
    let root = tempfile::tempdir().unwrap();
    let wal = start_wal(root.path(), 1, 1);
    let metadata_path = wal.snapshot_metadata_path(placement());

    let mut engine_a = reopen_journal_engine(&wal, "restart-overlap-a").await;
    let mut stale = snapshot_builder_after_write(&mut engine_a, "written-by-a").await;
    engine_a.shutdown().await.unwrap();
    drop(engine_a);

    let mut engine_b = reopen_journal_engine(&wal, "restart-overlap-b").await;
    let mut first = snapshot_builder_after_write(&mut engine_b, "written-by-b").await;
    let first_built = first.build_snapshot().await.expect("B publishes");
    drop(first);
    let published = fs::read(&metadata_path).expect("B's pointer");
    assert!(
        stale.meta.last_log_id < first_built.meta.last_log_id,
        "the restarted replica applied past the stopped engine"
    );

    let error = stale.build_snapshot().await.unwrap_err();
    assert!(matches!(
        error.get_ref().and_then(
            |source| source.downcast_ref::<crate::state_machine::SnapshotMetadataStopped>()
        ),
        Some(crate::state_machine::SnapshotMetadataStopped::EngineShutDown)
    ));
    drop(stale);
    assert_eq!(
        fs::read(&metadata_path).expect("B's pointer"),
        published,
        "a build of the stopped engine replaced the restarted replica's pointer"
    );

    let mut second = snapshot_builder_after_write(&mut engine_b, "written-by-b-again").await;
    let second_built = second.build_snapshot().await.expect("B publishes again");
    drop(second);
    assert!(second_built.meta.last_log_id > first_built.meta.last_log_id);
    assert_ne!(
        fs::read(&metadata_path).expect("B's pointer"),
        published,
        "B's second snapshot is its current pointer"
    );

    engine_b.shutdown().await.unwrap();
    drop(engine_b);
    wal.shutdown().await.unwrap();
}
