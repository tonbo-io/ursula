use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(madsim)]
use std::sync::Mutex;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use futures_util::stream;
use openraft::BasicNode;
use openraft::Config;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::Raft;
use openraft::SnapshotPolicy;
use openraft::alias::VoteOf;
use openraft::entry::RaftEntry;
use openraft::rt::WatchReceiver;
use openraft::storage::IOFlushed;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use openraft::vote::RaftLeaderId;
use serde_json::json;
use ursula_control::ControlCommand;
use ursula_runtime::AppendBatchRequest;
use ursula_runtime::AppendRequest;
use ursula_runtime::CloseStreamRequest;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::GetStreamAttrsRequest;
use ursula_runtime::GroupEngine;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupEngineFactory;
use ursula_runtime::GroupInfraError;
use ursula_runtime::GroupWriteCommand;
use ursula_runtime::GroupWriteResponse;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::ProducerRequest;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::ReadStreamResponse;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::RuntimeThreading;
use ursula_runtime::ShardRuntime;
use ursula_runtime::StreamAttrs;
use ursula_runtime::StreamErrorCode;
use ursula_runtime::StreamErrorContext;
use ursula_runtime::UpdateStreamAttrsRequest;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;

use super::*;
use crate::codec::*;
use crate::engine::*;
use crate::forward::write_result_from_raft_response;
use crate::log_store::*;
use crate::registry::*;
use crate::types::*;

type CommittedLeaderId = <UrsulaRaftTypeConfig as openraft::RaftTypeConfig>::LeaderId;
type MetaLeaderId = <MetaRaftTypeConfig as openraft::RaftTypeConfig>::LeaderId;

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
        record: None,
        max_records: None,
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

fn append_command(stream_id: ursula_shard::BucketStreamId, payload: &[u8]) -> GroupWriteCommand {
    GroupWriteCommand::from(AppendRequest::from_bytes(stream_id, payload.to_vec()))
}

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
/// leader. Returns the registry, the engines (index = node id - 1), and the
/// elected leader id.
async fn build_three_node_cluster(
    cluster_name: &str,
    policy: Option<InProcessRaftNetworkPolicy>,
) -> (InProcessRaftRegistry, Vec<RaftGroupEngine>, u64) {
    let registry = InProcessRaftRegistry::default();
    let config = raft_config(cluster_name, 50, 100);
    let mut nodes = BTreeMap::new();
    for node_id in 1..=3 {
        nodes.insert(node_id, BasicNode::new(format!("node-{node_id}")));
    }

    let mut engines = Vec::new();
    for node_id in 1..=3 {
        let mut network = InProcessRaftNetworkFactory::new(registry.clone()).with_source(node_id);
        if let Some(policy) = &policy {
            network = network.with_policy(policy.clone());
        }
        let engine = RaftGroupEngine::new_node_with_log_store_and_network(
            placement(),
            node_id,
            config.clone(),
            network,
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
        .expect("initialize raft group");
    let leader_metrics = engines[0]
        .raft
        .wait(Some(Duration::from_secs(5)))
        .metrics(|metrics| metrics.current_leader.is_some(), "leader elected")
        .await
        .expect("wait for leader");
    let leader_id = leader_metrics.current_leader.expect("leader id");
    (registry, engines, leader_id)
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

fn stream_attrs(title: &str, purpose: &str) -> StreamAttrs {
    StreamAttrs {
        title: Some(title.to_owned()),
        metadata: json!({
            "agent": { "id": "agent-1", "version": 2 },
            "purpose": purpose
        })
        .as_object()
        .expect("metadata object")
        .clone(),
    }
}

#[test]
fn raft_group_command_round_trips_through_wire_codec() {
    let command = GroupWriteCommand::Stream(ursula_stream::StreamCommand::AppendBatch {
        stream_id: bsid("shared-wire-log"),
        content_type: Some("application/octet-stream".to_owned()),
        payloads: vec![b"ab".to_vec().into(), b"cd".to_vec().into()],
        producer: Some(ProducerRequest {
            producer_id: "writer-1".to_owned(),
            producer_epoch: 7,
            producer_seq: 42,
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
fn stream_attrs_update_command_round_trips_through_wire_codec() {
    let command = GroupWriteCommand::from(UpdateStreamAttrsRequest {
        stream_id: bsid("attrs-wire"),
        attrs: Some(stream_attrs("Support session", "customer-support")),
        now_ms: 123,
    });

    let encoded = encode_wire(&command);
    let decoded: GroupWriteCommand =
        decode_wire(&encoded, "group command").expect("decode attrs update command");
    assert_eq!(decoded, command);
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
        record_range: None,
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

fn temp_log_path(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time is after epoch")
        .as_nanos();
    std::env::temp_dir()
        .join("ursula-raft-tests")
        .join(format!("{name}-{}-{nonce}.bin", std::process::id()))
}

fn wire_frame_count<T: serde::Serialize + serde::de::DeserializeOwned>(path: &Path) -> usize {
    let bytes = fs::read(path).expect("read log file");
    read_wire_frames::<T>(&bytes)
        .expect("decode wire frames")
        .len()
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
async fn meta_raft_log_store_appends_reads_truncates_and_purges() {
    let mut store = MetaRaftLogStore::shared();
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
    let mut store = MetaRaftLogStore::shared();
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
    let mut store = MetaRaftLogStore::shared();
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

#[test]
fn public_meta_log_store_type_is_exported_from_crate_root() {
    let _store = crate::MetaRaftLogStore::shared();
    let _generic = crate::MemoryRaftLogStore::<crate::MetaRaftTypeConfig>::shared();
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
    assert_eq!(wire_frame_count::<RaftGroupLogRecord>(&path), 3);

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
    assert_eq!(wire_frame_count::<RaftGroupLogRecord>(&path), 2);

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
    assert_eq!(wire_frame_count::<RaftGroupLogRecord>(&path), 4);

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
async fn single_node_meta_raft_applies_node_registration() {
    let config = raft_config("ursula-meta-single-node-test", 30, 60);
    let mut log_store = MetaRaftLogStore::shared();
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

#[tokio::test]
async fn meta_raft_handle_registers_initial_data_nodes() {
    let config = raft_config("ursula-meta-initial-data-nodes-test", 30, 60);
    let handle = MetaRaftHandle::new_single_node_with_log_store(
        1,
        BasicNode::new("meta-local"),
        config,
        MetaRaftLogStore::shared(),
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

#[tokio::test]
async fn meta_raft_handle_rejects_invalid_initial_data_nodes() {
    let config = raft_config("ursula-meta-invalid-initial-data-nodes-test", 30, 60);
    let handle = MetaRaftHandle::new_single_node_with_log_store(
        1,
        BasicNode::new("meta-local"),
        config,
        MetaRaftLogStore::shared(),
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
    assert!(
        err.to_string()
            .contains("node 2 rejected: client_url must not be empty"),
        "unexpected error: {err}"
    );

    handle
        .shutdown()
        .await
        .expect("shutdown single-node meta raft handle");
}

#[test]
fn dynamic_group_hosting_allows_non_voter_warmup() {
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

#[tokio::test]
async fn single_node_openraft_group_applies_client_writes() {
    let config = raft_config("ursula-single-node-test", 30, 60);
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

#[tokio::test]
async fn three_node_openraft_group_replicates_group_writes() {
    let (_registry, engines, leader_id) =
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

// NOTE: tokio-level fault-injection tests for partition/heal replication and
// the "minority leader must not ack" invariant were removed; those scenarios
// are covered by ursula-sim DST scenarios PartitionHeal, SnapshotCatchUp,
// RuntimeRaftNetwork, and isolated_leader_pending_write_snapshot_purge.

#[cfg(madsim)]
static MADSIM_TEST_LOCK: Mutex<()> = Mutex::new(());

#[cfg(madsim)]
fn madsim_test_guard() -> std::sync::MutexGuard<'static, ()> {
    MADSIM_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(madsim)]
fn check_madsim_determinism<F>(seed: u64, config: madsim::Config, f: fn() -> F) -> F::Output
where
    F: std::future::Future + 'static,
    F::Output: Send,
{
    let _guard = madsim_test_guard();
    madsim::runtime::Runtime::check_determinism(seed, config, f)
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
#[ignore = "diagnostic probe; run individually because madsim check_determinism state is process-global"]
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
#[ignore = "diagnostic probe; run individually because madsim check_determinism state is process-global"]
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
#[ignore = "diagnostic probe; run individually because madsim check_determinism state is process-global"]
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
#[ignore = "diagnostic probe; run individually because madsim check_determinism state is process-global"]
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
#[ignore = "diagnostic probe; run individually because madsim check_determinism state is process-global"]
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
#[ignore = "diagnostic probe; run individually because madsim check_determinism state is process-global"]
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
#[ignore = "diagnostic probe; run individually because madsim check_determinism state is process-global"]
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
#[ignore = "diagnostic probe; run individually because madsim check_determinism state is process-global"]
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
    let mut runtime =
        madsim::runtime::Runtime::with_seed_and_config(seed, madsim::Config::default());
    runtime.set_time_limit(Duration::from_secs(10));
    runtime.block_on(crate::sim_runtime::MadsimOpenRaftRuntime::scope(
        seed,
        async move {
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
        },
    ))
}

#[cfg(madsim)]
fn run_madsim_three_node_raft_with_fault_script_once(seed: u64) -> (u64, u64, u64) {
    let mut runtime =
        madsim::runtime::Runtime::with_seed_and_config(seed, madsim::Config::default());
    runtime.set_time_limit(Duration::from_secs(10));
    runtime.block_on(crate::sim_runtime::MadsimOpenRaftRuntime::scope(
        seed,
        async move {
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
        },
    ))
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
                    RaftGroupLogStore::shared(),
                    None,
                    None,
                )
                .await
                .expect("create simulated raft group node");
                assert!(tx.send(engine).is_ok(), "send simulated raft group node");
            },
        ));
        let engine = rx.await.expect("receive simulated raft group node");
        registry.register(node_id, engine.raft.clone());
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
            InProcessRaftNetworkFactory::new(registry.clone()).with_source(node_id),
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

    let read = read_stream_via_state_machine(&engines[2], stream_id.clone(), 64)
        .await
        .expect("read lagging learner state machine")
        .expect("stream restored from snapshot is readable");
    assert_eq!(read.payload, b"snapshot-transfer");

    shutdown_all(&engines).await;
}

#[tokio::test]
async fn raft_group_engine_implements_runtime_group_engine_over_openraft() {
    let mut engine = RaftGroupEngine::new_single_node(placement())
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
    assert_eq!(snapshot.group_commit_index, 2);
    engine.shutdown().await.expect("shutdown raft group engine");
}

#[tokio::test]
async fn raft_group_engine_applies_batched_runtime_writes() {
    let mut engine = RaftGroupEngine::new_single_node(placement())
        .await
        .expect("create raft group engine");
    let stream_id = bsid("raft-group-engine-batch");

    let responses = engine
        .write_batch(
            vec![
                create_command(stream_id.clone()),
                GroupWriteCommand::from(AppendBatchRequest::new(stream_id.clone(), vec![
                    b"ab".to_vec(),
                    b"cd".to_vec(),
                ])),
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
        .read_stream(read_req(stream_id, 16), placement())
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
    let stream_id = bsid("raft-group-engine-cold-batch-many");

    engine
        .create_stream(
            CreateStreamRequest::new(stream_id.clone(), "application/octet-stream"),
            placement(),
            ColdWriteAdmission::default(),
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
        .append_batch_many(
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
        .read_stream(read_req(stream_id, 16), placement())
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
    let stream_id = bsid("raft-logical-command-metrics");

    runtime
        .create_stream(create_req(stream_id.clone()))
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
        .read_stream(read_req(stream_id, 16))
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

#[tokio::test]
async fn raft_group_engine_recovers_client_writes_from_file_log() {
    let path = temp_log_path("raft-group-engine-recover");
    let stream_id = bsid("raft-engine-recover");

    {
        let mut engine = RaftGroupEngine::new_single_node_with_file_log(placement(), &path)
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

    let mut recovered = RaftGroupEngine::new_single_node_with_file_log(placement(), &path)
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

    let _ = fs::remove_file(&path);
}

#[tokio::test]
async fn shard_runtime_uses_raft_group_engine_factory_for_owned_group() {
    let config = hosted_config(1, 1);
    let runtime = ShardRuntime::spawn_with_engine_factory(config, RaftGroupEngineFactory)
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
    let attrs = stream_attrs("Runtime raft session", "customer-support");
    runtime
        .update_stream_attrs(UpdateStreamAttrsRequest {
            stream_id: stream_id.clone(),
            attrs: Some(attrs.clone()),
            now_ms: 0,
        })
        .await
        .expect("update attrs through runtime-owned raft group");

    let current_attrs = runtime
        .get_stream_attrs(GetStreamAttrsRequest {
            stream_id: stream_id.clone(),
            now_ms: 0,
        })
        .await
        .expect("read attrs through runtime-owned raft group");
    assert_eq!(current_attrs.attrs, Some(attrs));

    let read = runtime
        .read_stream(read_req(stream_id, 16))
        .await
        .expect("read through runtime-owned raft group");
    assert_eq!(read.payload, b"payload");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn warm_group_registers_runtime_owned_raft_handle() {
    let registry = RaftGroupHandleRegistry::default();
    let config = hosted_config(2, 4);
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

    let config = hosted_config(1, 1);
    let runtime =
        ShardRuntime::spawn_with_engine_factory(config, DurableRaftGroupEngineFactory::new(&root))
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

    drop(runtime);
    let _ = fs::remove_dir_all(&root);
}

#[tokio::test]
async fn durable_raft_group_engine_recovers_from_core_journal() {
    let root = temp_log_path("raft-core-journal-recover-root").with_extension("");
    let _ = fs::remove_dir_all(&root);
    let stream_id = bsid("raft-core-journal-recover");

    {
        let config = hosted_config(1, 1);
        let runtime = ShardRuntime::spawn_with_engine_factory(
            config,
            DurableRaftGroupEngineFactory::new(&root),
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
        let config = hosted_config(1, 1);
        let recovered = ShardRuntime::spawn_with_engine_factory(
            config,
            DurableRaftGroupEngineFactory::new(&root),
        )
        .expect("spawn recovered durable runtime");
        let read = recovered
            .read_stream(read_req(stream_id, 32))
            .await
            .expect("read recovered stream");
        assert_eq!(read.payload, b"journal-payload");
    }

    let _ = fs::remove_dir_all(&root);
}

#[tokio::test]
async fn openraft_state_machine_applies_group_write_commands() {
    let stream_id = bsid("raft-apply");
    let mut sm = RaftGroupStateMachine::new(placement());
    let entries = vec![
        normal_entry(1, create_command(stream_id.clone())),
        normal_entry(2, append_command(stream_id.clone(), b"abc")),
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
    let stream_id = bsid("raft-snapshot");
    let mut source = RaftGroupStateMachine::new(placement());
    let entries = vec![
        normal_entry(1, create_command(stream_id.clone())),
        normal_entry(2, append_command(stream_id, b"payload")),
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
