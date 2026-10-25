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
use openraft::Config;
use openraft::EmptyNode;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::LogId;
#[cfg(not(madsim))]
use openraft::Raft;
#[cfg(not(madsim))]
use openraft::SnapshotPolicy;
#[cfg(not(madsim))]
use openraft::StorageError;
#[cfg(not(madsim))]
use openraft::alias::VoteOf;
use openraft::entry::RaftEntry;
#[cfg(not(madsim))]
use openraft::storage::IOFlushed;
#[cfg(not(madsim))]
use openraft::storage::RaftLogReader;
#[cfg(not(madsim))]
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
use ursula_runtime::GroupWriteResponse;
use ursula_runtime::HeadStreamRequest;
#[cfg(not(madsim))]
use ursula_runtime::LeadershipReason;
#[cfg(not(madsim))]
use ursula_runtime::LeadershipRefusal;
use ursula_runtime::ProducerEarlyAnswer;
use ursula_runtime::ProducerRequest;
#[cfg(not(madsim))]
use ursula_runtime::Proposal;
#[cfg(not(madsim))]
use ursula_runtime::ReadSnapshotRequest;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::ReadStreamResponse;
use ursula_runtime::ResponseKind;
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
use ursula_stream::StreamCommand;

use super::*;
use crate::codec::*;
#[cfg(not(madsim))]
use crate::election::TransferAsk;
#[cfg(not(madsim))]
use crate::engine::test_support::open_single_voter;
#[cfg(not(madsim))]
use crate::engine::test_support::single_node_factory;
#[cfg(not(madsim))]
use crate::engine::test_support::single_voter;
#[cfg(not(madsim))]
use crate::engine::test_support::start_single_voter_on;
use crate::engine::*;
use crate::forward::write_result_from_raft_response;
use crate::log_store::*;
#[cfg(not(madsim))]
use crate::registry::*;
use crate::types::*;

type CommittedLeaderId = <UrsulaRaftTypeConfig as openraft::RaftTypeConfig>::LeaderId;

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
        ursula_control::Admission::Create,
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
        ursula_control::Admission::Create,
        ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics(),
    )
    .expect("open the log store");
    (root, store)
}

/// A fresh WAL in a new temporary directory, which the caller keeps while
/// the WAL is in use.
#[cfg(not(madsim))]
fn fresh_wal() -> (tempfile::TempDir, RaftWal) {
    let root = tempfile::tempdir().expect("WAL root");
    let wal = RaftWal::start(
        root.path(),
        WalFsync::Never,
        &ursula_shard::StaticShardMap::new(1, 1).expect("valid topology"),
    )
    .expect("start the Raft WAL");
    (root, wal)
}

/// `placement()`'s log store on a fresh WAL on the simulated disk.
#[cfg(madsim)]
fn sim_journal_store(name: &str) -> Arc<RaftGroupFileLogStore> {
    sim_wal(name)
        .open(
            placement(),
            ursula_control::Admission::Create,
            ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics(),
        )
        .expect("open the log store")
}

/// The recovery gate of `node_id`'s replica of [`placement`]'s group on
/// `store`, opened as the engine factories open one.
async fn gate_of(node_id: u64, store: &Arc<RaftGroupFileLogStore>) -> Arc<GroupRejoin> {
    RaftGroupEngineOptions::gated_on(node_id, placement(), store)
        .await
        .recovery
}

/// A fresh WAL on the simulated disk.
#[cfg(madsim)]
fn sim_wal(name: &str) -> RaftWal {
    let root = SimDisk::provision_dir(name).expect("provision a simulated WAL");
    RaftWal::start(
        root,
        WalFsync::Never,
        &ursula_shard::StaticShardMap::new(1, 1).expect("valid topology"),
    )
    .expect("start the Raft WAL")
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
            (1u64, EmptyNode {}),
            (2u64, EmptyNode {}),
            (3u64, EmptyNode {}),
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

/// A forwarded write that the leader refuses as a whole answers each of
/// its commands with the leader's typed refusal, so the follower answers
/// what the leader would have (a pending history answers `503`), where a
/// gRPC status carried only its text.
#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forwarded_write_refused_as_a_whole_answers_each_command_typed() {
    use crate::raft_internal_proto::raft_internal_server::RaftInternal;

    let wal_root = tempfile::tempdir().unwrap();
    let registry = RaftGroupHandleRegistry::default();
    let wal = start_wal(wal_root.path(), 1, 1);
    let runtime = ShardRuntime::spawn_with_engine_factory(
        RuntimeConfig::new(1, 1),
        PlacedRaftGroupEngineFactory::single_node(wal, registry.clone(), 1),
    )
    .unwrap();
    runtime.warm_group(RaftGroupId(0)).await.unwrap();
    let service = crate::grpc::RaftGrpcService::new(registry);
    // A data group never takes CreateBucket: the leader refuses the batch
    // before it proposes any of it.
    let unproposable = RaftCommand::Stream(ursula_stream::StreamCommand::CreateBucket {
        bucket_id: "forwarded".to_owned(),
    });
    let response = service
        .group_write(tonic::Request::new(
            crate::raft_internal_proto::GroupWriteRequestV1 {
                plane: crate::raft_internal_proto::RaftPlaneV1::Data.into(),
                target_node: 1,
                protocol_version: crate::grpc::RAFT_GRPC_PROTOCOL_VERSION,
                command_payloads: vec![encode_wire(&unproposable), encode_wire(&unproposable)],
                ..Default::default()
            },
        ))
        .await
        .expect("a refused batch is an answer, not a gRPC error")
        .into_inner();
    assert_eq!(response.results.len(), 2);
    for result in response.results {
        assert!(!result.ok);
        let refusal: GroupEngineError =
            decode_wire(&result.payload, "group engine error").expect("decodes");
        assert_eq!(
            refusal,
            GroupEngineError::Infra(GroupInfraError::InvalidRaftCommand {
                command: ursula_runtime::UnproposableCommand::CreateBucket,
            })
        );
    }

    // A data group never takes a meta command: a batch that carries one is
    // refused as a whole, before anything of it is proposed.
    let meta = RaftCommand::Meta(crate::meta::MetaCommand::Control(
        ursula_control::samples::register(1, 1),
    ));
    let response = service
        .group_write(tonic::Request::new(
            crate::raft_internal_proto::GroupWriteRequestV1 {
                plane: crate::raft_internal_proto::RaftPlaneV1::Data.into(),
                target_node: 1,
                protocol_version: crate::grpc::RAFT_GRPC_PROTOCOL_VERSION,
                command_payloads: vec![
                    encode_wire(&create_stream_command("cross-plane")),
                    encode_wire(&meta),
                ],
                ..Default::default()
            },
        ))
        .await
        .expect("a refused batch is an answer, not a gRPC error")
        .into_inner();
    assert_eq!(response.results.len(), 2);
    for result in response.results {
        assert!(!result.ok);
        let refusal: GroupEngineError =
            decode_wire(&result.payload, "group engine error").expect("decodes");
        assert_eq!(
            refusal,
            GroupEngineError::Infra(GroupInfraError::InvalidRaftCommand {
                command: ursula_runtime::UnproposableCommand::OtherPlane,
            })
        );
    }
}

/// Every group RPC checks its target before the group: the plane, the
/// group a meta request names and the node the request is for. A request
/// for another node is refused with the misdelivery refusal, which names
/// this node and is no protocol mismatch, and the meta group, which no
/// node runs yet, is answered as a group this node does not run.
#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn group_rpcs_check_their_target_before_the_group() {
    use crate::raft_internal_proto as pb;
    use crate::raft_internal_proto::raft_internal_server::RaftInternal;

    let wal_root = tempfile::tempdir().unwrap();
    let registry = RaftGroupHandleRegistry::default();
    let wal = start_wal(wal_root.path(), 1, 1);
    let runtime = ShardRuntime::spawn_with_engine_factory(
        RuntimeConfig::new(1, 1),
        PlacedRaftGroupEngineFactory::single_node(wal, registry.clone(), 1),
    )
    .unwrap();
    runtime.warm_group(RaftGroupId(0)).await.unwrap();
    let service = crate::grpc::RaftGrpcService::new(registry);
    let version = crate::grpc::RAFT_GRPC_PROTOCOL_VERSION;
    let write = |plane: pb::RaftPlaneV1, target_node| pb::GroupWriteRequestV1 {
        plane: plane.into(),
        target_node,
        protocol_version: version,
        ..Default::default()
    };
    let read = |plane: pb::RaftPlaneV1, target_node| pb::GroupReadRequestV1 {
        plane: plane.into(),
        target_node,
        protocol_version: version,
        ..Default::default()
    };
    let barrier = |plane: pb::RaftPlaneV1, target_node| pb::RejoinBarrierRequestV1 {
        plane: plane.into(),
        target_node,
        protocol_version: version,
        ..Default::default()
    };
    fn code<T>(result: Result<tonic::Response<T>, tonic::Status>) -> Option<tonic::Code> {
        result.err().map(|status| status.code())
    }
    for (plane, node, expected) in [
        (
            pb::RaftPlaneV1::Unspecified,
            1,
            tonic::Code::InvalidArgument,
        ),
        (pb::RaftPlaneV1::Meta, 1, tonic::Code::NotFound),
        (pb::RaftPlaneV1::Data, 2, tonic::Code::FailedPrecondition),
    ] {
        assert_eq!(
            code(
                service
                    .group_write(tonic::Request::new(write(plane, node)))
                    .await
            ),
            Some(expected),
            "GroupWrite of plane {plane:?} for node {node}"
        );
        assert_eq!(
            code(
                service
                    .rejoin_barrier(tonic::Request::new(barrier(plane, node)))
                    .await
            ),
            Some(expected),
            "RejoinBarrier of plane {plane:?} for node {node}"
        );
    }
    // The meta group serves no stream reads.
    assert_eq!(
        code(
            service
                .group_read(tonic::Request::new(read(pb::RaftPlaneV1::Meta, 1)))
                .await
        ),
        Some(tonic::Code::InvalidArgument)
    );
    let misdelivered = service
        .group_read(tonic::Request::new(read(pb::RaftPlaneV1::Data, 2)))
        .await
        .expect_err("node 1 is not node 2");
    assert_eq!(misdelivered.code(), tonic::Code::FailedPrecondition);
    assert_eq!(crate::target::refusing_node(&misdelivered, 2), Some(1));
    assert!(!crate::format_epoch::is_protocol_mismatch(&misdelivered));
    // A meta request names group 0.
    let mut meta_group = write(pb::RaftPlaneV1::Meta, 1);
    meta_group.raft_group_id = 3;
    assert_eq!(
        code(service.group_write(tonic::Request::new(meta_group)).await),
        Some(tonic::Code::InvalidArgument)
    );
    // The genesis probe checks its target node, and no node answers it yet.
    let probe =
        crate::meta::genesis::genesis_probe_request(2, &crate::meta::genesis::GenesisProbe {
            claim: ursula_control::samples::sample_genesis_claim(),
            vote: crate::bootstrap_probe_vote(),
        });
    let refused = service
        .genesis_probe(tonic::Request::new(probe.clone()))
        .await
        .expect_err("node 1 is not node 2");
    assert_eq!(crate::target::refusing_node(&refused, 2), Some(1));
    let mut probe = probe;
    probe.target_node = 1;
    assert_eq!(
        code(service.genesis_probe(tonic::Request::new(probe)).await),
        Some(tonic::Code::NotFound)
    );
}

/// An inbound append to a data group whose entries carry a meta command is
/// refused (`INVALID_ARGUMENT`) before OpenRaft sees it: the group keeps
/// its vote, though the request's is higher. The same request without the
/// meta entry reaches OpenRaft, which takes its vote.
#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_append_carrying_a_meta_command_is_refused_before_openraft_sees_it() {
    use openraft::rt::WatchReceiver;

    use crate::raft_internal_proto as pb;
    use crate::raft_internal_proto::raft_internal_server::RaftInternal;

    let wal_root = tempfile::tempdir().unwrap();
    let registry = RaftGroupHandleRegistry::default();
    let wal = start_wal(wal_root.path(), 1, 1);
    let runtime = ShardRuntime::spawn_with_engine_factory(
        RuntimeConfig::new(1, 1),
        PlacedRaftGroupEngineFactory::single_node(wal, registry.clone(), 1),
    )
    .unwrap();
    runtime.warm_group(RaftGroupId(0)).await.unwrap();
    let mut metrics = registry.get(RaftGroupId(0)).unwrap().metrics();
    let term = metrics.borrow_watched().current_term;
    let higher = term + 10;
    let service = crate::grpc::RaftGrpcService::new(registry.clone());
    let append = |entries| {
        let request = UrsulaAppendEntriesRequest {
            vote: UrsulaVote::new_committed(higher, 2),
            prev_log_id: None,
            entries,
            leader_commit: None,
        };
        let mut envelope = pb::RaftRpcEnvelopeV1 {
            protocol_version: crate::grpc::RAFT_GRPC_PROTOCOL_VERSION,
            payload: encode_wire(&request),
            ..Default::default()
        };
        crate::target::WireTarget::stamp(crate::RaftTarget::Data(RaftGroupId(0)), 1, &mut envelope);
        tonic::Request::new(envelope)
    };
    let meta = RaftCommand::Meta(crate::meta::MetaCommand::Control(
        ursula_control::samples::register(2, 1),
    ));
    let entry = Entry::new(
        LogId {
            leader_id: CommittedLeaderId::new(higher, 2),
            index: 1,
        },
        EntryPayload::Normal(meta),
    );
    let refused = service
        .append(append(vec![entry]))
        .await
        .expect_err("a data group takes no meta command");
    assert_eq!(refused.code(), tonic::Code::InvalidArgument);
    assert!(!crate::format_epoch::is_protocol_mismatch(&refused));
    assert_eq!(metrics.borrow_watched().current_term, term);

    service
        .append(append(Vec::new()))
        .await
        .expect("an append without the meta entry reaches OpenRaft");
    tokio::time::timeout(Duration::from_secs(10), async {
        while metrics.borrow_watched().current_term < higher {
            metrics.changed().await.unwrap();
        }
    })
    .await
    .expect("OpenRaft takes the request's vote");
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

fn create_command(stream_id: ursula_shard::BucketStreamId) -> RaftCommand {
    RaftCommand::Stream(StreamCommand::from(create_req(stream_id)))
}

/// A group's genesis history event, for a test that builds a log or writes
/// to a raw Raft core: a group applies nothing before it (#581 section 4.3).
pub(crate) fn genesis_command() -> RaftCommand {
    RaftCommand::Stream(ursula_stream::StreamCommand::NewHistory {
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

fn append_command(stream_id: ursula_shard::BucketStreamId, payload: &[u8]) -> RaftCommand {
    RaftCommand::Stream(append_stream_command(stream_id, payload))
}

/// The append [`append_command`] carries, as a data engine proposes it.
fn append_stream_command(stream_id: ursula_shard::BucketStreamId, payload: &[u8]) -> StreamCommand {
    StreamCommand::from(AppendRequest::from_bytes(stream_id, payload.to_vec()))
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
    let (registry, engines, leader, roots, _gate) =
        build_three_node_cluster_gating(cluster_name, policy, address, None).await;
    (registry, engines, leader, roots)
}

/// The node that [`build_three_node_cluster_with_a_gated_node`] gates.
#[cfg(not(madsim))]
const GATED_NODE: u64 = 3;

/// [`build_three_node_cluster`] whose node [`GATED_NODE`] starts as a node on
/// an empty WAL does, as the node's engine factory starts it: with a closed
/// recovery gate over its own log store and no vote floor. It does not
/// campaign, joins only the first election, and takes no replication until
/// the test gives it a floor. Nodes 1 and 2 elect the leader.
#[cfg(not(madsim))]
async fn build_three_node_cluster_with_a_gated_node(
    cluster_name: &str,
) -> (
    InProcessRaftRegistry,
    Vec<RaftGroupEngine>,
    u64,
    Vec<tempfile::TempDir>,
    Arc<GroupRejoin>,
) {
    let (registry, engines, leader, roots, gate) = build_three_node_cluster_gating(
        cluster_name,
        None,
        |node_id| format!("node-{node_id}"),
        Some(GATED_NODE),
    )
    .await;
    (
        registry,
        engines,
        leader,
        roots,
        gate.expect("the gated node's gate"),
    )
}

#[cfg(not(madsim))]
async fn build_three_node_cluster_gating(
    cluster_name: &str,
    policy: Option<InProcessRaftNetworkPolicy>,
    address: impl Fn(u64) -> String,
    gated: Option<u64>,
) -> (
    InProcessRaftRegistry,
    Vec<RaftGroupEngine>,
    u64,
    Vec<tempfile::TempDir>,
    Option<Arc<GroupRejoin>>,
) {
    let registry = InProcessRaftRegistry::default();
    let mut gate = None;
    // OpenRaft also uses election_timeout_min as the vote RPC deadline.
    // Leave room for concurrent tests fsyncing votes on the same executor host.
    let config = raft_config(cluster_name, 500, 1_000);
    let nodes = BTreeSet::from([1, 2, 3]);
    let peers = || {
        crate::PeerDirectory::new(
            (1..=3).map(|node_id| (node_id, crate::PeerEndpoints::shared(address(node_id)))),
        )
    };

    let mut engines = Vec::new();
    let mut wal_roots = Vec::new();
    for node_id in 1..=3 {
        let mut network = InProcessRaftNetworkFactory::new(registry.clone()).with_source(node_id);
        if let Some(policy) = &policy {
            network = network.with_policy(policy.clone());
        }
        let (wal_root, log_store) = fresh_journal_store();
        let node_gate = Arc::new(
            GroupRejoin::durable(
                node_id,
                ursula_shard::RaftTarget::Data(placement().raft_group_id),
                &log_store,
            )
            .await
            .expect("open the gate"),
        );
        let engine = RaftGroupEngine::new_node_with_log_store_network_and_options(
            placement(),
            node_id,
            config.clone(),
            network,
            log_store,
            RaftGroupEngineOptions {
                peer_cluster: Some(crate::peer_cluster::test_peer_cluster_with(
                    node_id,
                    peers(),
                )),
                ..RaftGroupEngineOptions::gated(node_gate.clone())
            },
        )
        .await
        .expect("create cluster raft group node");
        if gated == Some(node_id) {
            // Bound before the node is reachable, so it never campaigns.
            engine
                .publish_recovery(node_gate.clone(), &RaftGroupHandleRegistry::default())
                .expect("bind the gate");
            gate = Some(node_gate);
        }
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
        let answer = registry
            .confirm_recovery_barrier(
                leader_id,
                crate::RaftTarget::Data(placement().raft_group_id),
            )
            .await
            .expect("selected leader confirms a fresh quorum before fixture writes");
        assert_eq!(*answer.vote.leader_id().node_id(), leader_id);
        leader_id
    })
    .await
    .expect("election and quorum readiness share the existing five-second budget");
    (registry, engines, leader_id, wal_roots, gate)
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
    command: RaftCommand,
) -> <UrsulaRaftTypeConfig as openraft::RaftTypeConfig>::Entry {
    Entry::new(log_id(index), EntryPayload::Normal(command))
}

fn create_stream_command(name: &str) -> RaftCommand {
    create_command(bsid(name))
}

#[test]
fn raft_group_command_round_trips_through_wire_codec() {
    let command = RaftCommand::Stream(ursula_stream::StreamCommand::Append {
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
    let decoded: RaftCommand = decode_wire(&encoded, "group command").expect("decode wire command");
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

    match write_result_from_raft_response(
        RaftGroupResponse::Write(Ok(decoded)),
        ResponseKind::CreateStream,
    )
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

    raft.initialize(BTreeSet::from([1]))
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
        write_result_from_raft_response(created.data, ResponseKind::CreateStream)
            .expect("decode create response"),
        Ok(GroupWriteResponse::CreateStream(_))
    ));

    let appended = raft
        .client_write(append_command(stream_id, b"payload"))
        .await
        .expect("append through openraft");
    match write_result_from_raft_response(appended.data, ResponseKind::Append)
        .expect("decode append response")
    {
        Ok(GroupWriteResponse::Append(response)) => {
            assert_eq!(response.start_offset, 0);
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
    let owner =
        crate::owner::OwnerRaftHandle::new(raft.clone(), ursula_runtime::NodeFence::unleased());
    // 2 x 700 ms + 300 ms.
    assert_eq!(owner.handoff_settle_time(), Duration::from_millis(1_700));
    raft.shutdown().await.expect("shutdown raft group");
}

/// The recovery barrier asks the group's placed voters and the members
/// of its effective membership, by node id, never itself.
#[cfg(not(madsim))]
#[tokio::test]
async fn the_recovery_barrier_asks_configured_voters_and_members_by_id() {
    let (_registry, engines, leader_id, _wal_roots, gate) =
        build_three_node_cluster_with_a_gated_node("barrier-candidates").await;
    // The leader's view names members 1 to 3; configuration names node 4
    // only. The gated node's gate keeps the driver asking every candidate.
    let leader = &engines[usize::try_from(leader_id).unwrap() - 1];
    let asked = Arc::new(std::sync::Mutex::new(BTreeSet::new()));
    let (rounds, mut seen) = tokio::sync::watch::channel(0_usize);
    let rounds = Arc::new(rounds);
    let recorded = asked.clone();
    let barrier = tokio::spawn(crate::run_rejoin_vote_barrier(
        leader.read_barrier.owner().clone(),
        gate,
        RaftGroupHandleRegistry::default().election_policy(),
        Arc::new(crate::FixedNodes::from_voters(BTreeSet::from([4]))),
        move |candidate| {
            let recorded = recorded.clone();
            let rounds = rounds.clone();
            async move {
                let count = {
                    let mut asked = recorded.lock().unwrap();
                    asked.insert(candidate);
                    asked.len()
                };
                rounds.send_replace(count);
                Err::<crate::BarrierAnswer, _>("refused")
            }
        },
        Duration::from_millis(100),
        Duration::from_millis(10),
        Duration::from_secs(60),
    ));
    tokio::time::timeout(Duration::from_secs(5), seen.wait_for(|count| *count >= 3))
        .await
        .expect("the driver asks every candidate")
        .expect("the driver runs");
    barrier.abort();
    let expected = (1..=4)
        .filter(|node_id| *node_id != leader_id)
        .collect::<BTreeSet<_>>();
    assert_eq!(*asked.lock().unwrap(), expected);
    shutdown_all(&engines).await;
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
    match write_result_from_raft_response(appended.data, ResponseKind::Append)
        .expect("decode append response")
    {
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

/// A write that reaches a new leader before its first follower answered is
/// not refused: OpenRaft refuses proposals until a quorum acknowledged the
/// leader, and the engine waits for that acknowledgement before it proposes.
/// Every RPC is held, so the write lands inside that window.
#[cfg(not(madsim))]
#[tokio::test]
async fn a_write_right_after_an_election_is_not_refused() {
    use openraft::rt::WatchReceiver;

    let policy = InProcessRaftNetworkPolicy::default();
    let (_registry, engines, old_leader, _wal_roots) =
        build_three_node_cluster("ursula-write-after-election-test", Some(policy.clone())).await;
    let old_index = usize::try_from(old_leader - 1).expect("leader id fits usize");
    let stream_id = bsid("write-after-election");
    create_stream_via_raft(&engines[old_index], stream_id.clone()).await;
    // Shorter than the wait (election_timeout_min, 500 ms) and the vote
    // deadline, longer than it takes to observe the election.
    policy.set_delay(Some(Duration::from_millis(300)));
    for node_id in (1..=3).filter(|node_id| *node_id != old_leader) {
        policy.partition_bidirectional(old_leader, node_id);
    }
    // Each survivor's own metrics: the leader sees its leadership before
    // its first append reaches a follower.
    let new_index = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(index) = (0..3).filter(|index| *index != old_index).find(|index| {
                engines[*index].raft.metrics().borrow_watched().state
                    == openraft::ServerState::Leader
            }) {
                break index;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the survivors elect a new leader");
    assert!(
        engines[new_index]
            .raft
            .metrics()
            .borrow_watched()
            .last_quorum_acked
            .is_none(),
        "the write lands before the new leader's first quorum acknowledgement"
    );
    let written = engines[new_index]
        .write(append_stream_command(stream_id, b"after-election"))
        .await;
    assert!(
        matches!(written, Ok(GroupWriteResponse::Append(_))),
        "a write right after the election: {written:?}"
    );
    policy.set_delay(None);
    shutdown_all(&engines).await;
}

/// A leader that no quorum acknowledged within its lease refuses a write
/// before it appends it: the answer is `NotProposed`, and the leader's log
/// does not grow. Staging cleanup and the cold-page release delete objects on
/// that answer, so an entry appended behind it would lose data.
#[cfg(not(madsim))]
#[tokio::test]
async fn a_leader_past_its_lease_refuses_a_write_before_appending_it() {
    use openraft::rt::WatchReceiver;

    let policy = InProcessRaftNetworkPolicy::default();
    let (_registry, engines, leader, _wal_roots) =
        build_three_node_cluster("ursula-past-lease-write-test", Some(policy.clone())).await;
    let index = usize::try_from(leader - 1).expect("leader id fits usize");
    let engine = &engines[index];
    let stream_id = bsid("past-lease-write");
    create_stream_via_raft(engine, stream_id.clone()).await;
    for node_id in (1..=3).filter(|node_id| *node_id != leader) {
        policy.partition_bidirectional(leader, node_id);
    }
    let lease = Duration::from_millis(engine.raft.config().election_timeout_max);
    tokio::time::sleep(lease + Duration::from_millis(200)).await;
    let before = engine.raft.metrics().borrow_watched().clone();
    assert_eq!(
        before.state,
        openraft::ServerState::Leader,
        "the cut-off leader still believes it leads"
    );
    let refused = engine
        .write(append_stream_command(stream_id, b"past-lease"))
        .await
        .expect_err("a leader past its lease refuses the write");
    assert!(
        matches!(
            refused.not_leader(),
            Some(ursula_runtime::LeadershipRefusal {
                proposal: ursula_runtime::Proposal::NotProposed,
                reason: LeadershipReason::QuorumUnconfirmed,
                ..
            })
        ),
        "{refused:?}"
    );
    let after = engine.raft.metrics().borrow_watched().clone();
    assert_eq!(
        after.last_log_index, before.last_log_index,
        "nothing was appended"
    );
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
    // A new leader proposes only once a quorum acknowledged it.
    engines[new_index]
        .raft
        .wait(Some(Duration::from_secs(5)))
        .leader_with_quorum_acked(None, "the new leader's first quorum acknowledgement")
        .await
        .expect("the new leader holds a quorum acknowledgement");
    let acked = engines[new_index]
        .raft
        .client_write(append_command(stream_id.clone(), b"after"))
        .await
        .expect("append acknowledged by the new leader");
    match write_result_from_raft_response(acked.data, ResponseKind::Append)
        .expect("decode append response")
    {
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

/// A leadership refusal raised before anything was proposed.
#[cfg(not(madsim))]
fn refused_before_proposal(error: &GroupEngineError) -> bool {
    matches!(
        error.not_leader(),
        Some(LeadershipRefusal {
            proposal: Proposal::NotProposed,
            ..
        })
    )
}

/// The group's leader waits for its history event (#581 section 4.5).
#[cfg(not(madsim))]
fn is_history_pending(error: &GroupEngineError) -> bool {
    matches!(error.infra(), Some(GroupInfraError::RecoveryPending { .. }))
        || error.code() == Some(StreamErrorCode::HistoryPending)
}

#[track_caller]
#[cfg(not(madsim))]
fn assert_refused<T>(what: &str, result: Result<T, GroupEngineError>) {
    match result {
        Ok(_) => panic!("{what}: the deposed leader served its stale view"),
        Err(err) => {
            assert!(
                err.not_leader().is_some(),
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

/// A ReadIndex round answers with its full read log id, and that id names
/// the leadership the round confirmed: the leader's committed vote. A
/// replica that does not lead confirms no round.
#[cfg(not(madsim))]
#[tokio::test]
async fn the_read_index_barrier_names_the_leadership_of_its_round() {
    use openraft::rt::WatchReceiver;

    let (_registry, engines, leader, _wal_roots) =
        build_three_node_cluster("ursula-read-log-id-test", None).await;
    let leader_index = usize::try_from(leader - 1).expect("leader id fits usize");
    let engine = &engines[leader_index];
    create_stream_via_raft(engine, bsid("read-log-id")).await;
    let vote = engine.raft.metrics().borrow_watched().vote;
    assert!(vote.is_committed());

    let read_log_id = engine
        .read_barrier
        .confirm_read_log_id()
        .await
        .expect("a round with a live quorum")
        .expect("the leader confirms its round");
    assert_eq!(
        *read_log_id.committed_leader_id(),
        vote.leader_id().to_committed()
    );
    let after = engine.raft.metrics().borrow_watched().clone();
    assert_eq!(after.vote, vote, "the leadership did not change meanwhile");
    assert!(
        after
            .last_applied
            .is_some_and(|applied| applied.index() >= read_log_id.index()),
        "the round answers once the leader applied its read log id"
    );

    let follower = &engines[usize::from(leader_index == 0)];
    assert_eq!(
        follower
            .read_barrier
            .confirm_read_log_id()
            .await
            .expect("a follower's round"),
        None
    );
    shutdown_all(&engines).await;
}

/// A ReadIndex round needs a quorum that answers after the read arrives. A
/// leader cut off right after its quorum confirmed it still holds that
/// acknowledgement within the leader lease, so a lease read answers from
/// it. The round must not: it confirms with heartbeats of its own, which no
/// quorum answers, and the read is refused as leader-unavailable.
#[cfg(not(madsim))]
#[tokio::test]
async fn a_read_index_round_needs_a_quorum_after_the_read_arrives() {
    use openraft::Instant;
    use openraft::rt::WatchReceiver;

    let policy = InProcessRaftNetworkPolicy::default();
    // The cluster's leader has confirmed a quorum (`confirm_recovery_barrier`)
    // and keeps it fresh with heartbeats until the partition.
    let (_registry, engines, leader, _wal_roots) =
        build_three_node_cluster("ursula-fresh-read-index-test", Some(policy.clone())).await;
    let engine = &engines[usize::try_from(leader - 1).expect("leader id fits usize")];
    for node_id in (1..=3).filter(|node_id| *node_id != leader) {
        policy.partition_bidirectional(leader, node_id);
    }

    // Right after the partition the leader still holds that
    // acknowledgement within the lease, so a lease read answers from it.
    let lease = Duration::from_millis(engine.raft.config().election_timeout_max);
    assert!(
        engine
            .raft
            .metrics()
            .borrow_watched()
            .last_quorum_acked
            .as_ref()
            .is_some_and(|acked| acked.elapsed() < lease),
        "the acknowledgement from before the partition is within the lease"
    );
    let lease_read = engine
        .raft
        .get_read_linearizer(openraft::ReadPolicy::LeaseRead)
        .await
        .expect("a lease read answers from that acknowledgement");
    assert!(
        lease_read.read_log_id().index() > 0,
        "the lease read confirmed a read log id"
    );

    let refused = engine
        .read_barrier
        .confirm_read_log_id()
        .await
        .expect_err("no quorum answers the round");
    assert!(
        matches!(
            refused.not_leader(),
            Some(ursula_runtime::LeadershipRefusal {
                leader: None,
                proposal: ursula_runtime::Proposal::NotProposed,
                reason: ursula_runtime::LeadershipReason::QuorumUnconfirmed,
            })
        ),
        "expected leader-unavailable, got {refused:?}"
    );
    assert!(engine.raft.is_leader(), "only the round refuses the read");
    shutdown_all(&engines).await;
}

/// The engine's one leadership check, which runs right before commands go to
/// OpenRaft, reports its forward as raised before proposal: on its own,
/// inside `write`, and inside a batch, for every entry. OpenRaft's own
/// forward of a write is raised before it appends too, and only an entry it
/// appended and then discarded on losing leadership stays ambiguous, since it
/// may still commit
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
        error
            .not_leader()
            .and_then(|refusal| refusal.leader.as_ref())
            .map(|leader| leader.node_id)
            == Some(leader_id)
    };

    let checked = follower
        .forward_write_to_leader_if_follower(append_stream_command(stream_id.clone(), b"a"))
        .await
        .expect_err("the engine's check refuses a write on a follower");
    assert!(refused_before_proposal(&checked), "{checked:?}");
    assert!(names_the_leader(&checked), "{checked:?}");

    let written = follower
        .write(append_stream_command(stream_id.clone(), b"a"))
        .await
        .expect_err("the engine's check refuses a write on a follower");
    assert!(refused_before_proposal(&written), "{written:?}");
    assert!(names_the_leader(&written), "{written:?}");

    let batch = crate::forward::write_commands_on_raft(
        follower.raft_handle(),
        follower.fence(),
        follower.contact(),
        &follower.history,
        vec![
            append_command(stream_id.clone(), b"a"),
            append_command(stream_id, b"b"),
        ],
    )
    .await
    .expect("the batch is answered entry by entry");
    assert_eq!(batch.len(), 2);
    for entry in batch {
        let error = entry.expect_err("the engine's check refuses each batch entry on a follower");
        assert!(refused_before_proposal(&error), "{error:?}");
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
        // An upload is fenced to the leadership the engine leads in when the
        // write starts, which its deposition ends only after the append.
        let upload = ursula_runtime::ProposedUpload::new(
            engine
                .upload_fence()
                .expect("the engine leads when the write starts"),
        );
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
                            0,
                            upload.clone(),
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
                            message_count: 0,
                            close_after: false,
                            stream_seq: None,
                            producer: None,
                            now_ms: 0,
                            if_incarnation: None,
                            upload: upload.clone(),
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
/// replaces the entries and OpenRaft answers the write with a forward to it,
/// unless the leader's lease ended first and the engine answered that its
/// leadership lapsed. Either answer says the write may have committed.
/// Had a follower stored an entry first, the new leader could have committed
/// it instead, and the deposed node cannot tell the two apart. That node's
/// own leadership check now refuses before proposal, so running the check
/// again after the hand-off, or reporting OpenRaft's forward as raised
/// before proposal, would treat a write that may have committed as never
/// proposed. The HTTP layer deletes a staged external payload exactly after
/// the engine's errors that [`GroupEngineError::refused_before_proposal`]
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
            assert!(!error.refused_before_proposal(), "{write:?}: {error:?}");
            let refusal = error
                .not_leader()
                .unwrap_or_else(|| panic!("{write:?}: expected a leadership refusal: {error:?}"));
            assert_eq!(refusal.proposal, Proposal::MayHaveCommitted, "{write:?}");
            // OpenRaft's forward once the new leader's log replaced the
            // entries, or the engine's own answer once the cut-off leader's
            // lease ended first.
            assert!(
                matches!(
                    refusal.reason,
                    LeadershipReason::SteppedDown | LeadershipReason::QuorumUnconfirmed
                ),
                "{write:?}: {error:?}"
            );
            // OpenRaft names the new leader; the refusal carries it, with the
            // term it leads in, once this node's view knows that term, and
            // names no leader before.
            if let Some(leader) = &refusal.leader {
                assert_eq!(leader.node_id, new_leader, "{write:?}: {error:?}");
                assert!(leader.term > 0, "{write:?}: {error:?}");
            }
        }
        // The deposed node now follows, so its own check refuses before
        // proposal. Running it after the hand-off would misclassify the
        // write. A write its lapse answered can end before the node hears
        // of the new leader. OpenRaft may name the new leader from a higher
        // vote in an answer before the new leader's first append reaches
        // the node, and only that append makes it follow the new leader.
        let deposed = engines[index(leader_id)].read_barrier.owner();
        tokio::time::timeout(Duration::from_secs(10), async {
            while deposed.leadership().role
                != (crate::LeaderRole::Follows {
                    leader_id: new_leader,
                })
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_elapsed| panic!("{write:?}: the deposed node follows the new leader"));
        let checked = engines[index(leader_id)]
            .forward_write_to_leader_if_follower(append_stream_command(stream_id.clone(), b"abc"))
            .await
            .expect_err("the deposed node follows the new leader");
        assert!(refused_before_proposal(&checked), "{write:?}: {checked:?}");
        // The refusal names the new leader with the term this node knows it
        // in, from one view, so the front door can redirect typed.
        let leader = checked
            .not_leader()
            .and_then(|refusal| refusal.leader.as_ref())
            .expect("a known leader");
        assert_eq!(leader.node_id, new_leader, "{write:?}: {checked:?}");
        assert!(leader.term > 0, "{write:?}: {checked:?}");
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
        .client_write(RaftCommand::Stream(StreamCommand::from(append)))
        .await
        .expect("append through the leader");
    let index = appended.log_id.index();
    let incarnation = match write_result_from_raft_response(appended.data, ResponseKind::Append)
        .expect("decode")
    {
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
                    early_answer: Some(ProducerEarlyAnswer::Repeat(_)),
                    ..
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
            match write_result_from_raft_response(appended.data, ResponseKind::Append)
                .expect("decode append response")
            {
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
    match write_result_from_raft_response(appended.data, ResponseKind::Append)
        .expect("decode append response")
    {
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
        write_result_from_raft_response(created.data, ResponseKind::CreateStream)
            .expect("decode create response"),
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
            match write_result_from_raft_response(appended.data, ResponseKind::Append)
                .expect("decode append response")
            {
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
    let nodes = BTreeSet::from([1, 2, 3]);

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
                let store = sim_journal_store(&format!("raft-node-{node_id}"));
                let gate = gate_of(node_id, &store).await;
                let engine = RaftGroupEngine::new_node_with_log_store_and_network(
                    placement(),
                    node_id,
                    config_for_node,
                    InProcessRaftNetworkFactory::new(registry_for_node)
                        .with_source(node_id)
                        .with_policy(policy_for_node),
                    store,
                    gate,
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
        let gate = gate_of(node_id, &log_store).await;
        let engine = RaftGroupEngine::new_node_with_log_store_and_network(
            placement(),
            node_id,
            config.clone(),
            InProcessRaftNetworkFactory::new(registry.clone()).with_source(node_id),
            log_store,
            gate,
        )
        .await
        .expect("create cluster raft group node");
        if node_id != 3 {
            registry.register(node_id, &engine);
        }
        engines.push(engine);
    }

    engines[0]
        .raft
        .initialize(BTreeSet::from([1, 2]))
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
        write_result_from_raft_response(appended.data, ResponseKind::Append)
            .expect("decode append response"),
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
            .add_learner(3, EmptyNode {}, true),
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
    let (_wal_root, wal) = fresh_wal();
    let mut engine = single_voter(&wal, placement()).await;
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
    let (_wal_root, wal) = fresh_wal();
    let mut engine = single_voter(&wal, placement()).await;
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

    {
        let mut engine = single_voter(&start_wal(root.path(), 1, 1), placement()).await;
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

    let mut recovered = single_voter(&start_wal(root.path(), 1, 1), placement()).await;
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
    let stream_id = bsid("genesis");
    let genesis = {
        let mut engine = single_voter(&start_wal(root.path(), 1, 1), placement()).await;
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
    let recovered = single_voter(&start_wal(root.path(), 1, 1), placement()).await;
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
    let (_root, wal) = fresh_wal();
    let mut engine = single_voter(&wal, placement()).await;
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
        matches!(&before, Err(error) if is_history_pending(error)),
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
    assert!(is_history_pending(&write), "{write:?}");
    assert!(write.refused_before_proposal());
    let read = engine
        .read_stream(read_req(stream_id.clone(), 16), placement())
        .await
        .expect_err("a fenced leader serves no read");
    assert!(is_history_pending(&read), "{read:?}");
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
        single_node_factory(&start_wal(wal_root.path(), 1, 1)),
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

/// The single-node factory hands its engines the WAL's fence, so the one
/// leader predicate stops a fenced single node's leader-gated jobs as it
/// does a cluster node's. Catches an engine whose leadership reads an
/// unleased fence because its factory gave it no peer cluster.
#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_single_node_engine_leads_nothing_once_its_wal_fence_closes() {
    let wal_root = tempfile::tempdir().expect("WAL root");
    let registry = RaftGroupHandleRegistry::default();
    let wal = RaftWal::start_in_cluster(
        wal_root.path(),
        crate::JournalTuning::new(WalFsync::Never),
        &ursula_shard::StaticShardMap::new(1, 1).expect("valid topology"),
        ursula_proto::placement::NodeId::new(1),
        None,
        ursula_runtime::NodeFence::leased(Duration::from_secs(5)),
    )
    .expect("start the Raft WAL");
    // The fresh root's data replicas are held until meta answers its
    // registration. No meta runs here, so settle it as a forming
    // cluster's answer does.
    wal.fence().bind(false);
    let factory = PlacedRaftGroupEngineFactory::single_node(wal.clone(), registry.clone(), 1);
    let runtime = ShardRuntime::spawn_with_engine_factory(hosted_config(1, 1), factory)
        .expect("spawn runtime with raft group engine factory");
    let stream_id = bsid("fenced-single-node");
    runtime
        .create_stream(create_req(stream_id.clone()))
        .await
        .expect("create");
    let raft_group_id = runtime.locate(&stream_id).raft_group_id;
    let owner = registry
        .entry(raft_group_id)
        .expect("the group's engine is registered")
        .raft()
        .clone();
    assert!(owner.is_leader(), "the single node leads its group");
    assert!(owner.leadership().upload_fence().is_some());
    wal.fence().close();
    assert!(!owner.is_leader(), "a fenced single node leads nothing");
    assert_eq!(owner.leadership().upload_fence(), None);
}

/// A restart whose old engine's OpenRaft work does not end keeps the group
/// closed (#709): the shutdown fails typed, a request that would start the
/// group again is refused at once, typed, while that work runs, and the
/// group starts on the first request after it ended, with what its log
/// store held.
#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_keeps_the_group_closed_while_its_old_work_runs() {
    let wal_root = tempfile::tempdir().expect("WAL root");
    let registry = RaftGroupHandleRegistry::default();
    let wal = start_wal(wal_root.path(), 1, 1);
    let mut factory = PlacedRaftGroupEngineFactory::single_node(wal.clone(), registry.clone(), 1);
    factory.work_stop_timeout = Some(Duration::from_millis(200));
    let runtime = ShardRuntime::spawn_with_engine_factory(hosted_config(1, 1), factory)
        .expect("spawn runtime with raft group engine factory");
    let stream_id = bsid("held-restart");
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
    let owner = registry
        .entry(raft_group_id)
        .expect("the group's engine is registered")
        .raft()
        .clone();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let held = tokio::spawn(async move {
        let _answered = owner
            .with_state_machine(move |_state_machine| {
                Box::pin(async move {
                    entered_tx.send(()).expect("the test waits for the worker");
                    released.await.expect("the test releases the worker");
                })
            })
            .await;
    });
    entered_rx
        .await
        .expect("the worker runs the external function");

    let restarted = runtime.restart_group_engine(raft_group_id).await;
    assert!(
        matches!(
            &restarted,
            Err(ursula_runtime::RuntimeError::GroupEngine {
                error: GroupEngineError::Infra(GroupInfraError::EngineWorkOutlivedShutdown { .. }),
                ..
            })
        ),
        "{restarted:?}"
    );
    let refused = runtime.read_stream(read_req(stream_id.clone(), 16)).await;
    assert!(
        matches!(
            &refused,
            Err(ursula_runtime::RuntimeError::GroupEngine {
                error: GroupEngineError::Infra(GroupInfraError::PreviousEngineWorkRunning {
                    group: RaftTarget::Data(refused_group),
                }),
                ..
            }) if *refused_group == raft_group_id
        ),
        "{refused:?}"
    );
    release.send(()).expect("the worker is still held");
    held.await.unwrap();
    // The group opens on the first request once the old work ended.
    wal.engine_work_ended(raft_group_id).await;
    let read = runtime
        .read_stream(read_req(stream_id, 16))
        .await
        .expect("the group starts once its old work ended");
    assert_eq!(read.payload, b"kept");
}

/// An engine whose start fails once its Raft runs keeps its group closed
/// until its work ended (#709): the WAL refuses to open the
/// group while the failed engine's worker still runs, and opens it once the
/// stopping task the WAL keeps finished.
#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_start_that_fails_after_its_raft_runs_keeps_the_group_closed() {
    let root = tempfile::tempdir().unwrap();
    let wal = start_wal(root.path(), 1, 1);
    let metrics = || ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics();
    let release = Arc::new(crate::rt::sync::Notify::new());
    let mut factory = single_node_factory(&wal);
    factory.startup_fault = Some(StartupFault {
        release: Arc::clone(&release),
    });
    let started = factory.open(placement(), metrics()).await;
    assert!(matches!(
        started,
        Err(GroupEngineError::Infra(
            GroupInfraError::LeaderWaitTimedOut { timeout_ms: 0, .. }
        ))
    ));
    assert!(matches!(
        wal.open(placement(), ursula_control::Admission::Create, metrics()),
        Err(crate::log_store::RaftWalError::PreviousEngineRunning { placement: refused })
            if refused == placement()
    ));

    release.notify_one();
    wal.engine_work_ended(placement().raft_group_id).await;
    let store = wal
        .open(placement(), ursula_control::Admission::Create, metrics())
        .expect("the group opens once the failed start's work ended");
    drop(store);
    wal.shutdown().await.unwrap();
}

/// An engine dropped before its shutdown, as by a group actor that
/// panicked while the registry still holds its Raft, stops in the
/// background (#709): its group stays closed while the engine's work runs,
/// and opens once that work ended, though a handle on its Raft outlives the
/// engine.
#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_engine_dropped_before_its_shutdown_stops_in_the_background() {
    let root = tempfile::tempdir().unwrap();
    let wal = start_wal(root.path(), 1, 1);
    let metrics = || ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics();
    let engine = single_voter(&wal, placement()).await;
    // What the registry keeps of a registered engine.
    let kept = engine.raft_handle();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let worker = engine.raft_handle();
    let held = tokio::spawn(async move {
        let _answered = worker
            .with_state_machine(move |_state_machine| {
                Box::pin(async move {
                    entered_tx.send(()).expect("the test waits for the worker");
                    released.await.expect("the test releases the worker");
                })
            })
            .await;
    });
    entered_rx
        .await
        .expect("the worker runs the external function");

    drop(engine);
    assert!(matches!(
        wal.open(placement(), ursula_control::Admission::Create, metrics()),
        Err(crate::log_store::RaftWalError::PreviousEngineRunning { placement: refused })
            if refused == placement()
    ));

    release.send(()).expect("the worker is still held");
    held.await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        wal.engine_work_ended(placement().raft_group_id),
    )
    .await
    .expect("the dropped engine's work ends");
    let store = wal
        .open(placement(), ursula_control::Admission::Create, metrics())
        .expect("the group opens once the dropped engine's work ended");
    drop(store);
    drop(kept);
    wal.shutdown().await.unwrap();
}

#[cfg(not(madsim))]
#[tokio::test]
async fn shard_runtime_uses_raft_group_engine_factory_for_owned_group() {
    let wal_root = tempfile::tempdir().expect("WAL root");
    let config = hosted_config(1, 1);
    let runtime = ShardRuntime::spawn_with_engine_factory(
        config,
        single_node_factory(&start_wal(wal_root.path(), 1, 1)),
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
        PlacedRaftGroupEngineFactory::single_node(
            start_wal(wal_root.path(), 2, 4),
            registry.clone(),
            1,
        ),
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
        PlacedRaftGroupEngineFactory::single_node(
            start_wal(wal_root.path(), 1, 1),
            registry.clone(),
            1,
        ),
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
    entry.core.history.set_plan(Some(RecoveryRecord {
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
        single_node_factory(&start_wal(&root, 1, 1)),
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
        let runtime =
            ShardRuntime::spawn_with_engine_factory(config, single_node_factory(&log_stores))
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
        let recovered =
            ShardRuntime::spawn_with_engine_factory(config, single_node_factory(&log_stores))
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
    assert_eq!(snapshot.usage.streams.len(), 1);
    assert_eq!(snapshot.usage.streams[0].counters.appends, 1);
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
        RaftCommand::Stream(ursula_stream::StreamCommand::DeleteStream {
            stream_id: deleted,
            now_ms: 0,
        }),
    )])
    .await;
    let deleted_bytes = overhead.saturating_add(4);
    assert_eq!(owed(&sm), deleted_bytes);
    apply_entries(&mut sm, vec![normal_entry(
        9,
        RaftCommand::Stream(ursula_stream::StreamCommand::PurgeBucket {
            bucket_id: "removal-purged".to_owned(),
            now_ms: 9,
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
        .apply_committed_write(
            append_stream_command(bsid("raft-snapshot"), b"-next"),
            placement(),
        )
        .expect("append after install");
    match appended {
        GroupWriteResponse::Append(response) => {
            assert_eq!(response.start_offset, 7);
        }
        other => panic!("unexpected append response: {other:?}"),
    }
    // The installed version kept its usage, and counted this append on top
    // of it, under the next seq (F20).
    let source_usage = source.group_snapshot().await.expect("snapshot").usage;
    let installed = target.group_snapshot().await.expect("snapshot").usage;
    assert_eq!(installed.genesis, source_usage.genesis);
    assert_eq!(installed.streams.len(), 1);
    assert_eq!(installed.streams[0].counters.appends, 2);
    assert_eq!(installed.streams[0].seq, source_usage.usage_seq + 1);
    assert_eq!(
        installed.streams[0].counted_since,
        source_usage.streams[0].counted_since
    );
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
        let wire = crate::forward::read_stream_read_v1(&request).encode_to_vec();
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
async fn registry_handoff_rejects_invalid_targets_and_transfers_to_a_healthy_voter() {
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
    registry.register_engine(engine);
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

/// Every replica runs behind its recovery gate, so with OpenRaft's log
/// reversion allowed: its leader rewinds a follower that lost entries in
/// place. The config the engine was given is left as it was.
#[cfg(not(madsim))]
#[tokio::test]
async fn every_replica_allows_follower_log_reversion() {
    let config = raft_config("log-reversion", 30, 60);
    let (_root, store) = fresh_journal_store();
    let options = RaftGroupEngineOptions::gated_on(1, placement(), &store).await;
    let engine = RaftGroupEngine::new_node(
        placement(),
        1,
        config.clone(),
        SingleNodeRaftNetworkFactory,
        store,
        options,
    )
    .await
    .expect("start the replica");
    assert_eq!(
        engine.raft_handle().config().allow_log_reversion,
        Some(true)
    );
    engine.shutdown().await.expect("stop the replica");
    assert_eq!(
        config.allow_log_reversion, None,
        "the shared config is unchanged"
    );
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
            // A new leader proposes only once a quorum acknowledged it.
            |metrics| {
                metrics.state == openraft::ServerState::Leader
                    && metrics.vote.leader_id().term() > vote.leader_id().term()
                    && metrics.last_quorum_acked.is_some()
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
/// cannot refuse it. One whose last log id is behind the leader's log is
/// refused by the leader's own vote.
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

    // A step-down whose last log id is behind the leader's log, as when an
    // append lands between the read and the vote, is refused by the vote it
    // was made from: what `StepDownVote::LogAdvanced` reads.
    assert!(last_log_id.is_some(), "the fresh leader has a log");
    let behind = raft
        .vote(crate::owner::transfer_step_down(vote, target, None))
        .await
        .unwrap();
    assert!(
        !behind.vote_granted,
        "a step-down behind the log is refused"
    );
    assert_eq!(behind.vote, vote);
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
/// passes. A leader's handoff also needs the replica to have acknowledged
/// a leader of its group in this run, which a self-election does not.
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
        policy.validate_incoming_transfer(
            group,
            node,
            &transfer(leader_vote, node),
            None,
            TransferAsk::Handoff,
        ),
        Ok(())
    ));
    assert!(matches!(
        policy.validate_incoming_transfer(
            group,
            node,
            &transfer(candidacy, node),
            None,
            TransferAsk::Handoff,
        ),
        Ok(())
    ));
    assert!(matches!(
        policy.validate_incoming_transfer(
            group,
            node,
            &transfer(own_vote, node),
            None,
            TransferAsk::Handoff,
        ),
        Err(IncomingTransferError::OwnCommittedVote { vote, .. }) if vote == own_vote
    ));

    let (_root, store) = fresh_journal_store();
    let gate = GroupRejoin::durable(node, ursula_shard::RaftTarget::Data(group), &store)
        .await
        .unwrap();
    assert!(!gate.vote_gate_open());
    assert!(matches!(
        policy.validate_incoming_transfer(
            group,
            node,
            &transfer(leader_vote, node),
            Some(&gate),
            TransferAsk::Handoff,
        ),
        Err(IncomingTransferError::MayNotCampaign { .. })
    ));
    let registry = RaftGroupHandleRegistry::default();
    registry.mark_leadership_shed(LeadershipShedReason::MaintenanceDrain);
    let shed = registry.election_policy();
    assert!(matches!(
        shed.validate_incoming_transfer(
            group,
            node,
            &transfer(leader_vote, node),
            None,
            TransferAsk::Handoff,
        ),
        Err(IncomingTransferError::MayNotCampaign { .. })
    ));
    assert!(matches!(
        shed.validate_incoming_transfer(
            group,
            node,
            &transfer(leader_vote, 3),
            Some(&gate),
            TransferAsk::Handoff,
        ),
        Ok(())
    ));

    // A replica whose gate is open but that acknowledged no leader in this
    // run may campaign: it takes a self-election, and no handoff.
    let (_open_root, open_store) = fresh_journal_store();
    let open = GroupRejoin::durable(node, ursula_shard::RaftTarget::Data(group), &open_store)
        .await
        .unwrap();
    open.allow_fresh_bootstrap().await.unwrap();
    assert!(open.may_campaign());
    assert!(!policy.may_take_leadership(Some(&open)));
    assert!(matches!(
        policy.validate_incoming_transfer(
            group,
            node,
            &transfer(leader_vote, node),
            Some(&open),
            TransferAsk::Handoff,
        ),
        Err(IncomingTransferError::LeaderNotAcknowledged { .. })
    ));
    assert!(matches!(
        policy.validate_incoming_transfer(
            group,
            node,
            &transfer(leader_vote, node),
            Some(&open),
            TransferAsk::SelfElection,
        ),
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
    let (_network, engines, leader, _roots, gate) =
        build_three_node_cluster_with_a_gated_node("self-election").await;
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
    let gated = GATED_NODE;
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
    let (network, engines, leader, _roots, gate) =
        build_three_node_cluster_with_a_gated_node("handoff-gated-target").await;
    let group = placement().raft_group_id;
    let engine = |id: u64| {
        engines
            .iter()
            .find(|engine| engine.raft.metrics().borrow_watched().id == id)
            .unwrap()
    };
    let gated = GATED_NODE;
    let leader_raft = engine(leader).raft_handle();
    let vote = leader_raft.metrics().borrow_watched().vote;

    // The gate keeps the target out of elections. Its vote floor lets it
    // take the leader's appends, so the leader's handoff policy accepts it
    // as a target once it caught up.
    gate.establish_vote_floor(vote).await.unwrap();
    assert!(!gate.vote_gate_open());
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
            let round = self.barrier.confirm_read_log_id();
            Box::pin(async move {
                let outcome = round.await?.map(|read_log_id| read_log_id.index());
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
    let (_root, wal) = fresh_wal();
    let engine = single_voter(&wal, placement()).await;
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

/// The engine factory hands each group its snapshot record from the
/// group's log store, so a record it publishes follows a journal sync (R1).
#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_engine_factory_wires_the_snapshot_record_to_the_log_store() {
    let wal_root = tempfile::tempdir().unwrap();
    let wal = start_wal(wal_root.path(), 1, 1);
    let expected = wal.snapshot_metadata_path(placement());
    let registry = RaftGroupHandleRegistry::default();
    let runtime = ShardRuntime::spawn_with_engine_factory(
        RuntimeConfig::new(1, 1),
        PlacedRaftGroupEngineFactory::single_node(wal, registry.clone(), 1),
    )
    .unwrap();
    runtime.warm_group(RaftGroupId(0)).await.unwrap();
    let path = registry
        .get(RaftGroupId(0))
        .unwrap()
        .with_state_machine(|state_machine| {
            Box::pin(async move {
                state_machine
                    .snapshot_record
                    .store()
                    .map(|record| record.path().to_path_buf())
            })
        })
        .await
        .unwrap();
    assert_eq!(path, Some(expected));
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
        PlacedRaftGroupEngineFactory::single_node(
            start_wal(wal_root.path(), 1, 2),
            registry.clone(),
            1,
        ),
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
    let factory = single_node_factory(&start_wal(dir.path(), 1, 1));
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
                    .write(append_stream_command(stream.clone(), b"committed-poison"))
                    .await
                    .unwrap_err();
                assert!(matches!(
                    error,
                    GroupEngineError::Infra(ursula_runtime::GroupInfraError::OutcomeUnknown)
                ));
                let refused = leader
                    .write(append_stream_command(stream.clone(), b"after-stop"))
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
                    .write(append_stream_command(stream, b"after-re-election"))
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
fn expiring_create(stream_id: ursula_shard::BucketStreamId, expires_at_ms: u64) -> RaftCommand {
    let mut request = create_req(stream_id);
    request.stream_expires_at_ms = Some(expires_at_ms);
    request.now_ms = 1;
    RaftCommand::Stream(StreamCommand::from(request))
}

/// The end-record retention of the default `raft.deleted_data_max_age`.
const END_RECORD_RETAIN: Duration = Duration::from_secs(30 * 60);

/// #669: a group's leader sweeps the streams that expired in it in its
/// tidy pass, and the only entries the sweep appends are `Sweep` at its
/// clock less the grace, each removing up to 256 streams. Their end
/// records are not due yet, so the entries fold nothing.
#[cfg(not(madsim))]
#[tokio::test]
async fn a_leader_sweeps_expired_streams_with_sweep_entries() {
    let (_wal_root, wal) = fresh_wal();
    let log_store = reopen_journal_store(&wal);
    let mut engine =
        start_single_voter_on(&single_node_factory(&wal), placement(), log_store.clone()).await;
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
                expiry_sweep_budget: ursula_runtime::expiry_sweep::GROUP_BUDGET,
                end_record_retain: END_RECORD_RETAIN,
                now_ms: 20_000,
            },
            placement(),
        )
        .await
        .into_result()
        .expect("a tidy pass");
    // 256 streams, then the other 44.
    assert_eq!(report.expiry_sweeps, 2, "{report:?}");
    assert_eq!(report.expiry_swept_streams, 300, "{report:?}");
    assert_eq!(report.expiry_due_groups, 0, "{report:?}");
    assert_eq!(report.expiry_capped_groups, 0, "{report:?}");
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
                EntryPayload::Normal(RaftCommand::Stream(
                    ursula_stream::StreamCommand::Sweep {
                        now_ms: 19_000,
                        limit,
                        retire_through: 0,
                    }
                )) if limit.get() == 256
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
                expiry_sweep_budget: ursula_runtime::expiry_sweep::GROUP_BUDGET,
                end_record_retain: END_RECORD_RETAIN,
                now_ms: 20_000,
            },
            placement(),
        )
        .await
        .into_result()
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
        expiry_sweep_budget: ursula_runtime::expiry_sweep::GROUP_BUDGET,
        end_record_retain: END_RECORD_RETAIN,
        now_ms: 20_000,
    };
    let report = engines[follower_index]
        .tidy_streams(request, placement())
        .await
        .into_result()
        .expect("a follower's pass");
    assert_eq!(report, ursula_runtime::TidyStreamsResponse::default());
    let report = engines[leader_index]
        .tidy_streams(request, placement())
        .await
        .into_result()
        .expect("the leader's pass");
    assert_eq!(report.expiry_sweeps, 1, "{report:?}");
    assert_eq!(report.expiry_swept_streams, 1, "{report:?}");
    let swept = created_index.saturating_add(1);
    for engine in &engines {
        engine
            .raft
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(Some(swept), "sweep replicated")
            .await
            .expect("wait for the sweep");
        let streams = engine
            .with_state_machine(|state_machine| {
                Box::pin(async move { state_machine.engine.state_gauges().streams })
            })
            .await
            .expect("access the state machine");
        assert_eq!(streams, 0, "the replica still holds the expired stream");
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

/// F20: the leader's sweep folds the end records its replica's retirement
/// clock finds due, with the watermark in the entry, and every replica
/// folds the same ones. The clock seeds from the end records' removal
/// times at its first sample, so records removed the retention before the
/// pass fold in that pass, and later ones stay.
#[cfg(not(madsim))]
#[tokio::test]
async fn a_leader_retires_due_end_records_on_every_replica() {
    let (_registry, mut engines, leader_id, _wal_roots) =
        build_three_node_cluster("ursula-end-record-retirement-test", None).await;
    let leader_index = usize::try_from(leader_id.saturating_sub(1)).expect("leader id fits usize");
    let follower_index = if leader_index == 0 { 1 } else { 0 };
    wait_history(&engines[leader_index]).await;
    let mut last_index = 0;
    for (index, deleted_at_ms) in [(0, 10_000), (1, 10_000), (2, 90_000)] {
        let stream_id = bsid(&format!("deleted-{index}"));
        engines[leader_index]
            .raft
            .client_write(create_command(stream_id.clone()))
            .await
            .expect("create a stream");
        last_index = engines[leader_index]
            .raft
            .client_write(RaftCommand::Stream(
                ursula_stream::StreamCommand::DeleteStream {
                    stream_id,
                    now_ms: deleted_at_ms,
                },
            ))
            .await
            .expect("delete the stream")
            .log_id
            .index();
    }
    for engine in &engines {
        engine
            .raft
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(Some(last_index), "deletes replicated")
            .await
            .expect("wait for the deletes");
    }
    // A minute's retention at a clock of 70 s: the two records removed at
    // 10 s are due, the one removed at 90 s is not.
    let request = ursula_runtime::TidyStreamsRequest {
        max_streams: 0,
        max_expiry_sweeps: 16,
        expiry_sweep_budget: ursula_runtime::expiry_sweep::GROUP_BUDGET,
        end_record_retain: Duration::from_secs(60),
        now_ms: 70_000,
    };
    let report = engines[follower_index]
        .tidy_streams(request, placement())
        .await
        .into_result()
        .expect("a follower's pass");
    assert_eq!(report, ursula_runtime::TidyStreamsResponse::default());
    let report = engines[leader_index]
        .tidy_streams(request, placement())
        .await
        .into_result()
        .expect("the leader's pass");
    assert_eq!(report.expiry_sweeps, 1, "{report:?}");
    assert_eq!(report.expiry_swept_streams, 0, "{report:?}");
    assert_eq!(report.end_records_retired, 2, "{report:?}");
    assert_eq!(report.expiry_due_groups, 0, "{report:?}");
    let swept = last_index.saturating_add(1);
    for engine in &engines {
        engine
            .raft
            .wait(Some(Duration::from_secs(5)))
            .applied_index_at_least(Some(swept), "sweep replicated")
            .await
            .expect("wait for the sweep");
        let removed = engine
            .with_applied_state(|state_machine| {
                Box::pin(async move { state_machine.engine.removed_state() })
            })
            .await
            .expect("access the state machine");
        assert_eq!(removed.end_records, 2, "{removed:?}");
        assert_eq!(removed.end_records_early, 0, "{removed:?}");
    }
    // The last record is not due on the leader's clock: the next pass
    // proposes nothing, whatever its wall clock.
    let report = engines[leader_index]
        .tidy_streams(
            ursula_runtime::TidyStreamsRequest {
                now_ms: 1_000_000,
                ..request
            },
            placement(),
        )
        .await
        .into_result()
        .expect("an idle pass");
    assert_eq!(report.expiry_sweeps, 0, "{report:?}");
    shutdown_all(&engines).await;
}

/// OpenRaft runs a snapshot build in a task of its own that `Raft::shutdown`
/// neither stops nor waits for. A build still running when its group engine
/// shuts down publishes no snapshot metadata (#489), and the engine's
/// shutdown returns only once the build ended (#709): nothing writes the WAL
/// directory once the WAL closes.
#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_snapshot_build_running_at_shutdown_publishes_nothing_and_is_awaited() {
    let root = tempfile::tempdir().unwrap();
    let wal = start_wal(root.path(), 1, 1);
    let metadata_path = wal.snapshot_metadata_path(placement());
    let engine = reopen_journal_engine(&wal).await;
    // Stand in for the build OpenRaft detached: its builder, taken while the
    // group runs, builds only once the engine's shutdown began.
    let mut builder = engine
        .with_state_machine(|state_machine| {
            Box::pin(async move { state_machine.get_snapshot_builder().await })
        })
        .await
        .unwrap();
    let work = builder.work.clone();
    let shutdown = tokio::spawn(async move { engine.shutdown().await });
    work.stopping().await;

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
    assert!(
        !shutdown.is_finished(),
        "the engine's shutdown returned while a build of the engine ran"
    );
    drop(builder);
    drop(work);
    shutdown.await.unwrap().unwrap();
    wal.shutdown().await.unwrap();
    fs::remove_dir_all(root.path()).expect("nothing writes the closed WAL directory");
}

/// A single node's replica of `placement()` over `wal`, which the factory
/// opens with its snapshot metadata at the WAL's own path, once it leads.
/// The group's log store opens at once: a previous engine of the group
/// released it before its shutdown returned (#709).
#[cfg(not(madsim))]
async fn reopen_journal_engine(wal: &RaftWal) -> RaftGroupEngine {
    reopen_journal_engine_on(&single_node_factory(wal)).await
}

/// [`reopen_journal_engine`] through `factory`, a single node's factory.
#[cfg(not(madsim))]
async fn reopen_journal_engine_on(factory: &PlacedRaftGroupEngineFactory) -> RaftGroupEngine {
    open_single_voter(factory, placement()).await
}

/// `placement()`'s log store over `wal`. It opens at once: a previous
/// engine of the group released it before its shutdown returned (#709).
#[cfg(not(madsim))]
fn reopen_journal_store(wal: &RaftWal) -> Arc<RaftGroupFileLogStore> {
    wal.open(
        placement(),
        ursula_control::Admission::Create,
        ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics(),
    )
    .expect("the group's log store opens")
}

/// R1: a follower installed a snapshot over a conflicting uncommitted tail,
/// and a power loss under `fsync = never` kept the synced snapshot record but
/// lost the WAL's truncate of that tail. A replica now syncs the journal
/// before it publishes the record
/// (`a_snapshot_record_is_published_after_the_journal_syncs`), so only a
/// record written without that sync leaves this disk: WAL entries at and
/// after the snapshot's index from an older term. Under alpha.28 the group
/// opened and OpenRaft purged only through the snapshot's index, so the
/// later entries stayed and the log's last entry sat below its last purged
/// one. The replica refuses to open such a log.
#[cfg(not(madsim))]
#[tokio::test]
async fn a_log_below_its_restored_snapshot_refuses_to_open() {
    let root = tempfile::tempdir().unwrap();
    let wal = start_wal(root.path(), 1, 1);
    let membership = openraft::Membership::new_with_defaults(vec![BTreeSet::from([1, 2, 3])], []);
    // The disk the power loss left. Term 1 wrote entries 1 to 5 and
    // committed only entry 1. The replica then followed term 2's leader,
    // whose vote survives: votes are always synced.
    {
        let mut store = reopen_journal_store(&wal);
        store
            .save_vote(&openraft::Vote::new_committed(2, 2))
            .await
            .unwrap();
        let mut entries = vec![Entry::new(
            log_id(1),
            EntryPayload::Membership(membership.clone()),
        )];
        entries.extend((2..=5).map(|index| Entry::new(log_id(index), EntryPayload::Blank)));
        store.append(entries, IOFlushed::noop()).await.unwrap();
        store.save_committed(Some(log_id(1))).await.unwrap();
    }
    // Term 2 committed its own entry at index 3, and the follower's record
    // of the snapshot through it is durable.
    let snapshot_log_id = LogId {
        leader_id: CommittedLeaderId::new(2, 2),
        index: 3,
    };
    let metadata_path = wal.snapshot_metadata_path(placement());
    {
        let mut machine = RaftGroupStateMachine::new_with_stores_and_snapshot_install(
            placement(),
            None,
            None,
            ursula_runtime::default_snapshot_store(),
            crate::state_machine::SnapshotBuildCoordinator::default(),
            crate::state_machine::SnapshotInstallCoordinator::default(),
            crate::state_machine::RecordSite::Detached(metadata_path.clone()),
        );
        let snapshot = ursula_runtime::GroupSnapshot {
            placement: placement(),
            group_commit_index: snapshot_log_id.index,
            stream_snapshot: Default::default(),
            usage: Default::default(),
        };
        let bytes = crate::snapshot_codec::group_snapshot_frames(Arc::new(snapshot))
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .into_iter()
            .flatten()
            .collect();
        let pointer = ursula_runtime::SnapshotPointer {
            snapshot_id: "lost-truncate".to_owned(),
            location: ursula_runtime::SnapshotLocation::Inline { bytes },
        };
        let meta = openraft::alias::SnapshotMetaOf::<UrsulaRaftTypeConfig> {
            last_log_id: Some(snapshot_log_id),
            last_membership: openraft::alias::StoredMembershipOf::<UrsulaRaftTypeConfig>::new(
                Some(log_id(1)),
                membership,
            ),
        };
        machine
            .install_snapshot(
                &meta,
                crate::types::SnapshotData::new(pointer.encode_binary().unwrap()),
            )
            .await
            .unwrap();
    }

    let store = reopen_journal_store(&wal);
    let snapshot_record = store.snapshot_record();
    let options = RaftGroupEngineOptions::gated_on(1, placement(), &store).await;
    let opened = RaftGroupEngine::new_node(
        placement(),
        1,
        raft_config("lost-truncate", 500, 1_000),
        InProcessRaftNetworkFactory::new(InProcessRaftRegistry::default()).with_source(1),
        store,
        RaftGroupEngineOptions {
            snapshot_record: Some(snapshot_record),
            peer_cluster: Some(crate::peer_cluster::test_peer_cluster(1)),
            ..options
        },
    )
    .await;
    match opened {
        Err(GroupEngineError::Infra(GroupInfraError::LogBelowSnapshot {
            core_id,
            group: RaftTarget::Data(raft_group_id),
            snapshot,
            held,
        })) => {
            assert_eq!(
                (core_id, raft_group_id),
                (placement().core_id, placement().raft_group_id)
            );
            assert_eq!(snapshot, ursula_runtime::LogPosition {
                term: 2,
                node_id: 2,
                index: 3,
            });
            assert_eq!(held, ursula_runtime::LogPosition {
                term: 1,
                node_id: 1,
                index: 5,
            });
        }
        Err(other) => panic!("expected the log-below-snapshot refusal, got {other:?}"),
        Ok(_) => panic!("a replica opened a log that holds term 1's tail after its snapshot"),
    }
}

/// The restored-snapshot check refuses only a log that holds, or commits,
/// an entry at or after the snapshot's index below it in log-id order. A
/// log purged through the snapshot and extended by its leader, and a log
/// that holds the snapshot's own entry and later ones, open.
#[cfg(not(madsim))]
#[tokio::test]
async fn the_restored_snapshot_check_keeps_a_log_that_continues_the_snapshot() {
    // `IOFlushed::noop` does not wait for the writer: each case syncs, which
    // returns once every record before it joined the store's index.
    let snapshot = LogId {
        leader_id: CommittedLeaderId::new(2, 2),
        index: 3,
    };
    let term_2 = |index: u64| LogId {
        leader_id: CommittedLeaderId::new(2, 2),
        index,
    };
    let blank = |log_id| Entry::new(log_id, EntryPayload::Blank);

    // A follower that held term 1's committed entries 1 and 2 installed
    // term 2's snapshot through 3, purged through it, and took term 2's
    // next entries.
    let (_purged_root, mut purged) = fresh_journal_store();
    purged
        .save_vote(&openraft::Vote::new_committed(2, 2))
        .await
        .unwrap();
    purged
        .append(vec![blank(log_id(1)), blank(log_id(2))], IOFlushed::noop())
        .await
        .unwrap();
    purged.save_committed(Some(snapshot)).await.unwrap();
    purged.purge(snapshot).await.unwrap();
    purged
        .append(vec![blank(term_2(4)), blank(term_2(5))], IOFlushed::noop())
        .await
        .unwrap();
    purged.snapshot_record().sync().await.unwrap();
    purged
        .snapshot_record()
        .check_restored(snapshot)
        .expect("a log that continues a purged snapshot");

    // A replica that took term 2's entries 3 and 4 and built a snapshot
    // through 3: the snapshot's own entry, and a later one.
    let (_held_root, mut held) = fresh_journal_store();
    held.save_vote(&openraft::Vote::new_committed(2, 2))
        .await
        .unwrap();
    held.append(
        vec![
            blank(log_id(1)),
            blank(log_id(2)),
            blank(term_2(3)),
            blank(term_2(4)),
        ],
        IOFlushed::noop(),
    )
    .await
    .unwrap();
    held.save_committed(Some(term_2(4))).await.unwrap();
    held.snapshot_record().sync().await.unwrap();
    held.snapshot_record()
        .check_restored(snapshot)
        .expect("a log that holds the snapshot's entry");

    // An older term's entries at and after the index.
    let (_stale_root, mut stale) = fresh_journal_store();
    stale
        .append(
            (1..=5)
                .map(|index| blank(log_id(index)))
                .collect::<Vec<_>>(),
            IOFlushed::noop(),
        )
        .await
        .unwrap();
    stale.snapshot_record().sync().await.unwrap();
    assert!(matches!(
        stale.snapshot_record().check_restored(snapshot),
        Err(SnapshotOrderError::HeldBelow { held, .. }) if held == log_id(5)
    ));

    // A commit marker at or after the index from an older term.
    let (_committed_root, mut committed) = fresh_journal_store();
    committed
        .append(vec![blank(log_id(1)), blank(log_id(2))], IOFlushed::noop())
        .await
        .unwrap();
    committed.save_committed(Some(log_id(4))).await.unwrap();
    committed.snapshot_record().sync().await.unwrap();
    assert!(matches!(
        committed.snapshot_record().check_restored(snapshot),
        Err(SnapshotOrderError::CommittedBelow { committed, .. }) if committed == log_id(4)
    ));
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
/// on the same WAL and metadata path. A's build publishes nothing, A's
/// shutdown returns only once it ended, and B, which opens right after,
/// builds and publishes its own snapshots.
#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_build_of_a_stopped_engine_leaves_its_restarted_replica_alone() {
    let root = tempfile::tempdir().unwrap();
    let wal = start_wal(root.path(), 1, 1);
    let metadata_path = wal.snapshot_metadata_path(placement());

    let mut engine_a = reopen_journal_engine(&wal).await;
    let mut stale = snapshot_builder_after_write(&mut engine_a, "written-by-a").await;
    let work = stale.work.clone();
    let shutdown = tokio::spawn(async move { engine_a.shutdown().await });
    work.stopping().await;
    let error = stale.build_snapshot().await.unwrap_err();
    assert!(matches!(
        error.get_ref().and_then(
            |source| source.downcast_ref::<crate::state_machine::SnapshotMetadataStopped>()
        ),
        Some(crate::state_machine::SnapshotMetadataStopped::EngineShutDown)
    ));
    let stale_index = stale.meta.last_log_id;
    drop(stale);
    drop(work);
    shutdown.await.unwrap().unwrap();
    assert!(
        !metadata_path.exists(),
        "a build of the stopped engine published"
    );

    let mut engine_b = reopen_journal_engine(&wal).await;
    let mut first = snapshot_builder_after_write(&mut engine_b, "written-by-b").await;
    let first_built = first.build_snapshot().await.expect("B publishes");
    drop(first);
    let published = fs::read(&metadata_path).expect("B's pointer");
    assert!(
        stale_index < first_built.meta.last_log_id,
        "the restarted replica applied past the stopped engine"
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

/// Holds `raft`'s state-machine worker in an external function until
/// `release` fires, and returns once the worker runs it: every command
/// OpenRaft sends the worker meanwhile queues behind it.
#[cfg(not(madsim))]
async fn hold_state_machine_worker(
    raft: Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>,
    release: tokio::sync::oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let held = tokio::spawn(async move {
        // The worker answers once released; a worker that stopped first
        // answers nothing, which is no failure here.
        let _answered = raft
            .with_state_machine(move |_state_machine| {
                Box::pin(async move {
                    entered_tx.send(()).expect("the test waits for the worker");
                    release.await.expect("the test releases the worker");
                })
            })
            .await;
    });
    entered_rx
        .await
        .expect("the worker runs the external function");
    held
}

/// What a group's node-wide state shows of the work its engines ran: the
/// log gauge the next engine of the group shares, the builds in flight on
/// the node and the group's snapshot metadata.
#[cfg(not(madsim))]
#[derive(Debug, PartialEq)]
struct GroupWorkTrace {
    progress: crate::snapshot_cadence::GroupLogProgress,
    running_builds: usize,
    metadata: Option<Vec<u8>>,
}

#[cfg(not(madsim))]
fn group_work_trace(
    builds: &crate::state_machine::SnapshotBuildCoordinator,
    metadata_path: &Path,
) -> GroupWorkTrace {
    GroupWorkTrace {
        progress: builds.log_gauge(placement().raft_group_id.0).progress(),
        running_builds: builds.running_builds(),
        metadata: fs::read(metadata_path).ok(),
    }
}

/// A single-node engine of `placement()` whose state-machine worker is
/// held while three committed applies, a snapshot build and a full
/// snapshot install of a later leader queue behind it.
#[cfg(not(madsim))]
struct QueuedWork {
    /// The single node's factory the engine opened through.
    factory: PlacedRaftGroupEngineFactory,
    engine: RaftGroupEngine,
    raft: Raft<UrsulaRaftTypeConfig, RaftGroupStateMachine>,
    release: tokio::sync::oneshot::Sender<()>,
    held: tokio::task::JoinHandle<()>,
    /// Each queued write, answering whether it applied.
    writes: Vec<tokio::task::JoinHandle<bool>>,
    /// The install's request. A refused install answers nothing; what
    /// counts is that it ran.
    install: tokio::task::JoinHandle<()>,
    builds: crate::state_machine::SnapshotBuildCoordinator,
    /// Taken before the worker is released: a build starting or ending
    /// shows in it.
    builds_changed: crate::state_machine::BuildsChanged,
}

#[cfg(not(madsim))]
async fn queue_state_machine_work(
    wal: &RaftWal,
    cluster_name: &str,
    work_stop_timeout: Option<Duration>,
) -> QueuedWork {
    let mut factory = single_node_factory(wal);
    factory.work_stop_timeout = work_stop_timeout;
    let builds = factory.registry().snapshot_build_coordinator();
    let mut engine = reopen_journal_engine_on(&factory).await;
    let stream = bsid(cluster_name);
    engine
        .create_stream(
            create_req(stream.clone()),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .unwrap();
    let raft = engine.raft_handle();
    let (release, released) = tokio::sync::oneshot::channel();
    let held = hold_state_machine_worker(raft.clone(), released).await;

    // Applies: committed, so RaftCore sent them to the held worker.
    let committed = openraft::rt::WatchReceiver::borrow_watched(&raft.metrics())
        .local_committed
        .map_or(0, |log_id| log_id.index);
    let writes = (0..3)
        .map(|_| {
            let raft = raft.clone();
            let command = append_command(stream.clone(), b"queued");
            tokio::spawn(async move { raft.client_write(command).await.is_ok() })
        })
        .collect::<Vec<_>>();
    let queued = committed.saturating_add(3);
    raft.wait(Some(Duration::from_secs(5)))
        .metrics(
            |metrics| {
                metrics
                    .local_committed
                    .is_some_and(|log_id| log_id.index >= queued)
            },
            "the queued writes commit",
        )
        .await
        .unwrap();
    // A snapshot build, then a full snapshot install of a later leader:
    // RaftCore takes requests in order and sends both to the worker as it
    // handles them, so both are queued once it adopted the leader's term.
    raft.trigger().snapshot().await.unwrap();
    let installed_index = queued.saturating_add(10);
    let installed_id = LogId {
        leader_id: CommittedLeaderId::new(9, 2),
        index: installed_index,
    };
    let group = ursula_runtime::GroupSnapshot {
        placement: placement(),
        group_commit_index: installed_index,
        stream_snapshot: Default::default(),
        usage: Default::default(),
    };
    let bytes = crate::snapshot_codec::group_snapshot_frames(Arc::new(group))
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .concat();
    let pointer = ursula_runtime::SnapshotPointer {
        snapshot_id: "queued-install".to_owned(),
        location: ursula_runtime::SnapshotLocation::Inline { bytes },
    };
    let snapshot = crate::types::UrsulaSnapshot {
        meta: openraft::alias::SnapshotMetaOf::<UrsulaRaftTypeConfig> {
            last_log_id: Some(installed_id),
            last_membership: openraft::alias::StoredMembershipOf::<UrsulaRaftTypeConfig>::new(
                Some(installed_id),
                openraft::Membership::new(vec![BTreeSet::from([1])], BTreeSet::from([1])).unwrap(),
            ),
        },
        snapshot: crate::types::SnapshotData::new(pointer.encode_binary().unwrap()),
    };
    let install = {
        let raft = raft.clone();
        tokio::spawn(async move {
            let _answer = raft
                .install_full_snapshot(UrsulaVote::new_committed(9, 2), snapshot)
                .await;
        })
    };
    raft.wait(Some(Duration::from_secs(5)))
        .metrics(
            // It may campaign past that term once the leader stays silent.
            |metrics| metrics.current_term >= 9,
            "RaftCore adopts the installing leader's term",
        )
        .await
        .unwrap();
    let builds_changed = builds.subscribe_builds();
    QueuedWork {
        factory,
        engine,
        raft,
        release,
        held,
        writes,
        install,
        builds,
        builds_changed,
    }
}

/// OpenRaft's shutdown stops RaftCore and leaves its state-machine worker
/// running whatever it already received (#709). Applies, a snapshot build
/// and a full snapshot install queue behind a held worker, and the engine
/// shuts down. The worker runs them only after RaftCore stopped, and the
/// engine's shutdown returns only once it did: what the group's node-wide
/// state shows then holds, and the group's log store opens again at once,
/// as a restart in the same process opens it. The install of an engine
/// that is shutting down is refused and leaves the gauge alone.
///
/// The control below runs the same work with a shutdown that does not
/// wait.
#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_shutdown_returns_after_the_state_machine_work_it_queued() {
    let root = tempfile::tempdir().unwrap();
    let wal = start_wal(root.path(), 1, 1);
    let metadata_path = wal.snapshot_metadata_path(placement());
    let queued = queue_state_machine_work(&wal, "queued-work", None).await;
    let before = group_work_trace(&queued.builds, &metadata_path);
    assert!(!before.progress.has_snapshot);

    // The worker goes on only after RaftCore stopped.
    let release = queued.release;
    let releaser = {
        let raft = queued.raft.clone();
        tokio::spawn(async move {
            raft.wait(Some(Duration::from_secs(10)))
                .metrics(
                    |metrics| matches!(metrics.state, openraft::ServerState::Shutdown),
                    "RaftCore stops",
                )
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
            release.send(()).expect("the worker is still held");
        })
    };
    queued.engine.shutdown().await.unwrap();
    let after = group_work_trace(&queued.builds, &metadata_path);
    // The group opens again at once: nothing of the stopped engine holds
    // its log store.
    let reopened = wal
        .open(
            placement(),
            ursula_control::Admission::Create,
            ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics(),
        )
        .expect("the stopped engine released the group's log store");

    // The queued work ran before the shutdown returned: the applies
    // answered, a build started, and the refused install left the gauge
    // without a snapshot.
    for write in queued.writes {
        assert!(write.await.unwrap(), "the queued write applied");
    }
    tokio::time::timeout(Duration::from_millis(1), queued.builds_changed.changed())
        .await
        .expect("a queued build started before the shutdown returned");
    assert!(
        !after.progress.has_snapshot
            && after.progress.last_snapshot_bytes == 0
            && after.running_builds == 0,
        "the refused install left the gauge alone: {after:?}"
    );
    assert!(after.progress.log_bytes > before.progress.log_bytes);
    releaser.await.unwrap();
    queued.held.await.unwrap();
    queued.install.await.unwrap();
    // And nothing of it runs after.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        group_work_trace(&queued.builds, &metadata_path),
        after,
        "work of the stopped engine ran after its shutdown returned"
    );

    let engine = queued
        .factory
        .start_on(
            placement(),
            reopened,
            crate::engine::test_support::engine_metrics(placement()),
        )
        .await
        .unwrap();
    engine.shutdown().await.unwrap();
    wal.shutdown().await.unwrap();
}

/// The negative control of the test above: a shutdown that waits no time
/// for OpenRaft's work returns while the worker is held. It fails typed,
/// the group's log store does not open, and the queued applies change the
/// gauge only after it returned.
#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shutdown_that_does_not_wait_leaves_the_queued_work_running() {
    let root = tempfile::tempdir().unwrap();
    let wal = start_wal(root.path(), 1, 1);
    let metadata_path = wal.snapshot_metadata_path(placement());
    let queued = queue_state_machine_work(&wal, "unwaited-work", Some(Duration::ZERO)).await;

    let stopped = queued.engine.shutdown().await;
    assert!(
        matches!(
            &stopped,
            Err(GroupEngineError::Infra(GroupInfraError::EngineWorkOutlivedShutdown {
                group: RaftTarget::Data(raft_group_id),
                waited_ms: 0,
            })) if *raft_group_id == placement().raft_group_id
        ),
        "{stopped:?}"
    );
    let at_return = group_work_trace(&queued.builds, &metadata_path);
    assert!(matches!(
        wal.open(
            placement(),
            ursula_control::Admission::Create,
            ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics(),
        ),
        Err(crate::log_store::RaftWalError::PreviousEngineRunning { placement: refused })
            if refused == placement()
    ));

    queued.release.send(()).expect("the worker is still held");
    for write in queued.writes {
        assert!(write.await.unwrap(), "the queued write applied");
    }
    assert_ne!(
        group_work_trace(&queued.builds, &metadata_path),
        at_return,
        "the queued applies ran after the shutdown returned"
    );
    queued.held.await.unwrap();
    queued.install.await.unwrap();
}

/// OpenRaft work of an engine that does not end within the shutdown's
/// bound fails the shutdown, typed, and keeps the group closed: its log
/// store does not open while the work holds it. A later shutdown returns
/// once the work ended (#709).
#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_shutdown_names_openraft_work_that_does_not_end() {
    let root = tempfile::tempdir().unwrap();
    let wal = start_wal(root.path(), 1, 1);
    let mut factory = single_node_factory(&wal);
    factory.work_stop_timeout = Some(Duration::from_millis(200));
    let engine = reopen_journal_engine_on(&factory).await;
    let (release, released) = tokio::sync::oneshot::channel();
    let held = hold_state_machine_worker(engine.raft_handle(), released).await;

    let stopped = engine.shutdown().await;
    assert!(
        matches!(
            &stopped,
            Err(GroupEngineError::Infra(GroupInfraError::EngineWorkOutlivedShutdown {
                group: RaftTarget::Data(raft_group_id),
                waited_ms: 200,
            })) if *raft_group_id == placement().raft_group_id
        ),
        "{stopped:?}"
    );
    assert!(matches!(
        wal.open(
            placement(),
            ursula_control::Admission::Create,
            ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics(),
        ),
        Err(crate::log_store::RaftWalError::PreviousEngineRunning { placement: refused })
            if refused == placement()
    ));

    release.send(()).unwrap();
    held.await.unwrap();
    engine.shutdown().await.unwrap();
    let store = wal
        .open(
            placement(),
            ursula_control::Admission::Create,
            ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics(),
        )
        .expect("the ended work released the group's log store");
    drop(store);
    wal.shutdown().await.unwrap();
}

/// The memory store of node `node_id` on a fresh bucket, under the prefix
/// `snapshots`, and the bucket (#687).
#[cfg(not(madsim))]
fn memory_snapshot_store(
    node_id: u64,
) -> (
    ursula_runtime::snapshot_harness::MemorySnapshotBucket,
    ursula_runtime::SharedSnapshotStore,
) {
    let bucket = ursula_runtime::snapshot_harness::MemorySnapshotBucket::new(
        node_id,
        ursula_runtime::SnapshotPrefix::try_from("snapshots").unwrap(),
        ursula_runtime::snapshot_harness::SnapshotStoreFaults::none(),
    );
    let store: ursula_runtime::SharedSnapshotStore = Arc::new(bucket.store(node_id));
    (bucket, store)
}

/// #687: a node killed after a build wrote its object and checked it with
/// a HEAD, and before the build synced the journal and wrote the pointer
/// record, leaves the object and no record. The kill point is the store's
/// gate after the HEAD. The restart restores no snapshot, opens the group,
/// and its first GC pass deletes the object, so the node's directory holds
/// only what it names (I5).
#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_kill_after_the_object_write_before_the_pointer_record_leaves_the_object_to_the_restart()
{
    use ursula_runtime::snapshot_harness::Gate;
    use ursula_runtime::snapshot_harness::GatedSnapshotStore;
    use ursula_runtime::snapshot_harness::NodeNames;
    use ursula_runtime::snapshot_harness::SnapshotStoreGates;
    use ursula_runtime::snapshot_harness::StoreOp;
    use ursula_runtime::snapshot_harness::check_quiet_prefixes;

    let root = tempfile::tempdir().unwrap();
    let wal = start_wal(root.path(), 1, 1);
    let metadata_path = wal.snapshot_metadata_path(placement());
    let (bucket, inner) = memory_snapshot_store(1);
    let gates = SnapshotStoreGates::new();
    let store: ursula_runtime::SharedSnapshotStore =
        Arc::new(GatedSnapshotStore::new(1, inner, gates.clone()));
    let factory = || single_node_factory(&wal).with_snapshot_store(Some(store.clone()));
    let engine = reopen_journal_engine_on(&factory()).await;
    let checked = Gate::after(1, StoreOp::Head);
    gates.hold(checked);
    let mut builder = engine
        .with_state_machine(|state_machine| {
            Box::pin(async move { state_machine.get_snapshot_builder().await })
        })
        .await
        .unwrap();
    let building = tokio::spawn(async move { builder.build_snapshot().await });
    tokio::time::timeout(Duration::from_secs(10), gates.wait_arrived(checked, 1))
        .await
        .expect("the build wrote and checked its object");
    assert_eq!(bucket.keys().len(), 1, "the object is written");
    // The kill: the build never syncs the journal or writes its record.
    building.abort();
    assert!(building.await.unwrap_err().is_cancelled());
    gates.open(checked);
    engine.shutdown().await.unwrap();
    assert!(!metadata_path.exists(), "no pointer record was written");
    assert_eq!(bucket.keys().len(), 1, "the object stays until a pass");

    let reopened = reopen_journal_engine_on(&factory()).await;
    let holds = reopened.snapshots.holds.clone();
    assert_eq!(holds.current(), None);
    assert!(holds.gc_due(crate::rt::time::Instant::now()));
    holds
        .collect_garbage(&store, crate::rt::time::Instant::now())
        .await
        .unwrap();
    assert_eq!(
        check_quiet_prefixes(&bucket.view(), [&NodeNames::new(1, Vec::<String>::new())]),
        Ok(())
    );
    reopened.shutdown().await.unwrap();
    wal.shutdown().await.unwrap();
}

/// #687, #709: an engine incarnation's holds are its own. What a shut-down
/// engine leaves running, a send's lease and a build refused at its
/// shutdown, changes nothing in the holds of the group's next engine,
/// which start empty and restored from the durable record alone.
#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shut_down_engine_changes_no_hold() {
    let root = tempfile::tempdir().unwrap();
    let wal = start_wal(root.path(), 1, 1);
    let (bucket, store) = memory_snapshot_store(1);
    let factory = || single_node_factory(&wal).with_snapshot_store(Some(store.clone()));
    let engine = reopen_journal_engine_on(&factory()).await;
    engine
        .with_state_machine(|state_machine| {
            Box::pin(async move {
                state_machine
                    .get_snapshot_builder()
                    .await
                    .build_snapshot()
                    .await
            })
        })
        .await
        .unwrap()
        .expect("a build");
    let old_holds = engine.snapshots.holds.clone();
    let current = old_holds.current().expect("an object is current");
    // A send still in flight, and a build taken while the group ran.
    let send = engine.raft_handle().get_snapshot().await.unwrap().unwrap();
    let mut builder = engine
        .with_state_machine(|state_machine| {
            Box::pin(async move { state_machine.get_snapshot_builder().await })
        })
        .await
        .unwrap();
    let work = builder.work.clone();
    let shutdown = tokio::spawn(async move { engine.shutdown().await });
    work.stopping().await;
    builder
        .build_snapshot()
        .await
        .expect_err("a build of a stopping engine is refused");
    drop(builder);
    drop(work);
    shutdown.await.unwrap().unwrap();

    let reopened = reopen_journal_engine_on(&factory()).await;
    let holds = reopened.snapshots.holds.clone();
    assert!(!Arc::ptr_eq(&holds, &old_holds), "a fresh incarnation");
    assert_eq!(holds.current(), Some(current.clone()));
    holds
        .collect_garbage(&store, crate::rt::time::Instant::now())
        .await
        .unwrap();
    let held = holds.held_keys();
    let due = holds.gc_due(crate::rt::time::Instant::now());
    drop(send);
    drop(old_holds);
    assert_eq!(holds.held_keys(), held);
    assert_eq!(holds.gc_due(crate::rt::time::Instant::now()), due);
    assert!(!due, "the old engine's send owes the new one no pass");
    assert_eq!(
        bucket.keys(),
        BTreeSet::from([current]),
        "the restored current object is all the directory holds"
    );
    reopened.shutdown().await.unwrap();
    wal.shutdown().await.unwrap();
}

/// #687, the in-process lease handoff: the in-process network keeps the
/// sender's lease until its RPC returns and hands the receiver the pointer
/// only, as the gRPC network does. While a learner's install has read the
/// leader's object, the leader replaces the object and runs its GC pass,
/// and the object survives. Once the RPC returned, the leader's next pass
/// deletes it, and the learner's current snapshot names its own copy.
#[cfg(not(madsim))]
#[tokio::test]
async fn the_in_process_network_holds_a_sent_object_until_its_rpc_returns() {
    use ursula_runtime::snapshot_harness::Gate;
    use ursula_runtime::snapshot_harness::GatedSnapshotStore;
    use ursula_runtime::snapshot_harness::SnapshotStoreGates;
    use ursula_runtime::snapshot_harness::StoreOp;

    let registry = InProcessRaftRegistry::default();
    let config = Arc::new(
        Config {
            cluster_name: "ursula-lease-handoff".to_owned(),
            heartbeat_interval: 10,
            election_timeout_min: 500,
            election_timeout_max: 1_000,
            install_snapshot_timeout: 30_000,
            max_in_snapshot_log_to_keep: 0,
            purge_batch_size: 1,
            replication_lag_threshold: 0,
            snapshot_policy: SnapshotPolicy::Never,
            ..Default::default()
        }
        .validate()
        .expect("valid raft config"),
    );
    let bucket = ursula_runtime::snapshot_harness::MemorySnapshotBucket::new(
        3,
        ursula_runtime::SnapshotPrefix::try_from("snapshots").unwrap(),
        ursula_runtime::snapshot_harness::SnapshotStoreFaults::none(),
    );
    let gates = SnapshotStoreGates::new();
    let stores: Vec<ursula_runtime::SharedSnapshotStore> = (1..=3)
        .map(|node_id| {
            Arc::new(GatedSnapshotStore::new(
                node_id,
                Arc::new(bucket.store(node_id)),
                gates.clone(),
            )) as ursula_runtime::SharedSnapshotStore
        })
        .collect();
    let mut engines = Vec::new();
    let mut wal_roots = Vec::new();
    for node_id in 1..=3_u64 {
        let (wal_root, log_store) = fresh_journal_store();
        wal_roots.push(wal_root);
        let options = RaftGroupEngineOptions::gated_on(node_id, placement(), &log_store).await;
        let engine = RaftGroupEngine::new_node_with_log_store_network_and_options(
            placement(),
            node_id,
            config.clone(),
            InProcessRaftNetworkFactory::new(registry.clone()).with_source(node_id),
            log_store,
            RaftGroupEngineOptions {
                snapshot_store: Some(stores[usize::try_from(node_id - 1).unwrap()].clone()),
                ..options
            },
        )
        .await
        .expect("create a node");
        if node_id != 3 {
            registry.register(node_id, &engine);
        }
        engines.push(engine);
    }
    engines[0]
        .raft
        .initialize(BTreeSet::from([1, 2]))
        .await
        .expect("initialize two voters");
    let leader_id = engines[0]
        .raft
        .wait(Some(Duration::from_secs(5)))
        .metrics(|metrics| metrics.current_leader.is_some(), "a leader")
        .await
        .expect("a leader")
        .current_leader
        .expect("a leader id");
    let leader = &engines[usize::try_from(leader_id - 1).unwrap()];
    for engine in &engines[..2] {
        engine
            .raft
            .wait(Some(Duration::from_secs(5)))
            .current_leader(leader_id, "both voters see the leader")
            .await
            .expect("a shared leader");
    }
    wait_history(leader).await;
    let stream_id = bsid("lease-handoff");
    leader
        .raft
        .client_write(create_command(stream_id.clone()))
        .await
        .expect("create");
    let written = leader
        .raft
        .client_write(append_command(stream_id.clone(), b"handed-off"))
        .await
        .expect("append");
    leader.raft.trigger().snapshot().await.unwrap();
    leader
        .raft
        .wait(Some(Duration::from_secs(5)))
        .snapshot(written.log_id, "the leader's snapshot")
        .await
        .unwrap();
    leader
        .raft
        .trigger()
        .purge_log(written.log_id.index())
        .await
        .unwrap();
    leader
        .raft
        .wait(Some(Duration::from_secs(5)))
        .purged(Some(written.log_id), "the leader purged its log")
        .await
        .unwrap();
    let sent = leader.snapshots.holds.current().expect("an object");

    // The learner's GET of the leader's object answers, and waits there.
    let read = Gate::after(3, StoreOp::Get);
    gates.hold(read);
    registry.register(3, &engines[2]);
    leader
        .raft
        .add_learner(3, EmptyNode {}, false)
        .await
        .expect("add the learner");
    tokio::time::timeout(Duration::from_secs(10), gates.wait_arrived(read, 1))
        .await
        .expect("the learner reads the sent object");

    // The leader replaces the object while the send is in flight.
    let replaced = leader
        .raft
        .client_write(append_command(stream_id.clone(), b"after"))
        .await
        .expect("append");
    leader.raft.trigger().snapshot().await.unwrap();
    leader
        .raft
        .wait(Some(Duration::from_secs(5)))
        .snapshot(replaced.log_id, "the leader's next snapshot")
        .await
        .unwrap();
    assert_ne!(leader.snapshots.holds.current(), Some(sent.clone()));
    leader
        .snapshots
        .holds
        .owe_pass(crate::rt::time::Instant::now());
    leader
        .snapshots
        .holds
        .collect_garbage(
            &stores[usize::try_from(leader_id - 1).unwrap()],
            crate::rt::time::Instant::now(),
        )
        .await
        .unwrap();
    assert!(
        bucket.keys().contains(&sent),
        "the send's lease keeps the object while its RPC runs"
    );

    gates.open(read);
    let learner = &engines[2];
    learner
        .raft
        .wait(Some(Duration::from_secs(10)))
        .metrics(
            |metrics| {
                metrics
                    .snapshot
                    .is_some_and(|snapshot| snapshot.index >= written.log_id.index())
            },
            "the learner installed the snapshot",
        )
        .await
        .expect("the learner installs");
    let copy = learner
        .snapshots
        .holds
        .current()
        .expect("the learner's copy");
    assert!(copy.starts_with("snapshots/node-3/group-0/"), "{copy}");

    // Once the RPC returned, the leader's pass deletes the object.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while bucket.keys().contains(&sent) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the sent object outlives its RPC"
        );
        leader
            .snapshots
            .holds
            .owe_pass(crate::rt::time::Instant::now());
        leader
            .snapshots
            .holds
            .collect_garbage(
                &stores[usize::try_from(leader_id - 1).unwrap()],
                crate::rt::time::Instant::now(),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(bucket.keys().contains(&copy), "the learner's copy stays");
    shutdown_all(&engines).await;
}

/// #687 B1: a GC pass that read a group's holds runs as the work of the
/// incarnation they belong to. Once that engine shut down and the group's
/// next engine committed a snapshot of its own, a pass on the old holds
/// deletes nothing. The control runs the same pass without the fence: the
/// old holds name the old current only, so the pass deletes the new one.
#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pass_on_a_stopped_incarnations_holds_deletes_nothing() {
    use ursula_runtime::snapshot_harness::NodeNames;
    use ursula_runtime::snapshot_harness::check_named_keys_exist;

    async fn build(engine: &RaftGroupEngine) {
        engine
            .with_state_machine(|state_machine| {
                Box::pin(async move {
                    state_machine
                        .get_snapshot_builder()
                        .await
                        .build_snapshot()
                        .await
                })
            })
            .await
            .unwrap()
            .expect("a build");
    }

    for fenced in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let wal = start_wal(root.path(), 1, 1);
        let (bucket, store) = memory_snapshot_store(1);
        let factory = || single_node_factory(&wal).with_snapshot_store(Some(store.clone()));
        let old = reopen_journal_engine_on(&factory()).await;
        build(&old).await;
        // What a driver tick copied before the group restarted.
        let snapshots = old.snapshots.clone();
        let admission = old.snapshot_installs.clone();
        old.shutdown().await.unwrap();

        let new = reopen_journal_engine_on(&factory()).await;
        build(&new).await;
        let current = new.snapshots.holds.current().expect("the new current");
        snapshots.holds.owe_pass(crate::rt::time::Instant::now());
        if fenced {
            assert!(
                snapshots
                    .fenced_pass(&admission, crate::rt::time::Instant::now())
                    .await
                    .is_none(),
                "a stopped incarnation runs no pass"
            );
        } else {
            snapshots
                .holds
                .collect_garbage(&snapshots.store, crate::rt::time::Instant::now())
                .await
                .unwrap();
        }
        let named = [NodeNames::new(1, [current.clone()])];
        assert_eq!(
            check_named_keys_exist(&bucket.view(), &named).is_ok(),
            fenced,
            "fenced {fenced}: the new incarnation's current object"
        );
        new.shutdown().await.unwrap();
        wal.shutdown().await.unwrap();
    }
}

/// #687 B1 through the driver's entry point: a round of
/// `collect_snapshot_garbage` whose pass waits before its LIST while the
/// group's engine shuts down is stopped with the engine, and once the next
/// engine committed Y and the LIST is let through, Y survives.
#[cfg(not(madsim))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gc_round_held_across_a_restart_never_deletes_the_new_incarnations_object() {
    use ursula_runtime::snapshot_harness::Gate;
    use ursula_runtime::snapshot_harness::GatedSnapshotStore;
    use ursula_runtime::snapshot_harness::SnapshotStoreGates;
    use ursula_runtime::snapshot_harness::StoreOp;

    async fn build(engine: &RaftGroupEngine) {
        engine
            .with_state_machine(|state_machine| {
                Box::pin(async move {
                    state_machine
                        .get_snapshot_builder()
                        .await
                        .build_snapshot()
                        .await
                })
            })
            .await
            .unwrap()
            .expect("a build");
    }

    let root = tempfile::tempdir().unwrap();
    let wal = start_wal(root.path(), 1, 1);
    let (bucket, inner) = memory_snapshot_store(1);
    let gates = SnapshotStoreGates::new();
    let store: ursula_runtime::SharedSnapshotStore =
        Arc::new(GatedSnapshotStore::new(1, inner, gates.clone()));
    let factory = || single_node_factory(&wal).with_snapshot_store(Some(store.clone()));
    let registry = RaftGroupHandleRegistry::default();
    let old = reopen_journal_engine_on(&factory()).await;
    registry.register_engine(&old);
    build(&old).await;

    let before_list = Gate::before(1, StoreOp::ListOwn);
    let listed = gates.arrived(before_list);
    gates.hold(before_list);
    old.snapshots
        .holds
        .owe_pass(crate::rt::time::Instant::now());
    let round = tokio::spawn({
        let registry = registry.clone();
        async move {
            registry
                .collect_snapshot_garbage(usize::MAX, Duration::from_secs(60))
                .await
        }
    });
    tokio::time::timeout(
        Duration::from_secs(10),
        gates.wait_arrived(before_list, listed.saturating_add(1)),
    )
    .await
    .expect("the round's pass waits before its LIST");
    // The restart: the old engine's shutdown stops the round's pass.
    old.shutdown().await.unwrap();
    let round = round.await.unwrap();
    assert_eq!(round.groups, 0, "the stopped engine's pass did not run");

    let new = reopen_journal_engine_on(&factory()).await;
    registry.register_engine(&new);
    gates.open(before_list);
    build(&new).await;
    let current = new.snapshots.holds.current().expect("the new current");
    registry
        .collect_snapshot_garbage(usize::MAX, Duration::from_secs(60))
        .await;
    assert!(
        bucket.keys().contains(&current),
        "the new incarnation's current object survives"
    );
    new.shutdown().await.unwrap();
    wal.shutdown().await.unwrap();
}
