//! Raft-engine regressions for the bounded-state cold-path defects: D1
//! (regressed cold frontier, including openraft snapshot build/install), D3
//! (stale page entries of rejected external appends) and F14e (stale
//! flushes leave no page entry).

use std::sync::Arc;

use futures_util::stream;
use openraft::BasicNode;
use openraft::Config;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::entry::RaftEntry;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use openraft::vote::RaftLeaderId;
use ursula_runtime::AppendExternalRequest;
use ursula_runtime::AppendRequest;
use ursula_runtime::ColdIndexPageKey;
use ursula_runtime::ColdIndexPageStore;
use ursula_runtime::ColdStore;
use ursula_runtime::ColdStoreColdIndexPageStore;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::FlushColdRequest;
use ursula_runtime::GroupEngine;
use ursula_runtime::GroupWriteCommand;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::StreamErrorCode;
use ursula_shard::BucketStreamId;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;
use ursula_stream::ColdChunkRef;
use ursula_stream::ExternalPayloadRef;
use ursula_stream::StreamCommand;

use crate::engine::RaftGroupEngine;
use crate::log_store::RaftGroupLogStore;
use crate::state_machine::RaftGroupStateMachine;
use crate::types::UrsulaRaftTypeConfig;

const OCTET: &str = "application/octet-stream";

type CommittedLeaderId = <UrsulaRaftTypeConfig as openraft::RaftTypeConfig>::LeaderId;

fn placement() -> ShardPlacement {
    ShardPlacement {
        core_id: CoreId(0),
        shard_id: ShardId(0),
        raft_group_id: RaftGroupId(0),
    }
}

fn bsid(name: &str) -> BucketStreamId {
    BucketStreamId::new("benchcmp", name)
}

fn read_req(stream_id: BucketStreamId, offset: u64, max_len: usize) -> ReadStreamRequest {
    ReadStreamRequest {
        stream_id,
        offset,
        max_len,
        now_ms: 0,
        record: None,
        max_records: None,
        leader_only: false,
    }
}

async fn cold_engine(cold_store: Arc<ColdStore>) -> RaftGroupEngine {
    let config = Arc::new(
        Config {
            cluster_name: "ursula-cold-correctness".to_owned(),
            heartbeat_interval: 10,
            election_timeout_min: 30,
            election_timeout_max: 60,
            ..Default::default()
        }
        .validate()
        .expect("valid config"),
    );
    RaftGroupEngine::new_single_node_with_log_store_and_metrics(
        placement(),
        1,
        BasicNode::new("local"),
        config,
        RaftGroupLogStore::shared(),
        None,
        Some(cold_store),
    )
    .await
    .expect("create raft group engine")
}

fn external_payload(s3_path: &str, len: u64) -> ExternalPayloadRef {
    ExternalPayloadRef {
        s3_path: s3_path.to_owned(),
        payload_len: len,
        object_size: len,
    }
}

fn append_external_req(
    stream_id: &BucketStreamId,
    s3_path: &str,
    len: u64,
    stream_seq: Option<&str>,
) -> AppendExternalRequest {
    AppendExternalRequest {
        stream_id: stream_id.clone(),
        content_type: OCTET.to_owned(),
        payload: external_payload(s3_path, len),
        record_ends: Vec::new(),
        close_after: false,
        stream_seq: stream_seq.map(str::to_owned),
        producer: None,
        now_ms: 0,
        record_match: None,
    }
}

fn append_req(
    stream_id: &BucketStreamId,
    payload: &[u8],
    stream_seq: Option<&str>,
) -> AppendRequest {
    let mut request = AppendRequest::from_bytes(stream_id.clone(), payload.to_vec());
    request.stream_seq = stream_seq.map(str::to_owned);
    request
}

fn chunk(start_offset: u64, end_offset: u64, s3_path: &str) -> ColdChunkRef {
    ColdChunkRef {
        start_offset,
        end_offset,
        s3_path: s3_path.to_owned(),
        object_size: end_offset - start_offset,
        ..Default::default()
    }
}

async fn stage(cold_store: &ColdStore, path: &str, payload: &[u8]) {
    cold_store
        .write_chunk(path, payload)
        .await
        .expect("stage cold object");
}

/// D1 through the Raft engine: reads of the external above a flushed hot
/// prefix work, and the engine's group snapshot (which still carries the
/// regressed frontier) installs into a fresh engine that serves the bytes.
#[tokio::test]
async fn d1_raft_engine_reads_and_installs_external_above_a_flushed_hot_prefix() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let mut engine = cold_engine(cold_store.clone()).await;
    let stream_id = bsid("raft-d1");
    engine
        .create_stream(
            CreateStreamRequest::new(stream_id.clone(), OCTET),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("create stream");
    engine
        .append(
            append_req(&stream_id, b"ab", None),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("append hot prefix");
    stage(&cold_store, "benchcmp/raft-d1/external/xyz.bin", b"XYZ").await;
    engine
        .append_external(
            append_external_req(&stream_id, "benchcmp/raft-d1/external/xyz.bin", 3, None),
            placement(),
        )
        .await
        .expect("append external");
    stage(&cold_store, "benchcmp/raft-d1/chunks/ab.bin", b"ab").await;
    engine
        .flush_cold(
            FlushColdRequest {
                stream_id: stream_id.clone(),
                chunk: chunk(0, 2, "benchcmp/raft-d1/chunks/ab.bin"),
            },
            placement(),
        )
        .await
        .expect("flush hot prefix");

    let read = engine
        .read_stream(read_req(stream_id.clone(), 0, 16), placement())
        .await
        .expect("read across the flushed prefix and the external");
    assert_eq!(read.payload, b"abXYZ");

    let snapshot = engine.snapshot(placement()).await.expect("snapshot");
    let entry = snapshot
        .stream_snapshot
        .streams
        .iter()
        .find(|entry| entry.metadata.stream_id == stream_id)
        .expect("snapshot entry");
    assert_eq!(entry.cold_frontier_offset, 2);
    engine.shutdown().await.expect("shutdown source");

    let mut target = cold_engine(cold_store).await;
    target
        .install_snapshot(snapshot)
        .await
        .expect("install snapshot with a regressed frontier");
    let read = target
        .read_stream(read_req(stream_id, 0, 16), placement())
        .await
        .expect("read after install");
    assert_eq!(read.payload, b"abXYZ");
    target.shutdown().await.expect("shutdown target");
}

fn log_id(index: u64) -> LogId<CommittedLeaderId> {
    LogId {
        leader_id: CommittedLeaderId::new(1, 1),
        index,
    }
}

fn normal_entry(
    index: u64,
    command: StreamCommand,
) -> <UrsulaRaftTypeConfig as openraft::RaftTypeConfig>::Entry {
    Entry::new(
        log_id(index),
        EntryPayload::Normal(GroupWriteCommand::from(command)),
    )
}

/// D1 through openraft's own snapshot path: a snapshot built after the
/// frontier regressed installs on a lagging replica.
#[tokio::test]
async fn d1_openraft_snapshot_with_regressed_frontier_builds_and_installs() {
    let stream_id = bsid("raft-d1-install");
    let commands = vec![
        StreamCommand::CreateStream {
            stream_id: stream_id.clone(),
            content_type: OCTET.to_owned(),
            initial_payload: bytes::Bytes::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            attrs: None,
            now_ms: 0,
        },
        StreamCommand::Append {
            stream_id: stream_id.clone(),
            content_type: Some(OCTET.to_owned()),
            payload: bytes::Bytes::from_static(b"ab"),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            record_match: None,
        },
        StreamCommand::AppendExternal {
            stream_id: stream_id.clone(),
            content_type: Some(OCTET.to_owned()),
            payload: external_payload("external/xyz.bin", 3),
            record_ends: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
            record_match: None,
        },
        StreamCommand::FlushCold {
            stream_id: stream_id.clone(),
            chunk: chunk(0, 2, "chunks/ab.bin"),
        },
    ];
    let entries = commands
        .into_iter()
        .zip(1..)
        .map(|(command, index)| normal_entry(index, command))
        .collect::<Vec<_>>();
    let mut source = RaftGroupStateMachine::new(placement());
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
        .expect("install snapshot with a regressed frontier");
    assert_eq!(target.engine.stream_tail_offset(&stream_id), Some(5));
}

/// D3 through the Raft engine: the page entry of a rejected external append
/// must not serve bytes once a flush proves the range.
#[tokio::test]
async fn d3_raft_flush_clips_the_page_entry_of_a_rejected_external_append() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let mut engine = cold_engine(cold_store.clone()).await;
    let stream_id = bsid("raft-d3-clip");
    engine
        .create_stream(
            CreateStreamRequest::new(stream_id.clone(), OCTET),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("create stream");
    engine
        .append(
            append_req(&stream_id, b"ab", Some("5")),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("append with stream seq");
    stage(
        &cold_store,
        "benchcmp/raft-d3-clip/external/rejected.bin",
        b"0123456789",
    )
    .await;
    let err = engine
        .append_external(
            append_external_req(
                &stream_id,
                "benchcmp/raft-d3-clip/external/rejected.bin",
                10,
                Some("1"),
            ),
            placement(),
        )
        .await
        .expect_err("a regressed stream seq rejects the external append");
    assert_eq!(err.code(), Some(StreamErrorCode::StreamSeqConflict));
    engine
        .append(
            append_req(&stream_id, b"cdefgh", None),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("append hot bytes over the rejected range");
    stage(
        &cold_store,
        "benchcmp/raft-d3-clip/chunks/0-8.bin",
        b"abcdefgh",
    )
    .await;
    engine
        .flush_cold(
            FlushColdRequest {
                stream_id: stream_id.clone(),
                chunk: chunk(0, 8, "benchcmp/raft-d3-clip/chunks/0-8.bin"),
            },
            placement(),
        )
        .await
        .expect("flush hot prefix");
    stage(
        &cold_store,
        "benchcmp/raft-d3-clip/external/tail.bin",
        b"WXYZ",
    )
    .await;
    engine
        .append_external(
            append_external_req(
                &stream_id,
                "benchcmp/raft-d3-clip/external/tail.bin",
                4,
                None,
            ),
            placement(),
        )
        .await
        .expect("append external tail");

    let read = engine
        .read_stream(read_req(stream_id, 0, 64), placement())
        .await
        .expect("read cold history");
    assert_eq!(read.payload, b"abcdefghWXYZ");
    engine.shutdown().await.expect("shutdown");
}

/// F14e: a stale flush on the Raft engine is rejected before it writes a
/// page entry, so no entry is left behind for an unreferenced chunk.
#[tokio::test]
async fn f14e_raft_stale_flush_leaves_no_page_entry() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let mut engine = cold_engine(cold_store.clone()).await;
    let stream_id = bsid("raft-stale-flush");
    engine
        .create_stream(
            CreateStreamRequest::new(stream_id.clone(), OCTET),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("create stream");
    engine
        .append(
            append_req(&stream_id, b"abcd", None),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("append");
    stage(
        &cold_store,
        "benchcmp/raft-stale-flush/chunks/live.bin",
        b"abcd",
    )
    .await;
    engine
        .flush_cold(
            FlushColdRequest {
                stream_id: stream_id.clone(),
                chunk: chunk(0, 4, "benchcmp/raft-stale-flush/chunks/live.bin"),
            },
            placement(),
        )
        .await
        .expect("live flush");
    stage(
        &cold_store,
        "benchcmp/raft-stale-flush/chunks/stale.bin",
        b"ab",
    )
    .await;
    let err = engine
        .flush_cold(
            FlushColdRequest {
                stream_id: stream_id.clone(),
                chunk: chunk(0, 2, "benchcmp/raft-stale-flush/chunks/stale.bin"),
            },
            placement(),
        )
        .await
        .expect_err("stale flush is rejected");
    assert_eq!(err.code(), Some(StreamErrorCode::InvalidColdFlush));

    let page = ColdStoreColdIndexPageStore::new(cold_store)
        .get_page(&ColdIndexPageKey {
            stream_id,
            generation: 0,
            page_id: 0,
        })
        .await
        .expect("read page")
        .expect("page exists");
    let paths = page
        .cold_chunks
        .iter()
        .map(|chunk| chunk.s3_path.as_str())
        .collect::<Vec<_>>();
    assert_eq!(paths, vec!["benchcmp/raft-stale-flush/chunks/live.bin"]);
    engine.shutdown().await.expect("shutdown");
}

/// D3 through the Raft engine: a rejected and a committed external append at
/// the same start; page repair keeps the committed (last-written) entry.
#[tokio::test]
async fn d3_raft_repair_keeps_the_last_written_external_entry_at_each_start() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let mut engine = cold_engine(cold_store.clone()).await;
    let stream_id = bsid("raft-d3-repair");
    engine
        .create_stream(
            CreateStreamRequest::new(stream_id.clone(), OCTET),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("create stream");
    engine
        .append(
            append_req(&stream_id, b"ab", Some("5")),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("append with stream seq");
    stage(
        &cold_store,
        "benchcmp/raft-d3-repair/external/rejected.bin",
        b"0123456789",
    )
    .await;
    engine
        .append_external(
            append_external_req(
                &stream_id,
                "benchcmp/raft-d3-repair/external/rejected.bin",
                10,
                Some("1"),
            ),
            placement(),
        )
        .await
        .expect_err("a regressed stream seq rejects the external append");
    stage(
        &cold_store,
        "benchcmp/raft-d3-repair/external/live.bin",
        b"WXYZ",
    )
    .await;
    engine
        .append_external(
            append_external_req(
                &stream_id,
                "benchcmp/raft-d3-repair/external/live.bin",
                4,
                None,
            ),
            placement(),
        )
        .await
        .expect("append external");

    let response = engine
        .repair_cold_index(
            ursula_runtime::RepairColdIndexRequest {
                after: None,
                max_streams: 16,
            },
            placement(),
        )
        .await
        .expect("repair");
    assert!(response.cycle_completed);
    assert_eq!(response.report.superseded_entries_dropped, 1);
    assert_eq!(response.report.pages_rewritten, 1);

    let read = engine
        .read_stream(read_req(stream_id, 2, 64), placement())
        .await
        .expect("read external");
    assert_eq!(read.payload, b"WXYZ");
    engine.shutdown().await.expect("shutdown");
}
