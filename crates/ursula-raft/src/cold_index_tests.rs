//! Raft-engine wiring of the cold index: what the Raft engine does itself
//! rather than through the shared state machine and page helpers. Openraft
//! snapshot build/install of a regressed cold frontier (D1), the leader's
//! stale-flush check before its page write (F14e), the page cache shared by
//! the read path and apply, and cold-index repair. The cold-path contracts
//! themselves are pinned once, against the in-memory engine, by
//! `ursula-runtime`'s cold-path tests.

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
use ursula_runtime::AppendRequest;
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
        leader_only: false,
        read_index: None,
    }
}

async fn cold_engine(cold_store: Arc<ColdStore>) -> RaftGroupEngine {
    let config = Arc::new(
        Config {
            cluster_name: "ursula-cold-index".to_owned(),
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
async fn openraft_snapshot_with_regressed_frontier_builds_and_installs() {
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
        },
        StreamCommand::FlushCold {
            // C7: the group's first incarnation, created at 0, is 1.
            cold_generation: 1,
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

/// F14e: a stale flush on the Raft engine is rejected before it writes a
/// page entry, so no entry is left behind for an unreferenced chunk.
#[tokio::test]
async fn stale_flush_leaves_no_page_entry() {
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
    let generation = engine
        .local_cold_index_generation(stream_id.clone())
        .await
        .expect("cold generation");
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
                cold_generation: generation,
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
                cold_generation: generation,
                stream_id: stream_id.clone(),
                chunk: chunk(0, 2, "benchcmp/raft-stale-flush/chunks/stale.bin"),
            },
            placement(),
        )
        .await
        .expect_err("stale flush is rejected");
    assert_eq!(err.code(), Some(StreamErrorCode::InvalidColdFlush));

    // C7/F14g: the page lives under the stream's incarnation generation.
    let key = cold_store
        .list_cold_index_pages()
        .await
        .expect("list pages")
        .into_iter()
        .find(|key| key.stream_id == stream_id)
        .expect("page key");
    let page = ColdStoreColdIndexPageStore::new(cold_store)
        .get_page(&key)
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

/// Wave-1 follow-up: the Raft read path and the state machine share one
/// cold-index page cache, so the page invalidation that runs when a
/// replicated `FlushCold` applies (on every replica) also drops the pages the
/// read path cached.
#[tokio::test]
async fn raft_read_path_shares_the_page_cache_that_apply_invalidates() {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let mut engine = cold_engine(cold_store.clone()).await;
    let stream_id = bsid("raft-shared-cache");
    engine
        .create_stream(
            CreateStreamRequest::new(stream_id.clone(), OCTET),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("create stream");
    let generation = engine
        .local_cold_index_generation(stream_id.clone())
        .await
        .expect("cold generation");
    engine
        .append(
            append_req(&stream_id, b"abcdefgh", None),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("append");
    for (start, end, path) in [
        (0, 4, "benchcmp/raft-shared-cache/chunks/abcd.bin"),
        (4, 8, "benchcmp/raft-shared-cache/chunks/efgh.bin"),
    ] {
        let payload = &b"abcdefgh"
            [usize::try_from(start).expect("start")..usize::try_from(end).expect("end")];
        stage(&cold_store, path, payload).await;
        if start == 4 {
            // Cache the stream's page through the read path first.
            let read = engine
                .read_stream(read_req(stream_id.clone(), 0, 4), placement())
                .await
                .expect("cold read");
            assert_eq!(read.payload, b"abcd");
            let cache = engine.cold_index_cache.as_ref().expect("page cache");
            assert_eq!(cache.cached_page_count(), 1);
        }
        engine
            .flush_cold(
                FlushColdRequest {
                    cold_generation: generation,
                    stream_id: stream_id.clone(),
                    chunk: chunk(start, end, path),
                },
                placement(),
            )
            .await
            .expect("flush");
    }
    let cache = engine.cold_index_cache.as_ref().expect("page cache");
    assert_eq!(cache.cached_page_count(), 0);
    let read = engine
        .read_stream(read_req(stream_id, 0, 8), placement())
        .await
        .expect("read both chunks");
    assert_eq!(read.payload, b"abcdefgh");
    engine.shutdown().await.expect("shutdown");
}
