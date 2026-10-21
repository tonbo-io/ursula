//! The leadership fence of cold uploads in the Raft engine (F14j), on a
//! single-voter group whose leader steps down and campaigns again, so one
//! node leads in a later term:
//!
//! - R1: an upload proposed after its leadership ended is refused before
//!   OpenRaft appends anything, so its uploader may delete it.
//! - R2, the control: the same command proposed without the fence commits
//!   under the next leadership.
//! - R3: an upload command without a fence is refused, on the single and
//!   the batch path.
//! - R4: the fence covers an upload wrapped in `IfIncarnation`.
//! - R5: the fence is the committed leader id OpenRaft checks it against,
//!   and the write route carries it.

use std::sync::Arc;
use std::time::Duration;

use openraft::Config;
use openraft::ServerState;
use openraft::rt::WatchReceiver;
use ursula_runtime::AppendExternalRequest;
use ursula_runtime::ColdStore;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::CommittedLeader;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::ExternalPayloadRef;
use ursula_runtime::GroupEngine;
use ursula_runtime::GroupEngineError;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::LeadershipRefusal;
use ursula_runtime::Proposal;
use ursula_runtime::ProposedUpload;
use ursula_runtime::RouteWriteRequest;
use ursula_runtime::UploadFence;
use ursula_runtime::WriteRoute;
use ursula_shard::BucketStreamId;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;
use ursula_stream::StreamCommand;

use crate::RaftCommand;
use crate::RaftWal;
use crate::StepDown;
use crate::engine::RaftGroupEngine;

const OCTET: &str = "application/octet-stream";

fn placement() -> ShardPlacement {
    ShardPlacement {
        core_id: CoreId(0),
        shard_id: ShardId(0),
        raft_group_id: RaftGroupId(0),
    }
}

/// A single-voter group (node 1) with `cold_store`, on the per-core
/// journal in the returned directory, which must outlive the engine.
async fn engine(cold_store: Arc<ColdStore>) -> (RaftGroupEngine, tempfile::TempDir) {
    let wal_root = tempfile::tempdir().expect("WAL root");
    let config = Arc::new(
        Config {
            cluster_name: "ursula-cold-upload-fence".to_owned(),
            heartbeat_interval: 10,
            election_timeout_min: 30,
            election_timeout_max: 60,
            ..Default::default()
        }
        .validate()
        .expect("valid config"),
    );
    let log_store = RaftWal::start(
        wal_root.path(),
        ursula_config::WalFsync::Never,
        &ursula_shard::StaticShardMap::new(1, 1).expect("valid topology"),
    )
    .expect("start the WAL")
    .open(
        placement(),
        ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics(),
    )
    .expect("open the log store");
    let engine = RaftGroupEngine::new_single_node_with_log_store_and_metrics(
        placement(),
        1,
        config,
        log_store,
        None,
        Some(cold_store),
    )
    .await
    .expect("create raft group engine");
    (engine, wal_root)
}

/// A group with one stream, and the object a leader uploaded for it.
async fn staged_stream(
    name: &str,
) -> (
    RaftGroupEngine,
    tempfile::TempDir,
    BucketStreamId,
    ExternalPayloadRef,
    Arc<ColdStore>,
) {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let (mut engine, wal_root) = engine(cold_store.clone()).await;
    let stream_id = BucketStreamId::new("upload-fence", name);
    engine
        .create_stream(
            CreateStreamRequest::new(stream_id.clone(), OCTET),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("create stream");
    let path = ursula_runtime::new_external_payload_path(&stream_id);
    let object_size = cold_store
        .write_chunk(&path, b"payload")
        .await
        .expect("upload the payload");
    let payload = ExternalPayloadRef {
        s3_path: path,
        payload_len: 7,
        object_size,
    };
    (engine, wal_root, stream_id, payload, cold_store)
}

fn external_append(
    stream_id: &BucketStreamId,
    payload: &ExternalPayloadRef,
    upload: Arc<ProposedUpload>,
) -> AppendExternalRequest {
    AppendExternalRequest {
        stream_id: stream_id.clone(),
        content_type: OCTET.to_owned(),
        payload: payload.clone(),
        message_count: 0,
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms: 0,
        if_incarnation: None,
        upload,
    }
}

fn leads(engine: &RaftGroupEngine) -> UploadFence {
    engine.upload_fence().expect("the node leads its group")
}

fn leader_of(fence: UploadFence) -> CommittedLeader {
    match fence {
        UploadFence::Leader(leader) => Some(leader),
        UploadFence::Unreplicated => None,
    }
    .expect("a Raft group's fence names its leader")
}

fn last_log_index(engine: &RaftGroupEngine) -> Option<u64> {
    engine.raft.metrics().borrow_watched().last_log_index
}

/// Ends the leadership `before` without moving it: the leader steps down to
/// node 2, outside the group, and campaigns alone, so it leads again in a
/// later term. Returns the fence of the new leadership.
async fn reelect(engine: &RaftGroupEngine, before: UploadFence) -> UploadFence {
    let stepped = engine
        .step_down_and_campaign(2)
        .await
        .expect("step down and campaign");
    assert!(matches!(stepped, StepDown::Campaigned), "{stepped:?}");
    engine
        .raft
        .wait(Some(Duration::from_secs(5)))
        .metrics(
            // Its leadership's first entry, the blank one, applied: the log
            // holds nothing of the new term the test did not write.
            |metrics| {
                metrics.state == ServerState::Leader
                    && metrics.current_term > before.term()
                    && metrics
                        .last_applied
                        .is_some_and(|applied| applied.leader_id.term == metrics.current_term)
            },
            "the node leads again in a later term",
        )
        .await
        .expect("the node leads again");
    let after = leads(engine);
    assert_eq!(leader_of(after).node_id(), leader_of(before).node_id());
    assert!(leader_of(after).term() > leader_of(before).term());
    after
}

/// A refusal that says nothing was proposed and names no leader.
fn refused_before_proposal(error: &GroupEngineError) -> bool {
    error.refused_before_proposal()
        && error.applied_nothing()
        && matches!(
            error,
            GroupEngineError::NotLeader(LeadershipRefusal {
                leader: None,
                proposal: Proposal::NotProposed,
                ..
            })
        )
}

/// R1: an upload fenced to a leadership that ended, by a re-election of the
/// same node, is refused before OpenRaft appends it. The refusal says that
/// nothing was proposed, which lets its uploader delete the object. The same
/// upload fenced to the current leadership applies.
#[tokio::test]
async fn an_upload_proposed_after_its_leadership_ended_is_refused_before_append() {
    let (mut engine, _wal_root, stream_id, payload, cold_store) = staged_stream("r1").await;
    let before = leads(&engine);
    let after = reelect(&engine, before).await;
    let appended = last_log_index(&engine);

    let refused = engine
        .append_external(
            external_append(&stream_id, &payload, ProposedUpload::new(before)),
            placement(),
        )
        .await
        .expect_err("the fence refuses an upload of an ended leadership");
    assert!(refused_before_proposal(&refused), "{refused:?}");
    // No entry names the object: OpenRaft appended nothing, and the stream
    // holds none of its bytes.
    assert_eq!(
        last_log_index(&engine),
        appended,
        "OpenRaft appended nothing"
    );
    let head = engine
        .head_stream(
            HeadStreamRequest {
                stream_id: stream_id.clone(),
                now_ms: 0,
                linearizable: false,
                read_index: None,
            },
            placement(),
        )
        .await
        .expect("head");
    assert_eq!(head.tail_offset, 0, "nothing of the refused upload applied");
    // The refusal says nothing applied, so the uploader deletes its object,
    // as the front door and the runtime do.
    assert!(refused.applied_nothing());
    cold_store
        .delete_chunk(&payload.s3_path)
        .await
        .expect("delete the refused upload");
    assert!(
        cold_store.object_size(&payload.s3_path).await.is_err(),
        "the refused upload is gone"
    );

    // A new upload fenced to the current leadership applies.
    let path = ursula_runtime::new_external_payload_path(&stream_id);
    let object_size = cold_store
        .write_chunk(&path, b"payload")
        .await
        .expect("upload again");
    let again = ExternalPayloadRef {
        s3_path: path,
        payload_len: 7,
        object_size,
    };
    let applied = engine
        .append_external(
            external_append(&stream_id, &again, ProposedUpload::new(after)),
            placement(),
        )
        .await
        .expect("the upload applies under its own leadership");
    assert_eq!(applied.start_offset, 0);
    assert_eq!(applied.next_offset, 7);
}

/// R2, the control of R1: the same command handed to OpenRaft without the
/// fence commits under the next leadership, so a deleter that trusts the
/// leadership an upload began under would delete an object a committed
/// entry names.
#[tokio::test]
async fn an_unfenced_upload_commits_under_the_next_leadership() {
    let (engine, _wal_root, stream_id, payload, _cold_store) = staged_stream("r2").await;
    let before = leads(&engine);
    let after = reelect(&engine, before).await;

    let response = engine
        .raft
        .client_write(RaftCommand::Stream(StreamCommand::from(external_append(
            &stream_id,
            &payload,
            ProposedUpload::new(before),
        ))))
        .await
        .expect("OpenRaft takes an unfenced write");
    let committed = response.log_id.leader_id;
    let committed = CommittedLeader::new(committed.term, committed.node_id);
    assert_eq!(UploadFence::Leader(committed), after);
    assert_ne!(UploadFence::Leader(committed), before);
}

/// R3: an upload command that reaches the engine without a fence is refused
/// before proposal, through the unfenced write, an upload of no
/// leadership, and the batch path.
#[tokio::test]
async fn an_upload_command_without_a_fence_is_refused() {
    let (engine, _wal_root, stream_id, payload, _cold_store) = staged_stream("r3").await;
    let command = StreamCommand::from(external_append(
        &stream_id,
        &payload,
        ProposedUpload::new(UploadFence::Unreplicated),
    ));
    let appended = last_log_index(&engine);

    let unfenced = engine
        .write(command.clone())
        .await
        .expect_err("an unfenced upload is refused");
    assert!(refused_before_proposal(&unfenced), "{unfenced:?}");

    let unreplicated = engine
        .write_upload(
            command.clone(),
            &ProposedUpload::new(UploadFence::Unreplicated),
        )
        .await
        .expect_err("an upload of no leadership is refused");
    assert!(refused_before_proposal(&unreplicated), "{unreplicated:?}");

    let batch = crate::forward::write_commands_on_raft(
        engine.raft_handle(),
        engine.fence(),
        &engine.history,
        vec![RaftCommand::Stream(command)],
    )
    .await
    .expect_err("a batch carries no fence, so it refuses an upload");
    assert!(refused_before_proposal(&batch), "{batch:?}");

    assert_eq!(last_log_index(&engine), appended, "nothing was proposed");
}

/// R4: the fence covers an upload wrapped in `IfIncarnation`, whichever
/// path proposes it.
#[tokio::test]
async fn the_fence_covers_an_if_incarnation_wrapped_upload() {
    let (mut engine, _wal_root, stream_id, payload, _cold_store) = staged_stream("r4").await;
    let incarnation = engine
        .head_stream(
            HeadStreamRequest {
                stream_id: stream_id.clone(),
                now_ms: 0,
                linearizable: false,
                read_index: None,
            },
            placement(),
        )
        .await
        .expect("head")
        .incarnation;
    let wrapped = |upload| {
        let mut request = external_append(&stream_id, &payload, upload);
        request.if_incarnation = Some(incarnation);
        StreamCommand::from(request)
    };
    let StreamCommand::IfIncarnation { .. } =
        wrapped(ProposedUpload::new(UploadFence::Unreplicated))
    else {
        panic!("a precondition wraps the command");
    };
    let before = leads(&engine);
    let after = reelect(&engine, before).await;

    let unfenced = engine
        .write(wrapped(ProposedUpload::new(after)))
        .await
        .expect_err("an unfenced wrapped upload is refused");
    assert!(refused_before_proposal(&unfenced), "{unfenced:?}");

    let stale = ProposedUpload::new(before);
    let refused = engine
        .write_upload(wrapped(stale.clone()), &stale)
        .await
        .expect_err("a wrapped upload of an ended leadership is refused");
    assert!(refused_before_proposal(&refused), "{refused:?}");

    let current = ProposedUpload::new(after);
    engine
        .write_upload(wrapped(current.clone()), &current)
        .await
        .expect("a wrapped upload applies under its own leadership");
}

/// R5: the fence a leader takes from its metrics is the committed leader id
/// OpenRaft checks `with_leader` against, before and after a re-election,
/// and the write route of the front door and the engine carries it.
#[tokio::test]
async fn the_upload_fence_is_the_committed_leader_id() {
    let (mut engine, _wal_root, stream_id, _payload, _cold_store) = staged_stream("r5").await;
    let mut fence = leads(&engine);
    for round in 0..2 {
        let committed = engine
            .raft
            .as_leader()
            .expect("the node leads")
            .to_committed_leader_id();
        assert_eq!(
            leader_of(fence),
            CommittedLeader::new(committed.term, committed.node_id),
            "round {round}"
        );
        let route = engine
            .route_write(
                RouteWriteRequest {
                    stream_id: stream_id.clone(),
                    early_answer: None,
                },
                placement(),
            )
            .await
            .expect("route");
        assert!(
            matches!(route, WriteRoute::Leader { fence: routed, .. } if routed == fence),
            "round {round}: {route:?}"
        );
        fence = reelect(&engine, fence).await;
    }
}
