//! DST scenario `cold_uploads_stay_fenced` (bounded-stream-state F14j).
//!
//! A leader of a three-node group uploads cold objects, an external
//! payload or a flushed chunk, each fenced to the leadership it leads in
//! when it plans the upload. Between the PUT and the proposal the scenario
//! ends that leadership in one of three ways, by round:
//!
//! - no change: the proposal applies.
//! - a re-election of the uploader itself: it steps down and leads again in
//!   a later term, as a slow PUT across an election would see. OpenRaft
//!   refuses the proposal before it appends anything.
//! - a real leader change by isolation: the uploader follows the new leader
//!   when it proposes, and refuses before proposal.
//!
//! The uploader deletes every upload whose refusal says nothing applied, as
//! the runtime and the front door do. The fence ledger
//! (`ursula_runtime::cold_uploads::fence_ledger`) records the fence of each
//! proposed upload and the committed leader of every replica's apply of an
//! entry that names one. The invariant: every applied naming of an upload
//! carries the leadership the upload was fenced to (K3), every acknowledged
//! byte reads back on every replica, and every refused upload is gone. The
//! negative control turns `Defence::UploadFence` off, and an upload that
//! waited across the uploader's re-election commits under the later
//! leadership.

use std::sync::Arc;
use std::time::Duration;

use openraft::ServerState;
use openraft::rt::WatchReceiver;
use ursula_raft::InProcessRaftNetworkPolicy;
use ursula_raft::RaftGroupEngine;
use ursula_runtime::AppendExternalRequest;
use ursula_runtime::AppendRequest;
use ursula_runtime::ColdChunkRef;
use ursula_runtime::ColdStore;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::ExternalPayloadRef;
use ursula_runtime::FlushColdRequest;
use ursula_runtime::GroupEngine;
use ursula_runtime::GroupEngineError;
use ursula_runtime::PlanColdFlushRequest;
use ursula_runtime::UploadFence;
use ursula_runtime::cold_uploads::fence_ledger;
use ursula_runtime::cold_uploads::fence_ledger::UnfencedNaming;
use ursula_shard::BucketStreamId;
use ursula_stream::sim_controls::Defence;
use ursula_stream::sim_controls::set_off;

use super::build_three_node_cluster_with_cold_store;
use super::leader_upload;
use super::placement;
use super::raft_scenarios::apply_barrier;
use super::read_local_payload_eventually;
use super::run_with_madsim;
use super::sim_cold_store;
use super::sim_network_policy;

/// The invariant this scenario checks.
pub(super) const INVARIANT: &str = "cold_uploads_stay_fenced";

/// The default seeds. `COLD_UPLOAD_FENCE_SEEDS=start..=end` widens them.
const COLD_UPLOAD_FENCE_SEEDS: [u64; 4] = [11, 29, 47, 61];

const ROUNDS: u64 = 6;

/// How the leadership an upload was fenced to ends before its proposal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Churn {
    None,
    Reelection,
    LeaderChange,
}

/// What a round uploads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Upload {
    ExternalAppend,
    Flush,
}

/// The proposal of a round's upload.
enum UploadProposal {
    Append(AppendExternalRequest),
    Flush(FlushColdRequest),
}

/// How a round's proposal ended for its uploader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Applied,
    /// Refused with nothing applied: the uploader deleted its object.
    Refused,
    /// Neither: the uploader kept its object.
    Ambiguous,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Report {
    rounds: Vec<(Churn, Upload, Outcome)>,
    unfenced: Vec<UnfencedNaming>,
}

fn engine_index(node_id: u64) -> usize {
    node_id
        .checked_sub(1)
        .and_then(|index| usize::try_from(index).ok())
        .expect("node ids start at 1")
}

fn node_id(index: usize) -> u64 {
    u64::try_from(index)
        .expect("node index fits u64")
        .saturating_add(1)
}

/// The fence of `engine` once it leads with the blank entry of its term
/// applied, so a write it proposes is not held for its first quorum.
fn settled_fence(engine: &RaftGroupEngine) -> Option<UploadFence> {
    let metrics = engine.raft_handle().metrics().borrow_watched().clone();
    let settled = metrics.state == ServerState::Leader
        && metrics
            .last_applied
            .is_some_and(|applied| applied.leader_id.term == metrics.current_term);
    settled.then(|| engine.upload_fence()).flatten()
}

/// The node that leads the group with its term's blank entry applied.
async fn settled_leader(engines: &[RaftGroupEngine]) -> u64 {
    for _ in 0..200 {
        if let Some(index) = engines
            .iter()
            .position(|engine| settled_fence(engine).is_some())
        {
            return node_id(index);
        }
        madsim::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("{INVARIANT}: no leader settled");
}

/// Ends the uploader's leadership `ended` and has the uploader lead again,
/// in a later term: it steps down and campaigns, and when another node wins
/// that election, that node hands the leadership back.
async fn reelect(engines: &[RaftGroupEngine], uploader: u64, ended: UploadFence) {
    let target = (uploader % 3).saturating_add(1);
    engines[engine_index(uploader)]
        .step_down_and_campaign(target)
        .await
        .expect("the uploader steps down and campaigns");
    for _ in 0..400 {
        if settled_fence(&engines[engine_index(uploader)])
            .is_some_and(|fence| fence.term() > ended.term())
        {
            return;
        }
        for (index, engine) in engines.iter().enumerate() {
            if node_id(index) != uploader
                && settled_fence(engine).is_some()
                && let Err(error) = engine
                    .raft_handle()
                    .trigger()
                    .transfer_leader(uploader)
                    .await
            {
                tracing::debug!(%error, "a leadership transfer back to the uploader failed");
            }
        }
        madsim::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("{INVARIANT}: the uploader never led again after its re-election");
}

/// Moves the leadership away from the uploader: it is cut off until the
/// others elect a leader, then rejoins and follows it.
async fn change_leader(
    engines: &[RaftGroupEngine],
    policy: &InProcessRaftNetworkPolicy,
    uploader: u64,
) {
    for peer in (1..=3).filter(|peer| *peer != uploader) {
        policy.partition_bidirectional(uploader, peer);
    }
    let mut replacement = None;
    for _ in 0..400 {
        replacement = engines
            .iter()
            .enumerate()
            .find(|(index, engine)| node_id(*index) != uploader && settled_fence(engine).is_some())
            .map(|(index, _)| node_id(index));
        if replacement.is_some() {
            break;
        }
        madsim::time::sleep(Duration::from_millis(25)).await;
    }
    let replacement = replacement
        .unwrap_or_else(|| panic!("{INVARIANT}: the others never elected a replacement"));
    policy.clear();
    engines[engine_index(uploader)]
        .raft_handle()
        .wait(Some(Duration::from_secs(10)))
        .metrics(
            |metrics| {
                metrics.state != ServerState::Leader && metrics.current_leader == Some(replacement)
            },
            "the uploader follows the replacement",
        )
        .await
        .expect("the uploader follows the replacement");
}

/// The outcome of a proposal, deleting the upload at `path` when nothing of
/// it applied.
async fn settle(
    cold_store: &ColdStore,
    path: &str,
    result: Result<Result<(), GroupEngineError>, madsim::time::error::Elapsed>,
) -> Outcome {
    match result {
        Ok(Ok(())) => Outcome::Applied,
        Ok(Err(error)) if error.applied_nothing() => {
            cold_store
                .delete_chunk(path)
                .await
                .expect("delete a refused upload");
            assert!(
                cold_store.object_size(path).await.is_err(),
                "{INVARIANT}: a refused upload {path} is gone"
            );
            Outcome::Refused
        }
        Ok(Err(_)) | Err(_) => Outcome::Ambiguous,
    }
}

async fn append_inline(engines: &mut [RaftGroupEngine], stream: &BucketStreamId, bytes: &[u8]) {
    for _ in 0..50 {
        let leader = settled_leader(engines).await;
        if engines[engine_index(leader)]
            .append(
                AppendRequest::from_bytes(stream.clone(), bytes.to_vec()),
                placement(),
                ColdWriteAdmission::default(),
            )
            .await
            .is_ok()
        {
            return;
        }
        madsim::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("{INVARIANT}: an inline append never applied");
}

async fn cold_uploads_stay_fenced(seed: u64, fence_off: bool) -> Report {
    set_off(Defence::UploadFence, fence_off);
    fence_ledger::arm();
    let cold_store: Arc<ColdStore> = Arc::new(sim_cold_store());
    let policy = sim_network_policy();
    let (_registry, mut engines, leader_id) =
        build_three_node_cluster_with_cold_store(policy.clone(), Some(cold_store.clone())).await;
    let stream = BucketStreamId::new("simulated", "cold-uploads-stay-fenced");
    engines[engine_index(leader_id)]
        .create_stream(
            CreateStreamRequest::new(stream.clone(), "application/octet-stream"),
            placement(),
            ColdWriteAdmission::default(),
        )
        .await
        .expect("create stream");
    let mut acknowledged = Vec::new();
    let mut rounds = Vec::new();
    for round in 0..ROUNDS {
        let churn = match seed.wrapping_add(round) % 3 {
            0 => Churn::None,
            1 => Churn::Reelection,
            _ => Churn::LeaderChange,
        };
        let upload = if (seed / 3).wrapping_add(round) % 2 == 0 {
            Upload::ExternalAppend
        } else {
            Upload::Flush
        };
        let byte = b'a'.saturating_add(u8::try_from(round).expect("few rounds"));
        let len = 16_usize.saturating_add(usize::try_from(round).expect("few rounds"));
        let bytes = vec![byte; len];
        if upload == Upload::Flush {
            // A flush uploads what an inline append made hot.
            append_inline(&mut engines, &stream, &bytes).await;
            acknowledged.extend_from_slice(&bytes);
        }
        let uploader = settled_leader(&engines).await;
        let uploader_index = engine_index(uploader);
        let proposed = leader_upload(&engines[uploader_index]);
        let fence = proposed.fence();
        // The PUT, after the fence and before the proposal.
        let (path, request) = match upload {
            Upload::ExternalAppend => {
                let path = ursula_runtime::new_external_payload_path(&stream);
                let object_size = cold_store
                    .write_chunk(&path, &bytes)
                    .await
                    .expect("upload the external payload");
                let request = AppendExternalRequest {
                    stream_id: stream.clone(),
                    content_type: "application/octet-stream".to_owned(),
                    payload: ExternalPayloadRef {
                        s3_path: path.clone(),
                        payload_len: u64::try_from(len).expect("len fits u64"),
                        object_size,
                    },
                    message_count: 0,
                    close_after: false,
                    stream_seq: None,
                    producer: None,
                    now_ms: 0,
                    if_incarnation: None,
                    upload: proposed,
                };
                (path, UploadProposal::Append(request))
            }
            Upload::Flush => {
                let candidate = engines[uploader_index]
                    .plan_cold_flush(
                        PlanColdFlushRequest {
                            stream_id: stream.clone(),
                            min_hot_bytes: 1,
                            max_flush_bytes: 1 << 20,
                        },
                        placement(),
                    )
                    .await
                    .expect("plan a flush")
                    .expect("hot bytes to flush");
                let path = ursula_runtime::new_cold_chunk_path_in_generation(
                    &stream,
                    candidate.cold_generation,
                    candidate.start_offset,
                    candidate.end_offset,
                );
                let object_size = cold_store
                    .write_chunk(&path, &candidate.payload)
                    .await
                    .expect("upload the chunk");
                let request = FlushColdRequest {
                    stream_id: stream.clone(),
                    chunk: ColdChunkRef {
                        start_offset: candidate.start_offset,
                        end_offset: candidate.end_offset,
                        s3_path: path.clone(),
                        object_size,
                        object_offset: 0,
                        shared_object: false,
                        payload_digest: candidate.payload_digest,
                    },
                    cold_generation: candidate.cold_generation,
                    upload: proposed,
                };
                (path, UploadProposal::Flush(request))
            }
        };
        match churn {
            Churn::None => {}
            Churn::Reelection => reelect(&engines, uploader, fence).await,
            Churn::LeaderChange => change_leader(&engines, &policy, uploader).await,
        }
        let engine = &mut engines[uploader_index];
        let result = match request {
            UploadProposal::Append(append) => madsim::time::timeout(
                Duration::from_secs(5),
                engine.append_external(append, placement()),
            )
            .await
            .map(|result| result.map(drop)),
            UploadProposal::Flush(flush) => madsim::time::timeout(
                Duration::from_secs(5),
                engine.flush_cold(flush, placement()),
            )
            .await
            .map(|result| result.map(drop)),
        };
        let outcome = settle(&cold_store, &path, result).await;
        if outcome == Outcome::Applied && upload == Upload::ExternalAppend {
            acknowledged.extend_from_slice(&bytes);
        }
        if !fence_off {
            // With the fence, a leadership that ended refuses the upload
            // definitely, and one that did not lets it apply.
            let expected = match churn {
                Churn::None => Outcome::Applied,
                Churn::Reelection | Churn::LeaderChange => Outcome::Refused,
            };
            assert_eq!(
                outcome, expected,
                "{INVARIANT}: seed {seed} round {round} {churn:?} {upload:?}"
            );
        }
        rounds.push((churn, upload, outcome));
    }

    let leader = settled_leader(&engines).await;
    apply_barrier(
        &engines,
        engine_index(leader),
        "every replica applied the uploads",
    )
    .await;
    let unfenced = fence_ledger::disarm();
    set_off(Defence::UploadFence, false);
    if !fence_off {
        assert!(
            unfenced.is_empty(),
            "{INVARIANT}: seed {seed}: an upload applied under another leadership: {unfenced:?}"
        );
        for (index, engine) in engines.iter().enumerate() {
            read_local_payload_eventually(
                engine,
                node_id(index),
                &stream,
                0,
                acknowledged.len().saturating_add(16),
                &acknowledged,
                "a replica reads every acknowledged byte",
            )
            .await;
        }
    }
    Report { rounds, unfenced }
}

fn seeds() -> Vec<u64> {
    std::env::var("COLD_UPLOAD_FENCE_SEEDS")
        .ok()
        .and_then(|range| {
            let (start, end) = range.split_once("..=")?;
            Some((start.parse().ok()?..=end.parse().ok()?).collect())
        })
        .unwrap_or_else(|| COLD_UPLOAD_FENCE_SEEDS.to_vec())
}

#[test]
fn cold_uploads_stay_fenced_under_leader_churn() {
    let _guard = super::tests::sim_test_guard();
    for seed in seeds() {
        let report = run_with_madsim(seed, cold_uploads_stay_fenced(seed, false));
        assert!(
            report
                .rounds
                .iter()
                .any(|(churn, _, outcome)| *churn == Churn::Reelection
                    && *outcome == Outcome::Refused),
            "seed {seed}: no re-election refused an upload: {report:?}"
        );
    }
}

/// The scenario runs the same way twice per seed.
#[test]
fn cold_uploads_stay_fenced_replays() {
    let _guard = super::tests::sim_test_guard();
    let seed = COLD_UPLOAD_FENCE_SEEDS[0];
    let first = run_with_madsim(seed, cold_uploads_stay_fenced(seed, false));
    let second = run_with_madsim(seed, cold_uploads_stay_fenced(seed, false));
    assert_eq!(first, second, "seed {seed}");
}

/// Without the fence, an upload that waited across its uploader's
/// re-election commits under the later leadership: the ledger sees a
/// naming applied under a leadership other than its upload's.
#[test]
fn negative_control_an_unfenced_upload_commits_under_a_later_leadership() {
    let _guard = super::tests::sim_test_guard();
    let seed = COLD_UPLOAD_FENCE_SEEDS[0];
    let report = run_with_madsim(seed, cold_uploads_stay_fenced(seed, true));
    assert!(
        !report.unfenced.is_empty(),
        "with Defence::UploadFence off, `{INVARIANT}` holds on seed {seed}: {report:?}"
    );
    assert!(
        report.unfenced.iter().all(
            |naming| naming.applied_under.term() > naming.fenced_to.term()
                && naming.applied_under.node_id() == naming.fenced_to.node_id()
        ),
        "every broken fence is a later leadership of the same node: {report:?}"
    );
}
