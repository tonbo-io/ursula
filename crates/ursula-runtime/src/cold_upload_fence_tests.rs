//! The runtime's background uploads under a leadership fence (F14j): a
//! flush takes its fence after its plan and before its PUT, and an upload
//! its fence refuses is deleted, a pack included. The engine here stands in
//! for OpenRaft's `with_leader` check: it refuses a flush whose fence is not
//! the leadership it leads in when the flush is proposed.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;

use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardPlacement;

use crate::cold_store::ColdStore;
use crate::cold_store::ColdStoreEvent;
use crate::cold_store::DEFAULT_CONTENT_TYPE;
use crate::cold_uploads::CommittedLeader;
use crate::cold_uploads::UploadFence;
use crate::cold_uploads::upload_not_leading;
use crate::command::GroupSnapshot;
use crate::engine::GroupAppendFuture;
use crate::engine::GroupBucketUsageFuture;
use crate::engine::GroupCloseStreamFuture;
use crate::engine::GroupCreateStreamFuture;
use crate::engine::GroupDeleteStreamFuture;
use crate::engine::GroupEngine;
use crate::engine::GroupEngineCreateFuture;
use crate::engine::GroupEngineError;
use crate::engine::GroupEngineFactory;
use crate::engine::GroupEngineMetrics;
use crate::engine::GroupFlushColdFuture;
use crate::engine::GroupHeadStreamFuture;
use crate::engine::GroupInstallSnapshotFuture;
use crate::engine::GroupPlanNextColdFlushBatchFuture;
use crate::engine::GroupReadStreamFuture;
use crate::engine::GroupRouteWriteFuture;
use crate::engine::GroupSnapshotFuture;
use crate::engine::GroupTouchStreamAccessFuture;
use crate::engine::in_memory::InMemoryGroupEngine;
use crate::error::RuntimeError;
use crate::request::AppendRequest;
use crate::request::CloseStreamRequest;
use crate::request::ColdWriteAdmission;
use crate::request::CreateStreamRequest;
use crate::request::DeleteStreamRequest;
use crate::request::FlushColdRequest;
use crate::request::HeadStreamRequest;
use crate::request::PlanGroupColdFlushRequest;
use crate::request::ReadStreamRequest;
use crate::request::RouteWriteRequest;
use crate::runtime::RuntimeConfig;
use crate::runtime::ShardRuntime;

/// The leadership the engine leads in, which the test moves.
type Leadership = Arc<Mutex<Option<UploadFence>>>;

/// An answer the engine gives every flush instead of proposing it.
type Forced = Arc<Mutex<Option<GroupEngineError>>>;

/// How many more flushes the engine publishes before its leadership moves
/// to the next term, as a re-election between two slices of a pack would.
type MovesAfter = Arc<Mutex<Option<usize>>>;

fn leader(term: u64) -> Option<UploadFence> {
    Some(UploadFence::Leader(CommittedLeader::new(term, 1)))
}

fn set(leadership: &Leadership, fence: Option<UploadFence>) {
    *leadership.lock().unwrap_or_else(PoisonError::into_inner) = fence;
}

fn current(leadership: &Leadership) -> Option<UploadFence> {
    *leadership.lock().unwrap_or_else(PoisonError::into_inner)
}

/// An in-memory engine that leads in `leadership` and proposes a flush only
/// under the fence it carries.
struct FencedEngine {
    inner: InMemoryGroupEngine,
    leadership: Leadership,
    forced: Forced,
    moves_after: MovesAfter,
}

impl GroupEngine for FencedEngine {
    // Its flush plan always runs: a leadership that ends after the plan is
    // what the fence catches.
    fn accepts_local_writes(&self) -> bool {
        true
    }

    fn upload_fence(&self) -> Option<UploadFence> {
        current(&self.leadership)
    }

    fn route_write<'a>(
        &'a mut self,
        request: RouteWriteRequest,
        placement: ShardPlacement,
    ) -> GroupRouteWriteFuture<'a> {
        self.inner.route_write(request, placement)
    }

    fn bucket_usage<'a>(&'a mut self, placement: ShardPlacement) -> GroupBucketUsageFuture<'a> {
        self.inner.bucket_usage(placement)
    }

    fn create_stream<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupCreateStreamFuture<'a> {
        self.inner.create_stream(request, placement, admission)
    }

    fn head_stream<'a>(
        &'a mut self,
        request: HeadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupHeadStreamFuture<'a> {
        self.inner.head_stream(request, placement)
    }

    fn read_stream<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupReadStreamFuture<'a> {
        self.inner.read_stream(request, placement)
    }

    fn touch_stream_access<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
        placement: ShardPlacement,
    ) -> GroupTouchStreamAccessFuture<'a> {
        self.inner
            .touch_stream_access(stream_id, now_ms, renew_ttl, placement)
    }

    fn close_stream<'a>(
        &'a mut self,
        request: CloseStreamRequest,
        placement: ShardPlacement,
    ) -> GroupCloseStreamFuture<'a> {
        self.inner.close_stream(request, placement)
    }

    fn delete_stream<'a>(
        &'a mut self,
        request: DeleteStreamRequest,
        placement: ShardPlacement,
    ) -> GroupDeleteStreamFuture<'a> {
        self.inner.delete_stream(request, placement)
    }

    fn append<'a>(
        &'a mut self,
        request: AppendRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupAppendFuture<'a> {
        self.inner.append(request, placement, admission)
    }

    fn plan_next_cold_flush_batch<'a>(
        &'a mut self,
        request: PlanGroupColdFlushRequest,
        placement: ShardPlacement,
        max_candidates: usize,
    ) -> GroupPlanNextColdFlushBatchFuture<'a> {
        self.inner
            .plan_next_cold_flush_batch(request, placement, max_candidates)
    }

    fn flush_cold<'a>(
        &'a mut self,
        request: FlushColdRequest,
        placement: ShardPlacement,
    ) -> GroupFlushColdFuture<'a> {
        if let Some(error) = self
            .forced
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
        {
            return Box::pin(async move { Err(error) });
        }
        if current(&self.leadership) != Some(request.upload.fence()) {
            return Box::pin(async { Err(upload_not_leading()) });
        }
        {
            let mut moves_after = self
                .moves_after
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(left) = moves_after.as_mut() {
                *left = left.saturating_sub(1);
                if *left == 0 {
                    *moves_after = None;
                    let next = current(&self.leadership)
                        .map_or(1, UploadFence::term)
                        .saturating_add(1);
                    set(&self.leadership, leader(next));
                }
            }
        }
        self.inner.flush_cold(request, placement)
    }

    fn snapshot<'a>(&'a mut self, placement: ShardPlacement) -> GroupSnapshotFuture<'a> {
        self.inner.snapshot(placement)
    }

    fn install_snapshot<'a>(
        &'a mut self,
        snapshot: GroupSnapshot,
    ) -> GroupInstallSnapshotFuture<'a> {
        self.inner.install_snapshot(snapshot)
    }
}

struct FencedEngineFactory {
    cold_store: Arc<ColdStore>,
    leadership: Leadership,
    forced: Forced,
    moves_after: MovesAfter,
}

impl GroupEngineFactory for FencedEngineFactory {
    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        _metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        let mut inner = InMemoryGroupEngine::with_cold_store(self.cold_store.clone());
        inner.start_genesis(placement, 0);
        let leadership = self.leadership.clone();
        let forced = self.forced.clone();
        let moves_after = self.moves_after.clone();
        Box::pin(async move {
            let engine: Box<dyn GroupEngine> = Box::new(FencedEngine {
                inner,
                leadership,
                forced,
                moves_after,
            });
            Ok(engine)
        })
    }
}

/// What a test makes the engine do.
#[derive(Default)]
struct Controls {
    forced: Forced,
    moves_after: MovesAfter,
}

/// The objects a cold store wrote and deleted.
type Seen = Arc<Mutex<(Vec<String>, Vec<String>)>>;

/// A runtime of one group whose engine leads in term 1, the leadership the
/// test moves, and what its cold store writes and deletes. When `reelect`
/// is set, each finished PUT moves the leadership to the next term, as a
/// re-election of the same node during the upload would.
fn spawn(reelect: bool) -> (ShardRuntime, Leadership, Seen, Controls) {
    let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
    let leadership: Leadership = Arc::new(Mutex::new(leader(1)));
    let controls = Controls::default();
    let seen = Seen::default();
    let recorder = seen.clone();
    let moved = leadership.clone();
    cold_store.set_observer(move |event| match event {
        ColdStoreEvent::WriteChunkComplete { path, .. } => {
            recorder
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .0
                .push(path);
            if reelect {
                let next = current(&moved)
                    .map_or(1, UploadFence::term)
                    .saturating_add(1);
                set(&moved, leader(next));
            }
        }
        ColdStoreEvent::DeleteChunkComplete { path } => recorder
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .1
            .push(path),
        _ => {}
    });
    let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1),
        FencedEngineFactory {
            cold_store: cold_store.clone(),
            leadership: leadership.clone(),
            forced: controls.forced.clone(),
            moves_after: controls.moves_after.clone(),
        },
        Some(cold_store),
    )
    .expect("spawn runtime");
    (runtime, leadership, seen, controls)
}

async fn create_and_append(runtime: &ShardRuntime, stream: &BucketStreamId, payload: &[u8]) {
    runtime
        .create_stream(CreateStreamRequest::new(
            stream.clone(),
            DEFAULT_CONTENT_TYPE,
        ))
        .await
        .expect("create stream");
    runtime
        .append(AppendRequest::from_bytes(stream.clone(), payload.to_vec()))
        .await
        .expect("append");
}

async fn flush(runtime: &ShardRuntime) -> Result<usize, RuntimeError> {
    runtime
        .flush_cold_group_batch_once(
            RaftGroupId(0),
            PlanGroupColdFlushRequest {
                min_hot_bytes: 1,
                max_flush_bytes: 8 << 20,
                max_batch_bytes: 8 << 20,
                pressure: None,
                max_hot_age: None,
            },
            8,
        )
        .await
        .map(|responses| responses.len())
}

fn refused_by_the_fence(result: &Result<usize, RuntimeError>) -> bool {
    matches!(
        result,
        Err(RuntimeError::GroupEngine {
            error: GroupEngineError::NotLeader(refusal),
            ..
        }) if refusal.leader.is_none()
    )
}

fn seen(seen: &Seen) -> (Vec<String>, Vec<String>) {
    seen.lock().unwrap_or_else(PoisonError::into_inner).clone()
}

async fn read_all(runtime: &ShardRuntime, stream: &BucketStreamId, len: usize) -> Vec<u8> {
    runtime
        .read_stream(ReadStreamRequest {
            stream_id: stream.clone(),
            offset: 0,
            max_len: len,
            now_ms: 0,
            leader_only: false,
            read_index: None,
            follower: crate::request::FollowerRead::Prefix,
        })
        .await
        .expect("read stream")
        .payload
        .to_vec()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_flush_under_its_own_leadership_publishes_and_keeps_its_upload() {
    let (runtime, _leadership, seen_objects, _controls) = spawn(false);
    let stream = BucketStreamId::new("fence", "kept");
    create_and_append(&runtime, &stream, b"abcd").await;

    assert_eq!(flush(&runtime).await.expect("flush"), 1);
    let (written, deleted) = seen(&seen_objects);
    assert_eq!(written.len(), 1, "{written:?}");
    assert!(deleted.is_empty(), "{deleted:?}");
    assert_eq!(read_all(&runtime, &stream, 16).await, b"abcd");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pack_whose_leadership_ended_during_its_put_is_refused_and_deleted() {
    let (runtime, _leadership, seen_objects, _controls) = spawn(true);
    let first = BucketStreamId::new("fence", "pack-a");
    let second = BucketStreamId::new("fence", "pack-b");
    create_and_append(&runtime, &first, b"aaaa").await;
    create_and_append(&runtime, &second, b"bbbb").await;

    let result = flush(&runtime).await;
    assert!(refused_by_the_fence(&result), "{result:?}");
    let (written, deleted) = seen(&seen_objects);
    assert_eq!(written.len(), 1, "one pack: {written:?}");
    assert_eq!(deleted, written, "nothing references the refused pack");
    // Nothing applied: the bytes stay hot.
    assert_eq!(read_all(&runtime, &first, 16).await, b"aaaa");
    assert_eq!(read_all(&runtime, &second, 16).await, b"bbbb");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_exclusive_chunk_whose_leadership_ended_during_its_put_is_refused_and_deleted() {
    let (runtime, _leadership, seen_objects, _controls) = spawn(true);
    let stream = BucketStreamId::new("fence", "exclusive");
    let payload = vec![b'x'; (1 << 20) + 10];
    create_and_append(&runtime, &stream, &payload).await;

    let result = flush(&runtime).await;
    assert!(refused_by_the_fence(&result), "{result:?}");
    let (written, deleted) = seen(&seen_objects);
    assert_eq!(written.len(), 1, "one chunk: {written:?}");
    assert_eq!(deleted, written, "nothing references the refused chunk");
    assert_eq!(read_all(&runtime, &stream, payload.len()).await, payload);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replica_that_no_longer_leads_uploads_nothing() {
    let (runtime, leadership, seen_objects, _controls) = spawn(false);
    let stream = BucketStreamId::new("fence", "follower");
    create_and_append(&runtime, &stream, b"abcd").await;
    set(&leadership, None);

    let result = flush(&runtime).await;
    assert!(refused_by_the_fence(&result), "{result:?}");
    let (written, deleted) = seen(&seen_objects);
    assert!(written.is_empty(), "no PUT without a fence: {written:?}");
    assert!(deleted.is_empty(), "{deleted:?}");
}

/// A pack whose slice answers an unknown outcome, or an entry OpenRaft
/// appended and then discarded, may still commit: the pack is kept for the
/// orphan sweep, never deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pack_whose_outcome_is_unknown_is_kept() {
    let ambiguous = [
        GroupEngineError::Infra(crate::engine::GroupInfraError::OutcomeUnknown),
        GroupEngineError::NotLeader(crate::engine::LeadershipRefusal {
            leader: None,
            proposal: crate::engine::Proposal::MayHaveCommitted,
            reason: crate::engine::LeadershipReason::SteppedDown,
        }),
    ];
    for error in ambiguous {
        let (runtime, _leadership, seen_objects, controls) = spawn(false);
        let first = BucketStreamId::new("fence", "unknown-a");
        let second = BucketStreamId::new("fence", "unknown-b");
        create_and_append(&runtime, &first, b"aaaa").await;
        create_and_append(&runtime, &second, b"bbbb").await;
        *controls
            .forced
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(error.clone());

        let result = flush(&runtime).await;
        assert!(
            matches!(&result, Err(RuntimeError::GroupEngine { error: refused, .. }) if *refused == error),
            "{result:?}"
        );
        let (written, deleted) = seen(&seen_objects);
        assert_eq!(written.len(), 1, "one pack: {written:?}");
        assert!(deleted.is_empty(), "{error:?} keeps the pack: {deleted:?}");
    }
}

/// A pack whose first slice published and whose second its fence refused,
/// because the leadership moved between them, is kept: the published slice
/// references it. The pack is deleted only when no slice was published.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_partly_published_pack_refused_by_its_fence_is_kept() {
    let (runtime, _leadership, seen_objects, controls) = spawn(false);
    let first = BucketStreamId::new("fence", "partly-a");
    let second = BucketStreamId::new("fence", "partly-b");
    create_and_append(&runtime, &first, b"aaaa").await;
    create_and_append(&runtime, &second, b"bbbb").await;
    *controls
        .moves_after
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = Some(1);

    let result = flush(&runtime).await;
    assert!(refused_by_the_fence(&result), "{result:?}");
    let (written, deleted) = seen(&seen_objects);
    assert_eq!(written.len(), 1, "one pack: {written:?}");
    assert!(
        deleted.is_empty(),
        "a published slice keeps the pack: {deleted:?}"
    );
    // Both streams still read all their bytes: one slice from the pack, the
    // other from the hot buffer.
    assert_eq!(read_all(&runtime, &first, 16).await, b"aaaa");
    assert_eq!(read_all(&runtime, &second, 16).await, b"bbbb");
}
