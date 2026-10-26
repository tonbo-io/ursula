//! The per-stream usage export's node-wide read
//! ([`ShardRuntime::stream_usage_groups`]) on the in-memory engine: groups
//! in ascending order, the groups this node hosts only, and the answer's
//! item budget, which caps each group's limit at what the groups before it
//! left and omits a hosted group it leaves nothing for.

use std::num::NonZeroUsize;

use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardPlacement;
use ursula_stream::GroupUsageView;
use ursula_stream::StreamUsageRead;
use ursula_stream::UsageCursor;
use ursula_stream::UsageListing;
use ursula_stream::UsagePage;
use ursula_stream::UsagePosition;

use crate::CreateStreamRequest;
use crate::GroupEngineCreateFuture;
use crate::GroupEngineFactory;
use crate::GroupEngineMetrics;
use crate::InMemoryGroupEngineFactory;
use crate::RuntimeConfig;
use crate::ShardRuntime;
use crate::StreamUsageGroups;
use crate::cold_store::DEFAULT_CONTENT_TYPE;

/// In-memory engines for the even groups only: this node hosts no replica
/// of an odd group.
struct EvenGroups(InMemoryGroupEngineFactory);

impl GroupEngineFactory for EvenGroups {
    fn hosts_group(&self, placement: ShardPlacement) -> bool {
        placement.raft_group_id.0.is_multiple_of(2)
    }

    fn create<'a>(
        &'a self,
        placement: ShardPlacement,
        metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        self.0.create(placement, metrics)
    }
}

/// A runtime of four groups that hosts groups 0 and 2, with three streams
/// in group 0 and two in group 2, all in one bucket: four items in group
/// 0's relisting and three in group 2's.
async fn runtime_with_streams() -> ShardRuntime {
    let runtime = ShardRuntime::spawn_with_engine_factory(
        RuntimeConfig::new(1, 4),
        EvenGroups(InMemoryGroupEngineFactory::new()),
    )
    .expect("spawn runtime");
    let mut wanted = [(RaftGroupId(0), 3_usize), (RaftGroupId(2), 2_usize)];
    for index in 0_u32.. {
        if wanted.iter().all(|(_, left)| *left == 0) {
            break;
        }
        let stream = BucketStreamId::new("usage", format!("s{index}"));
        let group = runtime.locate(&stream).raft_group_id;
        let Some((_, left)) = wanted
            .iter_mut()
            .find(|(wanted_group, left)| *wanted_group == group && *left > 0)
        else {
            continue;
        };
        runtime
            .create_stream(CreateStreamRequest::new(stream, DEFAULT_CONTENT_TYPE))
            .await
            .expect("create");
        *left = left.saturating_sub(1);
    }
    runtime
}

fn start(limit: usize) -> StreamUsageRead {
    StreamUsageRead {
        cursor: UsageCursor::Start,
        limit: NonZeroUsize::new(limit).expect("a positive limit"),
    }
}

/// Every group of the runtime, from the highest, each read from `start`
/// with a limit of 10.
async fn read_all(runtime: &ShardRuntime, max_items: usize) -> StreamUsageGroups {
    let reads = (0..4).rev().map(|group| (RaftGroupId(group), start(10)));
    runtime
        .stream_usage_groups(reads.collect(), max_items)
        .await
        .expect("the runtime serves")
}

fn listing(answer: &StreamUsageGroups, at: usize) -> (RaftGroupId, &UsageListing) {
    match &answer.groups[at] {
        (group, Ok(GroupUsageView::Listed(listing))) => (*group, listing),
        other => panic!("not listed: {other:?}"),
    }
}

fn items(listing: &UsageListing) -> usize {
    listing.rows.len().saturating_add(listing.entries.len())
}

/// The hosted groups answer in ascending order, and the others are left out.
#[tokio::test]
async fn hosted_groups_answer_in_ascending_order() {
    let runtime = runtime_with_streams().await;
    let answer = read_all(&runtime, 100).await;
    assert_eq!(answer.groups.len(), 2);
    assert!(answer.omitted.is_empty());
    let (group, first) = listing(&answer, 0);
    assert_eq!(group, RaftGroupId(0));
    assert_eq!(items(first), 4);
    assert_eq!(first.page, UsagePage::Complete);
    let (group, second) = listing(&answer, 1);
    assert_eq!(group, RaftGroupId(2));
    assert_eq!(items(second), 3);
    assert_eq!(second.page, UsagePage::Complete);
}

/// Each group's limit is capped at what the answer has left: the first
/// group reads its whole limit, the next one what remains, and a hosted
/// group left with nothing is omitted, unread.
#[tokio::test]
async fn the_answer_budget_caps_each_group_and_omits_the_rest() {
    let runtime = runtime_with_streams().await;

    let answer = read_all(&runtime, 5).await;
    assert!(answer.omitted.is_empty());
    let (_, first) = listing(&answer, 0);
    assert_eq!(items(first), 4);
    let (group, second) = listing(&answer, 1);
    assert_eq!(group, RaftGroupId(2));
    assert_eq!(items(second), 1);
    assert_eq!(second.page, UsagePage::More {
        next: UsagePosition::Entries { after_seq: 0 },
    });

    let answer = read_all(&runtime, 4).await;
    assert_eq!(answer.groups.len(), 1);
    assert_eq!(items(listing(&answer, 0).1), 4);
    assert_eq!(answer.omitted, [RaftGroupId(2)]);

    let answer = read_all(&runtime, 2).await;
    assert_eq!(answer.groups.len(), 1);
    let (group, first) = listing(&answer, 0);
    assert_eq!(group, RaftGroupId(0));
    assert_eq!(items(first), 2);
    assert!(matches!(first.page, UsagePage::More { .. }));
    assert_eq!(answer.omitted, [RaftGroupId(2)]);
}
