//! The faulty in-memory snapshot store under madsim (#687, P4's store).
//!
//! Three nodes share one bucket with every fault on, at rates the seed
//! picks. Each node builds snapshots one after another and runs an owner's
//! GC pass after each: it lists its directory and deletes every listed key
//! but the one it names. Its requests interleave with the other nodes' by
//! the simulated delays. The run checks I1, I2 and, once the store settled
//! and a fault-free pass ran on every node, I5. A seed replays the same
//! bucket log.

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use ursula_runtime::SnapshotBytesIterator;
use ursula_runtime::SnapshotLocation;
use ursula_runtime::SnapshotStore;
use ursula_runtime::StagedSnapshot;
use ursula_runtime::snapshot_harness::MemorySnapshotBucket;
use ursula_runtime::snapshot_harness::MemorySnapshotStore;
use ursula_runtime::snapshot_harness::NodeNames;
use ursula_runtime::snapshot_harness::PutOutcome;
use ursula_runtime::snapshot_harness::SnapshotStoreEvent;
use ursula_runtime::snapshot_harness::SnapshotStoreFaults;
use ursula_runtime::snapshot_harness::check_named_keys_exist;
use ursula_runtime::snapshot_harness::check_no_foreign_delete;
use ursula_runtime::snapshot_harness::check_quiet_prefixes;
use ursula_shard::RaftGroupId;

use super::seeds_from_env;
use super::sim_test_guard;
use crate::madsim_harness::run_with_madsim;

const SNAPSHOT_STORE_HARNESS_SEEDS: [u64; 4] = [1, 2, 3, 4];
const NODES: [u64; 3] = [1, 2, 3];
const GROUP: RaftGroupId = RaftGroupId(0);
const BUILDS: u64 = 24;

/// One build: a fresh key at `index`, written, then verified.
async fn build(store: &MemorySnapshotStore, index: u64) -> Option<String> {
    let chunks: SnapshotBytesIterator = Box::new(std::iter::once(Ok(Bytes::from(format!(
        "node-{}-index-{index}",
        store.node_id()
    )))));
    let staged = store.stage(GROUP, index, chunks).await.expect("stage");
    let StagedSnapshot::Object(object) = &staged else {
        panic!("the memory store stages objects");
    };
    let path = object.path();
    let location = store.write_staged(staged).await.ok()?;
    assert!(matches!(&location, SnapshotLocation::S3 { key, .. } if *key == path));
    store.verify_uploaded(&location).await.ok()?;
    Some(path)
}

/// An owner's pass: deletes every listed key but `named`.
async fn pass(store: &MemorySnapshotStore, named: Option<&String>) {
    let listing = store.list_own(GROUP).await.expect("list");
    for object in listing.owned {
        if named.map(String::as_str) != Some(object.path()) {
            store.delete_own(object).await.expect("delete");
        }
    }
}

/// Every node's builds and passes, then a quiet pass: the bucket log and
/// what each node names at the end.
async fn run_nodes(seed: u64) -> (Vec<SnapshotStoreEvent>, BTreeMap<u64, Option<String>>) {
    let bucket = MemorySnapshotBucket::new(
        seed,
        "snapshots".try_into().expect("prefix"),
        SnapshotStoreFaults::from_seed(seed, Duration::from_millis(20)),
    );
    let tasks: Vec<_> = NODES
        .iter()
        .map(|&node_id| {
            let bucket = bucket.clone();
            madsim::task::spawn(async move {
                let store = bucket.store(node_id);
                let mut named = None;
                for index in 1..=BUILDS {
                    if let Some(path) = build(&store, index).await {
                        named = Some(path);
                    }
                    pass(&store, named.as_ref()).await;
                    let names = NodeNames::new(node_id, named.clone());
                    let view = bucket.view();
                    check_named_keys_exist(&view, [&names]).expect("I1");
                    check_no_foreign_delete(&view).expect("I2");
                }
                (node_id, named)
            })
        })
        .collect();
    let mut named = BTreeMap::new();
    for task in tasks {
        let (node_id, path) = task.await.expect("node task");
        named.insert(node_id, path);
    }
    // Quiet: every late PUT landed, and a strict pass ran on every node.
    bucket.settle();
    bucket.set_faults(SnapshotStoreFaults::none());
    for (node_id, path) in &named {
        pass(&bucket.store(*node_id), path.as_ref()).await;
    }
    let names: Vec<NodeNames> = named
        .iter()
        .map(|(node_id, path)| NodeNames::new(*node_id, path.clone()))
        .collect();
    let view = bucket.view();
    check_named_keys_exist(&view, &names).expect("I1 once quiet");
    check_no_foreign_delete(&view).expect("I2");
    check_quiet_prefixes(&view, &names).expect("I5");
    (bucket.events(), named)
}

#[test]
fn snapshot_store_harness_faults_replay_per_seed() {
    let _guard = sim_test_guard();
    let mut late = 0usize;
    for seed in seeds_from_env(
        "SNAPSHOT_STORE_HARNESS_SEEDS",
        &SNAPSHOT_STORE_HARNESS_SEEDS,
    ) {
        let first = run_with_madsim(seed, run_nodes(seed));
        let second = run_with_madsim(seed, run_nodes(seed));
        assert_eq!(first, second, "seed {seed} replays");
        late = late.saturating_add(
            first
                .0
                .iter()
                .filter(|event| {
                    matches!(event, SnapshotStoreEvent::Put {
                        outcome: PutOutcome::Late { .. },
                        ..
                    })
                })
                .count(),
        );
        assert!(
            first.1.values().all(Option::is_some),
            "seed {seed}: every node built at least once"
        );
    }
    assert!(late > 0, "some PUT failed to its client and landed later");
}
