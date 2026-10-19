//! The faulty in-memory snapshot store under madsim (#687).
//!
//! These cases exercise the store, the gates and the checkers, with a toy
//! owner's pass of this file. The server's ownership protocol, its builds,
//! sends, install copies, restores and GC passes, is tested on the same
//! store and gates in `ursula-raft` (`snapshot_ownership_tests`):
//!
//! - `run_nodes`: three nodes share one bucket with PUT faults at rates the
//!   seed picks and stale LISTs on. Each node builds and then runs an
//!   owner's pass, one after the other, so a node never races itself, and
//!   every node deletes only through its own store, so I2 cannot fail. I1
//!   and I2 are checked at every step and I5 once quiet, right after a
//!   fault-free pass.
//! - The gated case: a build against a pass on one node, in one order the
//!   gates force. Its seeds change only the delays, so they explore no
//!   other order.
//! - The concurrent case: a build and a pass on one node with no gates,
//!   so the seed picks the order. With the lease after the PUT, I1 breaks
//!   on some seeds, one of them pinned.
//!
//! The negative controls switch the toy pass of this file, not production
//! code.
//!
//! Every seed runs twice and must log the same requests and gate events.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use bytes::Bytes;
use ursula_runtime::SnapshotBytesIterator;
use ursula_runtime::SnapshotLocation;
use ursula_runtime::SnapshotStore;
use ursula_runtime::StagedSnapshot;
use ursula_runtime::snapshot_harness::DelayRange;
use ursula_runtime::snapshot_harness::Gate;
use ursula_runtime::snapshot_harness::GateEvent;
use ursula_runtime::snapshot_harness::GatedSnapshotStore;
use ursula_runtime::snapshot_harness::MemorySnapshotBucket;
use ursula_runtime::snapshot_harness::MemorySnapshotStore;
use ursula_runtime::snapshot_harness::NodeNames;
use ursula_runtime::snapshot_harness::PerMille;
use ursula_runtime::snapshot_harness::PutOutcome;
use ursula_runtime::snapshot_harness::SnapshotInvariantViolation;
use ursula_runtime::snapshot_harness::SnapshotStoreEvent;
use ursula_runtime::snapshot_harness::SnapshotStoreFaults;
use ursula_runtime::snapshot_harness::SnapshotStoreGates;
use ursula_runtime::snapshot_harness::StoreOp;
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
const MAX_DELAY: Duration = Duration::from_millis(20);

fn seeds() -> Vec<u64> {
    seeds_from_env(
        "SNAPSHOT_STORE_HARNESS_SEEDS",
        &SNAPSHOT_STORE_HARNESS_SEEDS,
    )
}

fn bucket(seed: u64, faults: SnapshotStoreFaults) -> MemorySnapshotBucket {
    MemorySnapshotBucket::new(seed, "snapshots".try_into().expect("prefix"), faults)
}

fn chunks(text: String) -> SnapshotBytesIterator {
    Box::new(std::iter::once(Ok(Bytes::from(text))))
}

fn staged_path(staged: &StagedSnapshot) -> String {
    let StagedSnapshot::Object(object) = staged else {
        panic!("the memory store stages objects");
    };
    object.path()
}

/// One build: a fresh key at `index`, written, then verified.
async fn build(store: &MemorySnapshotStore, index: u64) -> Option<String> {
    let body = chunks(format!("node-{}-index-{index}", store.node_id()));
    let staged = store.stage(GROUP, index, body).await.expect("stage");
    let path = staged_path(&staged);
    let location = store.write_staged(staged).await.ok()?.into_location();
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
    let bucket = bucket(
        seed,
        SnapshotStoreFaults::from_seed(seed, MAX_DELAY).with_stale_lists(
            PerMille::new(300),
            8,
            MAX_DELAY.saturating_mul(4),
        ),
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
    // Quiet: simulated time passed every late PUT's landing, and a strict
    // pass ran on every node.
    bucket.wait_for_late_puts().await;
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
    for seed in seeds() {
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

/// The node of the gated case.
const NODE: u64 = 1;

/// What one gated run saw.
#[derive(Debug, PartialEq, Eq)]
struct GatedRun {
    gates: Vec<GateEvent>,
    bucket: Vec<SnapshotStoreEvent>,
    i1: Result<(), SnapshotInvariantViolation>,
}

/// A build against a pass on one node, in the order the gates force:
/// the build's PUT, the pass's LIST and read of the holds, the build's
/// commit, the pass's DELETEs. The build leases its key before the PUT, or,
/// for the negative control, after it.
async fn run_build_against_pass(seed: u64, lease_before_put: bool) -> GatedRun {
    let bucket = bucket(seed, SnapshotStoreFaults {
        request_start_delay: DelayRange::between(Duration::ZERO, MAX_DELAY),
        request_end_delay: DelayRange::between(Duration::ZERO, MAX_DELAY),
        ..SnapshotStoreFaults::none()
    });
    let gates = SnapshotStoreGates::new();
    let store = GatedSnapshotStore::new(NODE, Arc::new(bucket.store(NODE)), gates.clone());
    let holds: Arc<Mutex<BTreeSet<String>>> = Arc::default();
    let stage = |index: u64| {
        let store = store.clone();
        async move {
            let body = chunks(format!("index-{index}"));
            store.stage(GROUP, index, body).await.expect("stage")
        }
    };

    // An orphan at index 0, the first key the pass deletes, and the
    // current snapshot at index 1, held.
    let orphan = stage(0).await;
    store.write_staged(orphan).await.expect("orphan");
    let first = stage(1).await;
    let first_path = staged_path(&first);
    holds.lock().expect("holds").insert(first_path.clone());
    store.write_staged(first).await.expect("first");

    let list = Gate::before(NODE, StoreOp::ListOwn);
    let delete = Gate::before(NODE, StoreOp::DeleteOwn);
    gates.hold(list);
    gates.hold(delete);
    let gc = {
        let (store, holds) = (store.clone(), Arc::clone(&holds));
        madsim::task::spawn(async move {
            let listing = store.list_own(GROUP).await.expect("list");
            let held = holds.lock().expect("holds").clone();
            for object in listing.owned {
                if !held.contains(object.path()) {
                    store.delete_own(object).await.expect("delete");
                }
            }
        })
    };
    gates.wait_arrived(list, 1).await;

    // The build's PUT, before the pass lists.
    let second = stage(2).await;
    let second_path = staged_path(&second);
    if lease_before_put {
        holds.lock().expect("holds").insert(second_path.clone());
    }
    store.write_staged(second).await.expect("second");
    gates.open(list);
    // The pass listed and read the holds, and waits at its first DELETE.
    gates.wait_arrived(delete, 1).await;

    // The commit: the new key becomes current and the old one is released.
    {
        let mut held = holds.lock().expect("holds");
        held.insert(second_path.clone());
        held.remove(&first_path);
    }
    gates.open(delete);
    gc.await.expect("pass");

    let names = NodeNames::new(NODE, [second_path]);
    GatedRun {
        gates: gates.log(),
        bucket: bucket.events(),
        i1: check_named_keys_exist(&bucket.view(), [&names]),
    }
}

#[test]
fn snapshot_store_harness_gated_build_against_pass_replays() {
    let _guard = sim_test_guard();
    for seed in seeds() {
        let first = run_with_madsim(seed, run_build_against_pass(seed, true));
        let second = run_with_madsim(seed, run_build_against_pass(seed, true));
        assert_eq!(first, second, "seed {seed} replays its gates and requests");
        assert_eq!(
            first.i1,
            Ok(()),
            "seed {seed}: a key leased before its PUT stays"
        );
    }
}

#[test]
fn snapshot_store_harness_negative_control_lease_after_put_breaks_i1() {
    let _guard = sim_test_guard();
    for seed in seeds() {
        let first = run_with_madsim(seed, run_build_against_pass(seed, false));
        let second = run_with_madsim(seed, run_build_against_pass(seed, false));
        assert_eq!(first, second, "seed {seed} replays its gates and requests");
        assert!(
            matches!(
                first.i1,
                Err(SnapshotInvariantViolation::NamedKeyMissing { node_id: NODE, .. })
            ),
            "seed {seed}: the pass deleted the key leased after its PUT: {:?}",
            first.i1
        );
    }
}

/// The seeds of the concurrent case: wide, since only some seeds reach the
/// order that breaks the control.
const CONCURRENT_SEEDS: std::ops::RangeInclusive<u64> = 1..=32;
/// Builds against passes per seed in the concurrent case.
const ROUNDS: u64 = 8;
/// A seed on which a lease after the PUT loses the new key, and the round
/// in which it does. Recorded from the CI run of #727, where 23 of the 32
/// seeds broke the control.
const LEASE_AFTER_PUT_BREAKS: (u64, u64) = (5, 1);

/// What one concurrent run saw.
#[derive(Debug, PartialEq, Eq)]
struct ConcurrentRun {
    bucket: Vec<SnapshotStoreEvent>,
    /// The first round after which I1 failed, and how.
    broken: Option<(u64, SnapshotInvariantViolation)>,
}

/// Rounds of a build and a pass on one node, run as concurrent tasks with
/// no gates, so the seed's delays pick the order of the build's PUT, lease
/// and commit against the pass's LIST, read of the holds and DELETEs.
async fn run_concurrent_build_and_pass(seed: u64, lease_before_put: bool) -> ConcurrentRun {
    let bucket = bucket(seed, SnapshotStoreFaults {
        request_start_delay: DelayRange::between(Duration::ZERO, MAX_DELAY),
        request_end_delay: DelayRange::between(Duration::ZERO, MAX_DELAY),
        ..SnapshotStoreFaults::none()
    });
    let store = Arc::new(bucket.store(NODE));
    let holds: Arc<Mutex<BTreeSet<String>>> = Arc::default();
    let mut current: Option<String> = None;
    let mut broken = None;
    for round in 1..=ROUNDS {
        let builder = {
            let (store, holds, previous) =
                (Arc::clone(&store), Arc::clone(&holds), current.clone());
            madsim::task::spawn(async move {
                let body = chunks(format!("round-{round}"));
                let staged = store.stage(GROUP, round, body).await.expect("stage");
                let path = staged_path(&staged);
                if lease_before_put {
                    holds.lock().expect("holds").insert(path.clone());
                }
                store.write_staged(staged).await.expect("put");
                let mut held = holds.lock().expect("holds");
                held.insert(path.clone());
                // The commit: the new key is current, the old one released.
                if let Some(previous) = previous {
                    held.remove(&previous);
                }
                path
            })
        };
        let gc = {
            let (store, holds) = (Arc::clone(&store), Arc::clone(&holds));
            madsim::task::spawn(async move {
                let listing = store.list_own(GROUP).await.expect("list");
                let held = holds.lock().expect("holds").clone();
                for object in listing.owned {
                    if !held.contains(object.path()) {
                        store.delete_own(object).await.expect("delete");
                    }
                }
            })
        };
        let path = builder.await.expect("build");
        gc.await.expect("pass");
        current = Some(path);
        let names = NodeNames::new(NODE, current.clone());
        if broken.is_none()
            && let Err(violation) = check_named_keys_exist(&bucket.view(), [&names])
        {
            broken = Some((round, violation));
        }
    }
    ConcurrentRun {
        bucket: bucket.events(),
        broken,
    }
}

#[test]
fn snapshot_store_harness_concurrent_build_and_pass_replays() {
    let _guard = sim_test_guard();
    for seed in CONCURRENT_SEEDS {
        let first = run_with_madsim(seed, run_concurrent_build_and_pass(seed, true));
        let second = run_with_madsim(seed, run_concurrent_build_and_pass(seed, true));
        assert_eq!(first, second, "seed {seed} replays");
        assert_eq!(
            first.broken, None,
            "seed {seed}: a key leased before its PUT stays"
        );
    }
}

#[test]
fn snapshot_store_harness_negative_control_concurrent_lease_after_put_breaks_i1() {
    let _guard = sim_test_guard();
    let mut breaking = Vec::new();
    for seed in CONCURRENT_SEEDS {
        let first = run_with_madsim(seed, run_concurrent_build_and_pass(seed, false));
        let second = run_with_madsim(seed, run_concurrent_build_and_pass(seed, false));
        assert_eq!(first, second, "seed {seed} replays");
        if let Some((round, violation)) = first.broken {
            assert!(matches!(
                violation,
                SnapshotInvariantViolation::NamedKeyMissing { node_id: NODE, .. }
            ));
            breaking.push((seed, round));
        }
    }
    println!("a lease after the PUT breaks I1 on (seed, round): {breaking:?}");
    assert!(
        breaking.contains(&LEASE_AFTER_PUT_BREAKS),
        "a lease after the PUT breaks I1 on the pinned (seed, round) {LEASE_AFTER_PUT_BREAKS:?}: {breaking:?}"
    );
}
