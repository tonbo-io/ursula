//! Tests of the snapshot store harness itself, on tokio's paused clock.
#![cfg(not(madsim))]

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use ursula_shard::RaftGroupId;

use super::CommittedRead;
use super::CommittedReadPurpose;
use super::DelayRange;
use super::ForeignDeletes;
use super::Gate;
use super::GatePass;
use super::GatedSnapshotStore;
use super::MemorySnapshotBucket;
use super::NodeNames;
use super::PerMille;
use super::PutOutcome;
use super::ReadOutcome;
use super::SnapshotInvariantViolation;
use super::SnapshotStoreEvent;
use super::SnapshotStoreFaults;
use super::SnapshotStoreGates;
use super::StoreOp;
use super::check_committed_reads_found;
use super::check_named_keys_exist;
use super::check_no_foreign_delete;
use super::check_quiet_prefixes;
use super::check_superseded_without_marker;
use super::contains_marker;
use crate::snapshot_store::SnapshotBytesIterator;
use crate::snapshot_store::SnapshotLocation;
use crate::snapshot_store::SnapshotPrefix;
use crate::snapshot_store::SnapshotStore;
use crate::snapshot_store::SnapshotStoreError;

const GROUP: RaftGroupId = RaftGroupId(0);

fn prefix() -> SnapshotPrefix {
    SnapshotPrefix::try_from("snapshots").unwrap()
}

fn body(bytes: &[u8]) -> SnapshotBytesIterator {
    Box::new(std::iter::once(Ok(Bytes::copy_from_slice(bytes))))
}

fn key_of(location: &SnapshotLocation) -> String {
    match location {
        SnapshotLocation::S3 { key, .. } => key.clone(),
        other => panic!("not an object: {other:?}"),
    }
}

/// Stages and writes a snapshot at `index`, answering its key and the
/// write's result.
async fn build(
    store: &dyn SnapshotStore,
    index: u64,
    bytes: &[u8],
) -> (String, Result<SnapshotLocation, SnapshotStoreError>) {
    let staged = store.stage(GROUP, index, body(bytes)).await.unwrap();
    let path = match &staged {
        crate::snapshot_store::StagedSnapshot::Object(object) => object.path(),
        crate::snapshot_store::StagedSnapshot::Inline { .. } => panic!("staged inline"),
    };
    (path, store.write_staged(staged).await)
}

/// Faults with no delays, so a test sets only the one it exercises.
fn only(change: impl FnOnce(&mut SnapshotStoreFaults)) -> SnapshotStoreFaults {
    let mut faults = SnapshotStoreFaults::none();
    change(&mut faults);
    faults
}

/// One node's builds and GC passes against a bucket with every fault on:
/// what the bucket logged.
async fn faulty_run(seed: u64) -> Vec<SnapshotStoreEvent> {
    let faults = SnapshotStoreFaults {
        late_put: PerMille::new(400),
        stale_list: PerMille::new(600),
        stale_window: 16,
        ..SnapshotStoreFaults::every_fault(Duration::from_millis(5))
    };
    let bucket = MemorySnapshotBucket::new(seed, prefix(), faults);
    let store = bucket.store(1);
    for index in 1..=64u64 {
        let (_path, _written) = build(&store, index, format!("body-{index}").as_bytes()).await;
        if index % 4 == 0 {
            let listing = store.list_own(GROUP).await.unwrap();
            // Keep the newest object, as a pass that holds the current one.
            let keep = listing.owned.len().saturating_sub(1);
            for object in listing.owned.into_iter().take(keep) {
                store.delete_own(object).await.unwrap();
            }
        }
    }
    bucket.events()
}

#[tokio::test(start_paused = true)]
async fn late_puts_and_stale_lists_replay_per_seed() {
    let mut late = 0;
    let mut landed = 0;
    let mut omitted = 0;
    let mut repeated = 0;
    for seed in 1..=4u64 {
        let first = faulty_run(seed).await;
        assert_eq!(first, faulty_run(seed).await, "seed {seed} replays");
        for event in &first {
            match event {
                SnapshotStoreEvent::Put {
                    outcome: PutOutcome::Late { .. },
                    ..
                } => late += 1,
                SnapshotStoreEvent::LatePutLanded { .. } => landed += 1,
                SnapshotStoreEvent::List {
                    omitted: left_out,
                    repeated: again,
                    ..
                } => {
                    omitted += left_out.len();
                    repeated += again.len();
                }
                _ => {}
            }
        }
    }
    assert_ne!(faulty_run(1).await, faulty_run(2).await, "seeds differ");
    assert!(
        late > 0 && landed > 0,
        "late PUTs: {late}, landed: {landed}"
    );
    assert!(
        omitted > 0 && repeated > 0,
        "omitted: {omitted}, repeated: {repeated}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_late_put_fails_to_the_client_and_lands_after_its_delay() {
    let bucket = MemorySnapshotBucket::new(
        7,
        prefix(),
        only(|faults| {
            faults.late_put = PerMille::ALWAYS;
            faults.late_put_delay = DelayRange::fixed(Duration::from_millis(10));
        }),
    );
    let store = bucket.store(1);
    let (path, written) = build(&store, 5, b"late").await;
    assert!(matches!(
        written,
        Err(SnapshotStoreError::Backend { ref key, .. }) if *key == path
    ));
    assert!(bucket.keys().is_empty(), "not landed yet");
    assert_eq!(bucket.pending_late_puts(), 1);
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(bucket.keys(), BTreeSet::from([path.clone()]));
    assert!(
        bucket
            .events()
            .contains(&SnapshotStoreEvent::LatePutLanded { path })
    );
}

#[tokio::test(start_paused = true)]
async fn settling_lands_every_late_put() {
    let bucket = MemorySnapshotBucket::new(
        7,
        prefix(),
        only(|faults| {
            faults.late_put = PerMille::ALWAYS;
            faults.late_put_delay = DelayRange::fixed(Duration::from_secs(3600));
        }),
    );
    let store = bucket.store(1);
    let (first, _failed) = build(&store, 1, b"a").await;
    let (second, _failed) = build(&store, 2, b"b").await;
    assert_eq!(bucket.pending_late_puts(), 2);
    bucket.settle();
    assert_eq!(bucket.pending_late_puts(), 0);
    assert_eq!(bucket.keys(), BTreeSet::from([first, second]));
}

#[tokio::test(start_paused = true)]
async fn a_lost_put_never_lands() {
    let bucket = MemorySnapshotBucket::new(
        7,
        prefix(),
        only(|faults| faults.lost_put = PerMille::ALWAYS),
    );
    let store = bucket.store(1);
    let (_path, written) = build(&store, 1, b"lost").await;
    assert!(matches!(written, Err(SnapshotStoreError::Backend { .. })));
    bucket.settle();
    assert!(bucket.keys().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_stale_list_omits_new_keys_and_repeats_deleted_ones() {
    let bucket = MemorySnapshotBucket::new(11, prefix(), SnapshotStoreFaults::none());
    let store = bucket.store(1);
    let mut written = Vec::new();
    for index in 1..=6 {
        let (path, result) = build(&store, index, b"x").await;
        result.unwrap();
        written.push(path);
    }
    let listing = store.list_own(GROUP).await.unwrap();
    for object in listing.owned.into_iter().take(3) {
        store.delete_own(object).await.unwrap();
    }
    let present = bucket.keys();
    assert_eq!(present.len(), 3);
    bucket.set_faults(only(|faults| {
        faults.stale_list = PerMille::ALWAYS;
        faults.stale_window = 64;
    }));
    let (mut omitted, mut repeated) = (BTreeSet::new(), BTreeSet::new());
    for _ in 0..16 {
        let listing = store.list_own(GROUP).await.unwrap();
        let listed: BTreeSet<String> = listing
            .owned
            .iter()
            .map(|object| object.path().to_owned())
            .collect();
        omitted.extend(present.difference(&listed).cloned());
        repeated.extend(listed.difference(&present).cloned());
        // A repeated key is gone, and deleting it again is not an error.
        for object in listing.owned {
            if !present.contains(object.path()) {
                store.delete_own(object).await.unwrap();
            }
        }
    }
    assert!(!omitted.is_empty(), "some LIST left out a recent key");
    assert!(!repeated.is_empty(), "some LIST repeated a deleted key");
    assert!(repeated.iter().all(|path| written.contains(path)));
    assert_eq!(bucket.keys(), present, "a stale LIST changes nothing");
}

#[tokio::test(start_paused = true)]
async fn a_create_refuses_a_name_that_exists() {
    let bucket = MemorySnapshotBucket::new(3, prefix(), SnapshotStoreFaults::none());
    let store = bucket.store(1).with_nonces(|| 7);
    let (path, first) = build(&store, 9, b"first").await;
    let location = first.unwrap();
    let (again, second) = build(&store, 9, b"second").await;
    assert_eq!(again, path);
    assert!(matches!(
        second,
        Err(SnapshotStoreError::NameCollision { ref key }) if *key == path
    ));
    // The object is still the first write's.
    assert_eq!(store.download(&location).await.unwrap(), b"first");
}

#[tokio::test(start_paused = true)]
async fn a_late_put_is_refused_when_its_name_was_taken() {
    let bucket = MemorySnapshotBucket::new(
        3,
        prefix(),
        only(|faults| {
            faults.late_put = PerMille::ALWAYS;
            faults.late_put_delay = DelayRange::fixed(Duration::from_millis(10));
        }),
    );
    let store = bucket.store(1).with_nonces(|| 7);
    let (path, _failed) = build(&store, 9, b"late").await;
    bucket.set_faults(SnapshotStoreFaults::none());
    let (again, written) = build(&store, 9, b"direct").await;
    assert_eq!(again, path);
    let location = written.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(bucket.pending_late_puts(), 0);
    assert!(
        bucket
            .events()
            .contains(&SnapshotStoreEvent::LatePutRefused { path })
    );
    // The object is the direct write's.
    let bytes = store.download(&location).await.unwrap();
    assert_eq!(bytes, b"direct");
}

#[tokio::test(start_paused = true)]
async fn nonces_follow_the_seed_and_a_restarted_node_draws_new_ones() {
    let names = |seed: u64| async move {
        let bucket = MemorySnapshotBucket::new(seed, prefix(), SnapshotStoreFaults::none());
        let (first, _written) = build(&bucket.store(1), 1, b"a").await;
        // The node restarts with a new store on the same bucket.
        let (second, _written) = build(&bucket.store(1), 1, b"a").await;
        (first, second)
    };
    let (first, second) = names(5).await;
    assert_ne!(first, second);
    assert_eq!(names(5).await, (first.clone(), second));
    assert_ne!(names(6).await.0, first);
}

#[tokio::test(start_paused = true)]
#[should_panic(expected = "I2: node 1 deleted")]
async fn a_delete_outside_the_own_directory_panics() {
    let bucket = MemorySnapshotBucket::new(1, prefix(), SnapshotStoreFaults::none());
    let (one, ten) = (bucket.store(1), bucket.store(10));
    build(&ten, 1, b"ten").await.1.unwrap();
    let listing = ten.list_own(GROUP).await.unwrap();
    let object = listing.owned.into_iter().next().unwrap();
    // `node-1` must not match `node-10`.
    let _refused = one.delete_own(object).await;
}

#[tokio::test(start_paused = true)]
#[should_panic(expected = "I2: node 1 deleted")]
async fn an_unscoped_delete_outside_the_own_directory_panics() {
    let bucket = MemorySnapshotBucket::new(1, prefix(), SnapshotStoreFaults::none());
    let (one, two) = (bucket.store(1), bucket.store(2));
    let location = build(&two, 1, b"two").await.1.unwrap();
    one.delete(&location).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_recorded_foreign_delete_fails_the_i2_check() {
    let bucket = MemorySnapshotBucket::new(
        1,
        prefix(),
        only(|faults| faults.foreign_deletes = ForeignDeletes::Record),
    );
    let (one, two) = (bucket.store(1), bucket.store(2));
    let location = build(&two, 1, b"two").await.1.unwrap();
    let object = two.list_own(GROUP).await.unwrap().owned.remove(0);
    assert!(matches!(
        one.delete_own(object).await,
        Err(SnapshotStoreError::NotOwnedHere { .. })
    ));
    assert_eq!(bucket.keys().len(), 1, "delete_own refuses as S3's does");
    one.delete(&location).await.unwrap();
    assert!(bucket.keys().is_empty(), "an unscoped delete goes ahead");
    let path = key_of(&location);
    assert_eq!(
        check_no_foreign_delete(&bucket.view()),
        Err(SnapshotInvariantViolation::ForeignDelete { node_id: 1, path })
    );
}

#[tokio::test(start_paused = true)]
async fn own_deletes_pass_the_i2_check() {
    let bucket = MemorySnapshotBucket::new(1, prefix(), SnapshotStoreFaults::none());
    let store = bucket.store(1);
    build(&store, 1, b"one").await.1.unwrap();
    for object in store.list_own(GROUP).await.unwrap().owned {
        store.delete_own(object).await.unwrap();
    }
    assert!(bucket.keys().is_empty());
    assert_eq!(check_no_foreign_delete(&bucket.view()), Ok(()));
}

#[tokio::test(start_paused = true)]
async fn the_checkers_name_what_violates_i1_i3_i4_and_i5() {
    let bucket = MemorySnapshotBucket::new(1, prefix(), SnapshotStoreFaults::none());
    let store = bucket.store(1);
    let current = key_of(&build(&store, 2, b"marker-new").await.1.unwrap());
    let superseded = key_of(&build(&store, 1, b"marker-old").await.1.unwrap());
    let missing = "snapshots/node-1/group-0/00000000000000000003-0000000000000001.snap".to_owned();
    let view = bucket.view();

    let named = NodeNames::new(1, [current.clone()]);
    assert_eq!(check_named_keys_exist(&view, [&named]), Ok(()));
    let dangling = NodeNames::new(1, [current.clone(), missing.clone()]);
    assert_eq!(
        check_named_keys_exist(&view, [&dangling]),
        Err(SnapshotInvariantViolation::NamedKeyMissing {
            node_id: 1,
            path: missing.clone(),
        })
    );

    let reads = [
        CommittedRead {
            node_id: 1,
            path: current.clone(),
            purpose: CommittedReadPurpose::Restore,
            outcome: ReadOutcome::of(&Ok::<(), SnapshotStoreError>(())),
        },
        CommittedRead {
            node_id: 2,
            path: missing.clone(),
            purpose: CommittedReadPurpose::CommittedInstall,
            outcome: ReadOutcome::of(&Err::<(), _>(SnapshotStoreError::NotFound {
                key: missing.clone(),
            })),
        },
    ];
    assert_eq!(check_committed_reads_found(&reads[..1]), Ok(()));
    assert_eq!(
        check_committed_reads_found(&reads),
        Err(SnapshotInvariantViolation::CommittedReadNotFound {
            node_id: 2,
            path: missing,
            purpose: CommittedReadPurpose::CommittedInstall,
        })
    );

    let marker = |bytes: &[u8]| contains_marker(bytes, b"marker-old");
    let bound = Duration::from_secs(10);
    let within = BTreeMap::from([(superseded.clone(), Duration::from_secs(5))]);
    assert_eq!(
        check_superseded_without_marker(&view, &within, bound, marker),
        Ok(())
    );
    let past = BTreeMap::from([(superseded.clone(), Duration::from_secs(11))]);
    assert_eq!(
        check_superseded_without_marker(&view, &past, bound, marker),
        Err(SnapshotInvariantViolation::SupersededHoldsMarker {
            path: superseded.clone(),
            superseded_for: Duration::from_secs(11),
            bound,
        })
    );

    // Node 2 names nothing and holds nothing.
    let other = NodeNames::new(2, []);
    assert_eq!(
        check_quiet_prefixes(&view, [&named, &other]),
        Err(SnapshotInvariantViolation::UnnamedObject {
            node_id: 1,
            path: superseded,
        })
    );
    for object in store.list_own(GROUP).await.unwrap().owned {
        if object.path() != current {
            store.delete_own(object).await.unwrap();
        }
    }
    assert_eq!(
        check_quiet_prefixes(&bucket.view(), [&named, &other]),
        Ok(())
    );
}

fn gated(bucket: &MemorySnapshotBucket, gates: &SnapshotStoreGates) -> GatedSnapshotStore {
    GatedSnapshotStore::new(Arc::new(bucket.store(1)), gates.clone())
}

#[tokio::test(start_paused = true)]
async fn gates_pause_and_release_in_arrival_order() {
    let bucket = MemorySnapshotBucket::new(1, prefix(), SnapshotStoreFaults::none());
    let gates = SnapshotStoreGates::new();
    let store = gated(&bucket, &gates);
    let put = Gate::before(StoreOp::WriteStaged);
    let answered = Gate::after(StoreOp::WriteStaged);
    gates.hold(put);

    let mut writes = Vec::new();
    let mut paths = Vec::new();
    for index in 1..=3u64 {
        let staged = store.stage(GROUP, index, body(b"x")).await.unwrap();
        let path = match &staged {
            crate::snapshot_store::StagedSnapshot::Object(object) => object.path(),
            crate::snapshot_store::StagedSnapshot::Inline { .. } => panic!("staged inline"),
        };
        paths.push(path);
        let writer = store.clone();
        writes.push(tokio::spawn(
            async move { writer.write_staged(staged).await },
        ));
        gates
            .wait_arrived(put, usize::try_from(index).unwrap())
            .await;
    }
    assert_eq!(gates.passed(put), 0);
    assert!(bucket.keys().is_empty(), "no PUT reached the store");

    gates.release(put, 1);
    gates.wait_passed(answered, 1).await;
    assert_eq!(gates.passed_subjects(put), paths[..1]);
    assert_eq!(bucket.keys(), BTreeSet::from([paths[0].clone()]));

    gates.release(put, 2);
    for write in writes {
        write.await.unwrap().unwrap();
    }
    assert_eq!(gates.passed_subjects(put), paths);
    assert_eq!(gates.passed_subjects(answered), paths);
    let tickets: Vec<usize> = gates
        .log()
        .iter()
        .filter(|event| event.gate == put && event.pass == GatePass::Passed)
        .map(|event| event.ticket)
        .collect();
    assert_eq!(tickets, [0, 1, 2]);
}

#[tokio::test(start_paused = true)]
async fn a_build_between_a_list_and_its_answer_is_not_listed() {
    let bucket = MemorySnapshotBucket::new(1, prefix(), SnapshotStoreFaults::none());
    let gates = SnapshotStoreGates::new();
    let store = gated(&bucket, &gates);
    let listed = Gate::after(StoreOp::ListOwn);
    gates.hold(listed);
    let lister = store.clone();
    let pass = tokio::spawn(async move { lister.list_own(GROUP).await });
    gates.wait_arrived(listed, 1).await;

    // The build runs whole while the pass holds its listing.
    let (path, written) = build(&store, 1, b"new").await;
    written.unwrap();

    gates.open(listed);
    let listing = pass.await.unwrap().unwrap();
    assert!(listing.owned.is_empty(), "the LIST acted before the PUT");
    assert_eq!(bucket.keys(), BTreeSet::from([path]));
    assert_eq!(gates.passed(Gate::before(StoreOp::WriteStaged)), 1);
}

#[tokio::test(start_paused = true)]
async fn the_memory_store_is_a_shared_snapshot_store() {
    let bucket = MemorySnapshotBucket::new(1, prefix(), SnapshotStoreFaults::none());
    let store: crate::snapshot_store::SharedSnapshotStore = Arc::new(bucket.store(1));
    let location = store
        .upload(
            crate::snapshot_store::SnapshotKey {
                raft_group_id: 0,
                snapshot_id: "group-0-1-42".to_owned(),
            },
            Bytes::from_static(b"legacy"),
        )
        .await
        .unwrap();
    let key = crate::snapshot_store::OwnedKey::parse(&prefix(), &key_of(&location)).unwrap();
    assert_eq!((key.owner().node_id(), key.index()), (1, 42));
    store.verify_uploaded(&location).await.unwrap();
    assert_eq!(store.download(&location).await.unwrap(), b"legacy");
}
