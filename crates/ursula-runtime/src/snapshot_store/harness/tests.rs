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
use super::DeleteOutcome;
use super::Gate;
use super::GateError;
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
use super::StoreDelete;
use super::StoreOp;
use super::check_committed_reads_found;
use super::check_named_keys_exist;
use super::check_no_foreign_delete;
use super::check_quiet_prefixes;
use super::check_superseded_without_marker;
use super::contains_marker;
use crate::SnapshotStoreOperation;
use crate::snapshot_store::SnapshotBytesIterator;
use crate::snapshot_store::SnapshotKey;
use crate::snapshot_store::SnapshotLocation;
use crate::snapshot_store::SnapshotPrefix;
use crate::snapshot_store::SnapshotStore;
use crate::snapshot_store::SnapshotStoreError;
use crate::snapshot_store::StagedSnapshot;

const GROUP: RaftGroupId = RaftGroupId(0);
const LONG_AGO: Duration = Duration::from_secs(3600);

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

fn staged_path(staged: &StagedSnapshot) -> String {
    match staged {
        StagedSnapshot::Object(object) => object.path(),
        StagedSnapshot::Inline { .. } => panic!("staged inline"),
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
    let path = staged_path(&staged);
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
        ..SnapshotStoreFaults::every_fault(Duration::from_millis(5))
    }
    .with_stale_lists(PerMille::new(600), 16, Duration::from_secs(1));
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

#[test]
fn a_seeded_profile_leaves_stale_lists_off() {
    for seed in 1..=16 {
        let faults = SnapshotStoreFaults::from_seed(seed, Duration::from_millis(5));
        assert_eq!(faults.stale_list, PerMille::NEVER);
    }
    assert_ne!(
        SnapshotStoreFaults::from_seed(1, Duration::from_millis(5)),
        SnapshotStoreFaults::from_seed(2, Duration::from_millis(5)),
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
    assert_eq!(bucket.pending_late_puts(), 0);
    // A view sees it before any request lands it.
    assert!(bucket.view().objects.contains_key(&path));
    store.list_own(GROUP).await.unwrap();
    assert!(
        bucket
            .events()
            .contains(&SnapshotStoreEvent::LatePutLanded { path })
    );
}

#[tokio::test(start_paused = true)]
async fn a_late_put_can_land_before_its_error_answers() {
    let bucket = MemorySnapshotBucket::new(
        7,
        prefix(),
        only(|faults| {
            faults.late_put = PerMille::ALWAYS;
            faults.late_put_delay = DelayRange::fixed(Duration::from_millis(1));
            faults.request_end_delay = DelayRange::fixed(Duration::from_millis(10));
        }),
    );
    let writer = bucket.store(1);
    let watcher = bucket.clone();
    let write = tokio::spawn(async move { build(&writer, 1, b"committed").await });
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(!write.is_finished(), "the PUT has not answered");
    assert_eq!(watcher.keys().len(), 1, "the object landed first");
    let (_path, written) = write.await.unwrap();
    assert!(matches!(written, Err(SnapshotStoreError::Backend { .. })));
}

#[tokio::test(start_paused = true)]
async fn settling_lands_every_late_put() {
    let bucket = MemorySnapshotBucket::new(
        7,
        prefix(),
        only(|faults| {
            faults.late_put = PerMille::ALWAYS;
            faults.late_put_delay = DelayRange::fixed(LONG_AGO);
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
async fn reading_the_bucket_changes_nothing_in_its_log() {
    let bucket = MemorySnapshotBucket::new(
        7,
        prefix(),
        only(|faults| {
            faults.late_put = PerMille::ALWAYS;
            faults.late_put_delay = DelayRange::fixed(Duration::from_millis(1));
        }),
    );
    build(&bucket.store(1), 1, b"a").await.1.unwrap_err();
    tokio::time::sleep(Duration::from_millis(2)).await;
    let before = bucket.events();
    let (_keys, _pending, _view) = (bucket.keys(), bucket.pending_late_puts(), bucket.view());
    assert_eq!(bucket.events(), before);
}

/// Six objects of node 1, of which the first three are then deleted: the
/// bucket, the store, and the keys left.
async fn six_then_three(
    faults: SnapshotStoreFaults,
) -> (
    MemorySnapshotBucket,
    super::MemorySnapshotStore,
    BTreeSet<String>,
) {
    let bucket = MemorySnapshotBucket::new(11, prefix(), faults);
    let store = bucket.store(1);
    for index in 1..=6 {
        build(&store, index, b"x").await.1.unwrap();
    }
    let listing = store.list_own(GROUP).await.unwrap();
    for object in listing.owned.into_iter().take(3) {
        store.delete_own(object).await.unwrap();
    }
    let present = bucket.keys();
    assert_eq!(present.len(), 3);
    (bucket, store, present)
}

/// The keys a LIST left out of and added to `present`.
async fn misreported(
    store: &super::MemorySnapshotStore,
    present: &BTreeSet<String>,
) -> (BTreeSet<String>, BTreeSet<String>) {
    let listed: BTreeSet<String> = store
        .list_own(GROUP)
        .await
        .unwrap()
        .owned
        .iter()
        .map(|object| object.path().to_owned())
        .collect();
    (
        present.difference(&listed).cloned().collect(),
        listed.difference(present).cloned().collect(),
    )
}

#[tokio::test(start_paused = true)]
async fn a_stale_list_omits_new_keys_and_repeats_deleted_ones() {
    // The faults start strict: the bucket keeps its history anyway.
    let (bucket, store, present) = six_then_three(SnapshotStoreFaults::none()).await;
    bucket.set_faults(SnapshotStoreFaults::none().with_stale_lists(PerMille::ALWAYS, 64, LONG_AGO));
    let (mut omitted, mut repeated) = (BTreeSet::new(), BTreeSet::new());
    for _ in 0..16 {
        let (left_out, again) = misreported(&store, &present).await;
        omitted.extend(left_out);
        repeated.extend(again);
    }
    assert!(!omitted.is_empty(), "some LIST left out a recent key");
    assert!(!repeated.is_empty(), "some LIST repeated a deleted key");
    // A repeated key is gone, and deleting it again is not an error.
    for object in store.list_own(GROUP).await.unwrap().owned {
        if !present.contains(object.path()) {
            store.delete_own(object).await.unwrap();
        }
    }
    assert_eq!(bucket.keys(), present, "a stale LIST changes nothing");
}

#[tokio::test(start_paused = true)]
async fn a_stale_list_lists_every_key_once_the_changes_are_old() {
    let stale_for = Duration::from_secs(5);
    let (bucket, store, present) = six_then_three(SnapshotStoreFaults::none()).await;
    bucket.set_faults(SnapshotStoreFaults::none().with_stale_lists(
        PerMille::ALWAYS,
        64,
        stale_for,
    ));
    tokio::time::sleep(stale_for).await;
    for _ in 0..16 {
        assert_eq!(
            misreported(&store, &present).await,
            (BTreeSet::new(), BTreeSet::new())
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_stale_list_misreports_only_the_last_changes_of_its_window() {
    // A window of one: only the last delete may be repeated, and no create.
    let (bucket, store, present) = six_then_three(SnapshotStoreFaults::none()).await;
    bucket.set_faults(SnapshotStoreFaults::none().with_stale_lists(PerMille::ALWAYS, 1, LONG_AGO));
    let last_deleted = bucket
        .events()
        .iter()
        .rev()
        .find_map(|event| match event {
            SnapshotStoreEvent::Delete { path, .. } => Some(path.clone()),
            _ => None,
        })
        .unwrap();
    for _ in 0..16 {
        let (left_out, again) = misreported(&store, &present).await;
        assert!(left_out.is_empty());
        assert!(again.is_empty() || again == BTreeSet::from([last_deleted.clone()]));
    }
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
    // The next request lands what is due.
    let bytes = store.download(&location).await.unwrap();
    assert_eq!(bytes, b"direct", "the object is the direct write's");
    assert!(
        bucket
            .events()
            .contains(&SnapshotStoreEvent::LatePutRefused { path })
    );
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
async fn the_opt_in_failures_answer_errors() {
    let bucket = MemorySnapshotBucket::new(1, prefix(), SnapshotStoreFaults::none());
    let store = bucket.store(1);
    let (path, written) = build(&store, 1, b"x").await;
    let location = written.unwrap();
    let object = store.list_own(GROUP).await.unwrap().owned.remove(0);

    bucket.set_faults(SnapshotStoreFaults::none().with_failed_requests(
        PerMille::ALWAYS,
        PerMille::NEVER,
        PerMille::ALWAYS,
    ));
    assert!(matches!(
        store.list_own(GROUP).await,
        Err(SnapshotStoreError::Backend { .. })
    ));
    assert!(matches!(
        store.download(&location).await,
        Err(SnapshotStoreError::Backend { .. })
    ));
    assert!(matches!(
        store.verify_uploaded(&location).await,
        Err(SnapshotStoreError::Backend { .. })
    ));
    assert!(matches!(
        store.delete_own(object.clone()).await,
        Err(SnapshotStoreError::Backend { .. })
    ));
    assert_eq!(
        bucket.keys(),
        BTreeSet::from([path.clone()]),
        "a failed DELETE keeps it"
    );

    bucket.set_faults(SnapshotStoreFaults::none().with_failed_requests(
        PerMille::NEVER,
        PerMille::ALWAYS,
        PerMille::NEVER,
    ));
    assert!(matches!(
        store.delete_own(object).await,
        Err(SnapshotStoreError::Backend { .. })
    ));
    assert!(
        bucket.keys().is_empty(),
        "a DELETE whose answer was lost deleted"
    );

    let events = bucket.events();
    assert!(events.contains(&SnapshotStoreEvent::TransientError {
        node_id: 1,
        operation: SnapshotStoreOperation::Head,
        path: path.clone(),
    }));
    let outcomes: Vec<DeleteOutcome> = events
        .iter()
        .filter_map(|event| match event {
            SnapshotStoreEvent::Delete { outcome, .. } => Some(*outcome),
            _ => None,
        })
        .collect();
    assert_eq!(outcomes, [DeleteOutcome::Failed, DeleteOutcome::AnswerLost]);
}

#[tokio::test(start_paused = true)]
async fn a_snapshot_id_without_an_index_is_refused() {
    let bucket = MemorySnapshotBucket::new(1, prefix(), SnapshotStoreFaults::none());
    let refused = bucket
        .store(1)
        .upload(
            SnapshotKey {
                raft_group_id: 0,
                snapshot_id: "group-0-empty".to_owned(),
            },
            Bytes::from_static(b"empty"),
        )
        .await;
    assert!(matches!(
        refused,
        Err(SnapshotStoreError::MissingField { .. })
    ));
    assert!(bucket.keys().is_empty());
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
async fn an_unscoped_delete_in_node_10s_directory_panics() {
    let bucket = MemorySnapshotBucket::new(1, prefix(), SnapshotStoreFaults::none());
    let (one, ten) = (bucket.store(1), bucket.store(10));
    let location = build(&ten, 1, b"ten").await.1.unwrap();
    one.delete(&location).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn node_1s_list_leaves_out_node_10s_directory() {
    let bucket = MemorySnapshotBucket::new(1, prefix(), SnapshotStoreFaults::none());
    let (one, ten) = (bucket.store(1), bucket.store(10));
    let own = build(&one, 1, b"one").await.1.unwrap();
    build(&ten, 1, b"ten").await.1.unwrap();
    let listing = one.list_own(GROUP).await.unwrap();
    let listed: Vec<&str> = listing.owned.iter().map(|object| object.path()).collect();
    assert_eq!(listed, [key_of(&own).as_str()]);
    assert!(listing.foreign.is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_recorded_foreign_delete_fails_the_i2_check() {
    let bucket =
        MemorySnapshotBucket::recording_foreign_deletes(1, prefix(), SnapshotStoreFaults::none());
    let (one, ten) = (bucket.store(1), bucket.store(10));
    let location = build(&ten, 1, b"ten").await.1.unwrap();
    let object = ten.list_own(GROUP).await.unwrap().owned.remove(0);
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
        Err(SnapshotInvariantViolation::ForeignDelete {
            node_id: 1,
            path: path.clone(),
        })
    );
    let foreign = StoreDelete { node_id: 1, path };
    assert_eq!(bucket.foreign_deletes(), [foreign.clone(), foreign]);
}

#[tokio::test(start_paused = true)]
#[should_panic(expected = "foreign deletes and a reader took only 0")]
async fn a_recording_bucket_panics_when_its_foreign_deletes_go_unread() {
    let bucket =
        MemorySnapshotBucket::recording_foreign_deletes(1, prefix(), SnapshotStoreFaults::none());
    let (one, two) = (bucket.store(1), bucket.store(2));
    let location = build(&two, 1, b"two").await.1.unwrap();
    one.delete(&location).await.unwrap();
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

#[test]
fn the_i2_check_tells_node_1_from_node_10() {
    let view = super::SnapshotStoreView {
        prefix: prefix(),
        objects: BTreeMap::new(),
        deletes: vec![StoreDelete {
            node_id: 1,
            path: "snapshots/node-10/group-0/00000000000000000001-0000000000000001.snap".to_owned(),
        }],
    };
    assert!(matches!(
        check_no_foreign_delete(&view),
        Err(SnapshotInvariantViolation::ForeignDelete { node_id: 1, .. })
    ));
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

    // Node 2 names nothing and holds nothing.
    let other = NodeNames::new(2, []);
    assert_eq!(
        check_quiet_prefixes(&view, [&named, &other]),
        Err(SnapshotInvariantViolation::UnnamedObject {
            node_id: 1,
            path: superseded.clone(),
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

#[tokio::test(start_paused = true)]
async fn i4_ages_an_unnamed_object_from_its_landing_or_its_supersession() {
    let bucket = MemorySnapshotBucket::new(1, prefix(), SnapshotStoreFaults::none());
    let store = bucket.store(1);
    let current = key_of(&build(&store, 2, b"marker").await.1.unwrap());
    // An orphan: no node ever named it, as a late PUT's object.
    let orphan = key_of(&build(&store, 1, b"marker").await.1.unwrap());
    let named = NodeNames::new(1, [current]);
    let marker = |bytes: &[u8]| contains_marker(bytes, b"marker");
    let bound = Duration::from_secs(10);
    let nobody = BTreeMap::new();

    tokio::time::sleep(Duration::from_secs(5)).await;
    let view = bucket.view();
    assert_eq!(view.objects[&orphan].age, Duration::from_secs(5));
    assert_eq!(
        check_superseded_without_marker(&view, [&named], &nobody, bound, marker),
        Ok(())
    );

    tokio::time::sleep(Duration::from_secs(6)).await;
    let view = bucket.view();
    assert_eq!(
        check_superseded_without_marker(&view, [&named], &nobody, bound, marker),
        Err(SnapshotInvariantViolation::SupersededHoldsMarker {
            path: orphan.clone(),
            age: Duration::from_secs(11),
            bound,
        }),
        "an orphan is aged from its landing"
    );
    // Superseded 3 s ago: aged from that, the later event.
    let recently = BTreeMap::from([(orphan.clone(), Duration::from_secs(3))]);
    assert_eq!(
        check_superseded_without_marker(&view, [&named], &recently, bound, marker),
        Ok(())
    );
    // An object without the marker is never a violation.
    let other = |bytes: &[u8]| contains_marker(bytes, b"absent");
    assert_eq!(
        check_superseded_without_marker(&view, [&named], &nobody, bound, other),
        Ok(())
    );
}

fn gated(bucket: &MemorySnapshotBucket, gates: &SnapshotStoreGates) -> GatedSnapshotStore {
    GatedSnapshotStore::new(1, Arc::new(bucket.store(1)), gates.clone())
}

#[tokio::test(start_paused = true)]
async fn gates_pause_and_release_in_arrival_order() {
    let bucket = MemorySnapshotBucket::new(1, prefix(), SnapshotStoreFaults::none());
    let gates = SnapshotStoreGates::new();
    let store = gated(&bucket, &gates);
    let put = Gate::before(1, StoreOp::WriteStaged);
    let answered = Gate::after(1, StoreOp::WriteStaged);
    gates.hold(put);

    let mut writes = Vec::new();
    let mut paths = Vec::new();
    for index in 1..=3u64 {
        let staged = store.stage(GROUP, index, body(b"x")).await.unwrap();
        paths.push(staged_path(&staged));
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

    gates.release(put, 1).unwrap();
    gates.wait_passed(answered, 1).await;
    assert_eq!(gates.passed_subjects(put), paths[..1]);
    assert_eq!(bucket.keys(), BTreeSet::from([paths[0].clone()]));

    gates.release(put, 2).unwrap();
    for write in writes {
        write.await.unwrap().unwrap();
    }
    assert_eq!(gates.passed_subjects(put), paths);
    // The ungated answers come back in whatever order the writes finish.
    let answers: BTreeSet<String> = gates.passed_subjects(answered).into_iter().collect();
    assert_eq!(answers, paths.iter().cloned().collect());
    let tickets: Vec<usize> = gates
        .log()
        .iter()
        .filter(|event| event.gate == put && event.pass == GatePass::Passed)
        .map(|event| event.ticket)
        .collect();
    assert_eq!(tickets, [0, 1, 2]);
}

#[tokio::test(start_paused = true)]
async fn a_dropped_waiter_gives_up_its_turn() {
    let bucket = MemorySnapshotBucket::new(1, prefix(), SnapshotStoreFaults::none());
    let gates = SnapshotStoreGates::new();
    let store = gated(&bucket, &gates);
    let put = Gate::before(1, StoreOp::WriteStaged);
    gates.hold(put);

    let first = store.stage(GROUP, 1, body(b"a")).await.unwrap();
    let first_path = staged_path(&first);
    let writer = store.clone();
    let abandoned = tokio::spawn(async move { writer.write_staged(first).await });
    gates.wait_arrived(put, 1).await;
    let second = store.stage(GROUP, 2, body(b"b")).await.unwrap();
    let second_path = staged_path(&second);
    let writer = store.clone();
    let kept = tokio::spawn(async move { writer.write_staged(second).await });
    gates.wait_arrived(put, 2).await;

    // Ticket 0's future is dropped while it waits.
    abandoned.abort();
    assert!(abandoned.await.unwrap_err().is_cancelled());
    gates.release(put, 1).unwrap();
    kept.await.unwrap().unwrap();
    assert_eq!(bucket.keys(), BTreeSet::from([second_path.clone()]));
    let at_put: Vec<(usize, GatePass, String)> = gates
        .log()
        .into_iter()
        .filter(|event| event.gate == put && event.pass != GatePass::Arrived)
        .map(|event| (event.ticket, event.pass, event.subject))
        .collect();
    assert_eq!(at_put, [
        (0, GatePass::Abandoned, first_path),
        (1, GatePass::Passed, second_path),
    ]);
}

#[test]
fn releasing_a_gate_that_is_not_held_is_refused() {
    let gates = SnapshotStoreGates::new();
    let gate = Gate::before(1, StoreOp::ListOwn);
    assert_eq!(gates.release(gate, 1), Err(GateError::NotHeld { gate }));
    gates.hold(gate);
    assert_eq!(gates.release(gate, 1), Ok(()));
    gates.open(gate);
    assert_eq!(gates.release(gate, 1), Err(GateError::NotHeld { gate }));
}

#[tokio::test(start_paused = true)]
async fn gates_are_kept_per_node() {
    let bucket = MemorySnapshotBucket::new(1, prefix(), SnapshotStoreFaults::none());
    let gates = SnapshotStoreGates::new();
    let one = GatedSnapshotStore::new(1, Arc::new(bucket.store(1)), gates.clone());
    let two = GatedSnapshotStore::new(2, Arc::new(bucket.store(2)), gates.clone());
    gates.hold(Gate::before(1, StoreOp::ListOwn));
    let lister = one.clone();
    let held = tokio::spawn(async move { lister.list_own(GROUP).await });
    gates
        .wait_arrived(Gate::before(1, StoreOp::ListOwn), 1)
        .await;
    // Node 2's LIST is not held by node 1's gate.
    two.list_own(GROUP).await.unwrap();
    assert_eq!(gates.passed_subjects(Gate::before(2, StoreOp::ListOwn)), [
        "node-2/group-0"
    ]);
    gates.open(Gate::before(1, StoreOp::ListOwn));
    held.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_build_between_a_list_and_its_answer_is_not_listed() {
    let bucket = MemorySnapshotBucket::new(1, prefix(), SnapshotStoreFaults::none());
    let gates = SnapshotStoreGates::new();
    let store = gated(&bucket, &gates);
    let listed = Gate::after(1, StoreOp::ListOwn);
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
    assert_eq!(gates.passed(Gate::before(1, StoreOp::WriteStaged)), 1);
}

#[tokio::test(start_paused = true)]
async fn the_memory_store_is_a_shared_snapshot_store() {
    let bucket = MemorySnapshotBucket::new(1, prefix(), SnapshotStoreFaults::none());
    let store: crate::snapshot_store::SharedSnapshotStore = Arc::new(bucket.store(1));
    let location = store
        .upload(
            SnapshotKey {
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
