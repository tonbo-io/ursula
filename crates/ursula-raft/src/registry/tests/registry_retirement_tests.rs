use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use ursula_runtime::SharedSnapshotStore;
use ursula_runtime::SnapshotPointer;
use ursula_shard::RaftGroupId;

use super::FailingReferenceStore;
use super::StaticSnapshotStore;
use super::external_snapshot;
use super::group_snapshot_bytes;
use crate::registry::RaftGroupHandleRegistry;

#[tokio::test]
async fn snapshot_retirement_drains_pins_and_old_lifecycles_never_reopen() {
    let raw = Arc::new(FailingReferenceStore::default());
    let shared: SharedSnapshotStore = raw.clone();
    let registry = RaftGroupHandleRegistry::default();
    let coordinator = registry.snapshot_install_coordinator();
    let old = coordinator.references(7);
    let location = SnapshotPointer::decode(external_snapshot("old").snapshot.get_ref())
        .unwrap()
        .location;
    let lease = old.prepare(&shared, 7, &location).await.unwrap();
    old.commit_current(&location);
    old.publish_current(&shared, 7).await.unwrap();
    let task_coordinator = coordinator.clone();
    let task_store = shared.clone();
    let task = tokio::spawn(async move { task_coordinator.retire_group(7, &task_store).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !old.activity.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        !task.is_finished(),
        "live pin must prevent a successful retirement"
    );
    assert!(old.prepare(&shared, 7, &location).await.is_err());
    assert!(coordinator.begin_group(7).is_err());
    drop(lease);
    task.await.unwrap().unwrap();
    assert!(raw.pins.lock().unwrap().is_empty());
    assert!(raw.current.lock().unwrap().is_none());
    coordinator.begin_group(7).unwrap();
    let new = coordinator.references(7);
    assert!(!Arc::ptr_eq(&old, &new));
    assert!(old.activity.enter().is_err());
    assert!(old.publish_current(&shared, 7).await.is_err());
    assert!(new.activity.enter().is_ok());
}

#[tokio::test]
async fn failed_snapshot_retirement_stays_closed_until_cleanup_succeeds() {
    let raw = Arc::new(FailingReferenceStore::default());
    let shared: SharedSnapshotStore = raw.clone();
    let coordinator = RaftGroupHandleRegistry::default().snapshot_install_coordinator();
    raw.fail_current.store(true, Ordering::SeqCst);
    assert!(coordinator.retire_group(7, &shared).await.is_err());
    assert!(coordinator.begin_group(7).is_err());
    assert!(coordinator.references(7).activity.enter().is_err());
    raw.fail_current.store(false, Ordering::SeqCst);
    coordinator.retire_group(7, &shared).await.unwrap();
    coordinator.begin_group(7).unwrap();
    assert!(coordinator.references(7).activity.enter().is_ok());
}

#[tokio::test]
async fn old_prefetch_guard_cannot_delete_a_newer_cache_owner_for_the_same_pointer() {
    let registry = RaftGroupHandleRegistry::default();
    registry.set_snapshot_store(Some(Arc::new(StaticSnapshotStore {
        bytes: Some(group_snapshot_bytes()),
    })));
    let old = registry
        .prefetch_snapshot_for_install(RaftGroupId(7), external_snapshot("same"))
        .await
        .unwrap();
    let newer = registry
        .prefetch_snapshot_for_install(RaftGroupId(7), external_snapshot("same"))
        .await
        .unwrap();
    let pointer = SnapshotPointer::decode(newer.snapshot.snapshot.get_ref()).unwrap();
    drop(old);
    assert!(
        registry
            .snapshot_install_coordinator()
            .take_prefetched(&pointer)
            .is_some(),
        "cancelling the old prefetch must not erase the newer owner's cache"
    );
    drop(newer);
}
