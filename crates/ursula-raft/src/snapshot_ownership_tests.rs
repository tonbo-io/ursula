//! Snapshot object ownership (#687), through the state machine's builds,
//! sends, restores and the registry's install copy, against a GC pass, on
//! the gated memory store. The rules under test:
//!
//! - a node writes objects only under its own directory, each under a name
//!   no object had, and deletes only objects it listed there;
//! - a build or an install copy holds its object from before its PUT until
//!   the object is current or given up, and a send holds the object it
//!   names until its RPC returns;
//! - a receiver commits only its own copy, and a restore refuses a pointer
//!   outside the node's directory before it reads anything;
//! - a GC pass lists, then reads the holds in one step, then deletes every
//!   listed object nothing holds.
//!
//! Each case runs in the order a test forces with the store's gates. A
//! negative control shows what the rule a case checks prevents, and the
//! stale-LIST case checks instead that its LISTs were stale.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use ursula_runtime::GroupEngine;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupInfraError;
use ursula_runtime::GroupSnapshot;
use ursula_runtime::GroupSnapshotError;
use ursula_runtime::SharedSnapshotStore;
use ursula_runtime::SnapshotLocation;
use ursula_runtime::SnapshotPointer;
use ursula_runtime::SnapshotPrefix;
use ursula_runtime::SnapshotStoreError;
use ursula_runtime::snapshot_harness::Gate;
use ursula_runtime::snapshot_harness::GatedSnapshotStore;
use ursula_runtime::snapshot_harness::MemorySnapshotBucket;
use ursula_runtime::snapshot_harness::NodeNames;
use ursula_runtime::snapshot_harness::PerMille;
use ursula_runtime::snapshot_harness::SnapshotStoreFaults;
use ursula_runtime::snapshot_harness::SnapshotStoreGates;
use ursula_runtime::snapshot_harness::StoreOp;
use ursula_runtime::snapshot_harness::check_named_keys_exist;
use ursula_runtime::snapshot_harness::check_no_foreign_delete;
use ursula_runtime::snapshot_harness::check_quiet_prefixes;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardPlacement;

use super::CurrentSnapshot;
use super::DEFAULT_LATE_WRITE_PASS;
use super::RaftGroupSnapshotBuilder;
use super::RaftGroupStateMachine;
use super::SnapshotBuildCoordinator;
use super::SnapshotInstallCoordinator;
use super::SnapshotRestoreError;
use super::group_snapshot_id;
use super::persist_snapshot_metadata;
use crate::registry::GroupSnapshots;
use crate::rt::time::Instant;
use crate::snapshot_holds::SnapshotHolds;
use crate::types::SnapshotData;
use crate::types::UrsulaRaftTypeConfig;
use crate::types::UrsulaSnapshot;

const GROUP: RaftGroupId = RaftGroupId(7);

fn placement() -> ShardPlacement {
    ShardPlacement {
        core_id: ursula_shard::CoreId(0),
        shard_id: ursula_shard::ShardId(0),
        raft_group_id: GROUP,
    }
}

fn prefix() -> SnapshotPrefix {
    SnapshotPrefix::try_from("snapshots").unwrap()
}

fn log_id(index: u64) -> openraft::alias::LogIdOf<UrsulaRaftTypeConfig> {
    openraft::LogId::new(openraft::vote::RaftLeaderId::new(1, 1), index)
}

fn meta(index: u64) -> openraft::alias::SnapshotMetaOf<UrsulaRaftTypeConfig> {
    openraft::alias::SnapshotMetaOf::<UrsulaRaftTypeConfig> {
        last_log_id: Some(log_id(index)),
        last_membership: Default::default(),
    }
}

fn group(index: u64) -> GroupSnapshot {
    GroupSnapshot {
        placement: placement(),
        group_commit_index: index,
        stream_snapshot: Default::default(),
        stream_append_counts: Vec::new(),
    }
}

/// The object key `location` names.
fn key(location: &SnapshotLocation) -> String {
    super::object_key(location).expect("an object")
}

fn bucket(faults: SnapshotStoreFaults) -> MemorySnapshotBucket {
    MemorySnapshotBucket::new(7, prefix(), faults)
}

/// Node `node_id`'s store on `bucket`, through `gates`.
fn gated(
    bucket: &MemorySnapshotBucket,
    gates: &SnapshotStoreGates,
    node_id: u64,
) -> SharedSnapshotStore {
    Arc::new(GatedSnapshotStore::new(
        node_id,
        Arc::new(bucket.store(node_id)),
        gates.clone(),
    ))
}

/// Restored holds of a new incarnation, with the restore's first pass run.
async fn restored_holds(store: &SharedSnapshotStore) -> Arc<SnapshotHolds> {
    let holds = Arc::new(SnapshotHolds::new(GROUP, DEFAULT_LATE_WRITE_PASS));
    holds.mark_restored();
    holds.collect_garbage(store, Instant::now()).await.unwrap();
    holds
}

/// A builder of the group at `index` for the holds and current snapshot.
async fn builder(
    index: u64,
    store: &SharedSnapshotStore,
    holds: &Arc<SnapshotHolds>,
    current: &Arc<Mutex<Option<CurrentSnapshot>>>,
) -> RaftGroupSnapshotBuilder {
    RaftGroupSnapshotBuilder {
        apply_health: Default::default(),
        placement: placement(),
        snapshot: Ok(Arc::new(group(index))),
        meta: meta(index),
        snapshot_id: group_snapshot_id(GROUP, Some(log_id(index))),
        current_snapshot: current.clone(),
        metadata_serial: Arc::default(),
        snapshot_store: store.clone(),
        metrics: None,
        build: SnapshotBuildCoordinator::default()
            .wait_lease(GROUP.0, 0)
            .await
            .expect("a build permit"),
        log_gauge: Arc::default(),
        log_mark: Default::default(),
        holds: holds.clone(),
        snapshot_metadata_path: None,
        snapshot_record: None,
        work: crate::openraft_work::WorkToken::detached(),
    }
}

/// Builds the group at `index` and answers the object its snapshot names.
async fn build(
    index: u64,
    store: &SharedSnapshotStore,
    holds: &Arc<SnapshotHolds>,
    current: &Arc<Mutex<Option<CurrentSnapshot>>>,
) -> SnapshotLocation {
    let built = builder(index, store, holds, current)
        .await
        .build_snapshot()
        .await
        .expect("a build");
    SnapshotPointer::decode(built.snapshot.pointer_bytes())
        .unwrap()
        .location
}

/// A state machine of the group on `store`, its snapshot record at `path`.
fn machine(
    store: &SharedSnapshotStore,
    install: &SnapshotInstallCoordinator,
    path: Option<&Path>,
) -> RaftGroupStateMachine {
    RaftGroupStateMachine::new_with_stores_and_snapshot_install(
        placement(),
        None,
        None,
        store.clone(),
        SnapshotBuildCoordinator::default(),
        install.clone(),
        path.map(Path::to_path_buf),
    )
}

/// Applies up to `index` on `machine` and builds its snapshot there.
async fn apply_and_build(machine: &mut RaftGroupStateMachine, index: u64) -> SnapshotLocation {
    machine.engine.install_snapshot(group(index)).await.unwrap();
    machine.last_applied_log_id = Some(log_id(index));
    let built = machine
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .expect("a build");
    SnapshotPointer::decode(built.snapshot.pointer_bytes())
        .unwrap()
        .location
}

fn named(node_id: u64, keys: impl IntoIterator<Item = String>) -> NodeNames {
    NodeNames::new(node_id, keys)
}

fn gone(result: Result<Vec<u8>, SnapshotStoreError>) -> bool {
    matches!(result, Err(SnapshotStoreError::NotFound { .. }))
}

/// Where a build is when the pass's LIST and its read of the holds run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum BuildAt {
    /// The build leased its object and has not sent its PUT.
    BeforePut,
    /// The PUT and its check ended, and the object is not current yet.
    BeforeCommit,
    /// The object is current, and its lease dropped.
    AfterCommit,
}

type Building = tokio::task::JoinHandle<Result<UrsulaSnapshot, std::io::Error>>;

/// A build held at the store's gates, taken one step at a time.
struct HeldBuild {
    gates: SnapshotStoreGates,
    at: BuildAt,
    /// How many checks had arrived at the commit gate before this build.
    commits: usize,
    /// The arrivals at the GC pass's LIST gate before the test's pass,
    /// once the test holds that gate.
    lists: Option<usize>,
    building: Option<Building>,
}

const BEFORE_PUT: Gate = Gate::before(1, StoreOp::WriteStaged);
const BEFORE_COMMIT: Gate = Gate::after(1, StoreOp::Head);
const LISTED: Gate = Gate::after(1, StoreOp::ListOwn);

impl HeldBuild {
    /// Starts `builder`, held where it leased its object, before its PUT.
    async fn start(gates: &SnapshotStoreGates, mut builder: RaftGroupSnapshotBuilder) -> Self {
        let puts = gates.arrived(BEFORE_PUT);
        let commits = gates.arrived(BEFORE_COMMIT);
        gates.hold(BEFORE_PUT);
        gates.hold(BEFORE_COMMIT);
        let building = tokio::spawn(async move { builder.build_snapshot().await });
        gates.wait_arrived(BEFORE_PUT, puts.saturating_add(1)).await;
        Self {
            gates: gates.clone(),
            at: BuildAt::BeforePut,
            commits,
            lists: None,
            building: Some(building),
        }
    }

    /// Takes the build on to `to`.
    async fn advance(&mut self, to: BuildAt) {
        while self.at < to {
            match self.at {
                BuildAt::BeforePut => {
                    self.gates.open(BEFORE_PUT);
                    self.gates
                        .wait_arrived(BEFORE_COMMIT, self.commits.saturating_add(1))
                        .await;
                    self.at = BuildAt::BeforeCommit;
                }
                BuildAt::BeforeCommit => {
                    self.gates.open(BEFORE_COMMIT);
                    match self.lists {
                        // The build's own pass reaches the held LIST gate
                        // once its commit ran.
                        Some(lists) => {
                            self.gates
                                .wait_arrived(LISTED, lists.saturating_add(2))
                                .await;
                        }
                        None => self.finish().await,
                    }
                    self.at = BuildAt::AfterCommit;
                }
                BuildAt::AfterCommit => {}
            }
        }
    }

    /// Holds the LIST gate for the test's pass.
    fn hold_the_list(&mut self) -> usize {
        let lists = self.gates.arrived(LISTED);
        self.lists = Some(lists);
        self.gates.hold(LISTED);
        lists
    }

    async fn finish(&mut self) {
        if let Some(building) = self.building.take() {
            building.await.unwrap().expect("the build");
        }
    }
}

/// Interleaving 1: a build against a GC pass, in every order of the build's
/// lease, PUT and commit against the pass's LIST and its read of the holds.
/// The new object survives every order, and the object it replaced goes
/// once a pass read the holds after the commit.
#[tokio::test]
async fn a_build_survives_every_order_against_a_pass() {
    use BuildAt::AfterCommit;
    use BuildAt::BeforeCommit;
    use BuildAt::BeforePut;

    let orders = [
        (BeforePut, BeforePut),
        (BeforePut, BeforeCommit),
        (BeforePut, AfterCommit),
        (BeforeCommit, BeforeCommit),
        (BeforeCommit, AfterCommit),
        (AfterCommit, AfterCommit),
    ];
    for (list_at, read_at) in orders {
        let bucket = bucket(SnapshotStoreFaults::none());
        let gates = SnapshotStoreGates::new();
        let store = gated(&bucket, &gates, 1);
        let holds = restored_holds(&store).await;
        let current = Arc::new(Mutex::new(None));
        let previous = build(1, &store, &holds, &current).await;

        let mut held = HeldBuild::start(&gates, builder(2, &store, &holds, &current).await).await;
        held.advance(list_at).await;
        let lists = held.hold_the_list();
        let pass = {
            let holds = holds.clone();
            let store = store.clone();
            tokio::spawn(async move {
                holds.owe_pass(Instant::now());
                holds.collect_garbage(&store, Instant::now()).await
            })
        };
        gates.wait_arrived(LISTED, lists.saturating_add(1)).await;
        held.advance(read_at).await;
        gates.open(LISTED);
        pass.await.unwrap().unwrap();
        gates.open(BEFORE_PUT);
        gates.open(BEFORE_COMMIT);
        held.finish().await;

        let next = holds.current().expect("the new object is current");
        assert!(
            bucket.keys().contains(&next),
            "{list_at:?}/{read_at:?}: the pass deleted the build's object"
        );
        assert_eq!(
            check_named_keys_exist(&bucket.view(), [&named(1, [next.clone()])]),
            Ok(()),
            "{list_at:?}/{read_at:?}"
        );
        // A later pass deletes the replaced object in every order.
        holds.owe_pass(Instant::now());
        holds.collect_garbage(&store, Instant::now()).await.unwrap();
        assert!(
            gone(store.download(&previous).await),
            "{list_at:?}/{read_at:?}"
        );
        assert_eq!(
            check_quiet_prefixes(&bucket.view(), [&named(1, [next])]),
            Ok(())
        );
    }
}

/// Negative control of interleaving 1: an object leased only after its PUT
/// goes at a pass that listed it before the lease, and the pointer then
/// committed names a missing object (I1).
#[tokio::test]
async fn control_a_lease_after_the_put_loses_the_object() {
    let bucket = bucket(SnapshotStoreFaults::none());
    let gates = SnapshotStoreGates::new();
    let store = gated(&bucket, &gates, 1);
    let holds = restored_holds(&store).await;
    let staged = store
        .stage(
            GROUP,
            1,
            Box::new(std::iter::once(Ok(bytes::Bytes::from_static(b"body")))),
        )
        .await
        .unwrap();
    let location = store.write_staged(staged).await.unwrap().into_location();
    holds.owe_pass(Instant::now());
    holds.collect_garbage(&store, Instant::now()).await.unwrap();
    let _late_lease = holds.lease(&key(&location));
    holds.commit_current(&location);
    assert!(
        check_named_keys_exist(&bucket.view(), [&named(1, [key(&location)])]).is_err(),
        "the late lease protected nothing"
    );
}

/// Interleaving 2, the send hold: the object a send names survives every
/// pass until the send's RPC returns, also after a build replaced it, and
/// goes at the next pass after that.
#[tokio::test]
async fn a_send_holds_its_object_until_the_rpc_returns() {
    for hold_the_send in [true, false] {
        let bucket = bucket(SnapshotStoreFaults::none());
        let store: SharedSnapshotStore = Arc::new(bucket.store(1));
        let install = SnapshotInstallCoordinator::default();
        let mut leader = machine(&store, &install, None);
        leader.restore_persisted_snapshot().await.unwrap();
        let sent = apply_and_build(&mut leader, 1).await;
        let send = leader
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("a snapshot");
        let (pointer, lease) = send.snapshot.into_parts();
        assert_eq!(SnapshotPointer::decode(&pointer).unwrap().location, sent);
        // The control's network drops the lease before its RPC returned.
        let rpc_lease = if hold_the_send {
            Some(lease.expect("a send leases its object"))
        } else {
            drop(lease);
            None
        };
        let replacing = apply_and_build(&mut leader, 2).await;
        leader.holds.owe_pass(Instant::now());
        leader
            .holds
            .collect_garbage(&store, Instant::now())
            .await
            .unwrap();
        // What the receiver reads while the RPC runs.
        let read = store.download(&sent).await;
        if !hold_the_send {
            assert!(
                gone(read),
                "control: without the hold the object goes mid-send"
            );
            continue;
        }
        read.expect("the sent object survives while the send runs");
        drop(rpc_lease);
        assert!(
            leader.holds.gc_due(Instant::now()),
            "the release owes a pass"
        );
        leader
            .holds
            .collect_garbage(&store, Instant::now())
            .await
            .unwrap();
        assert!(gone(store.download(&sent).await));
        assert_eq!(
            check_quiet_prefixes(&bucket.view(), [&named(1, [key(&replacing)])]),
            Ok(())
        );
    }
}

/// The snapshot `send` as it reaches the receiver: the pointer, without
/// the sender's lease, which the sender keeps until its RPC returns.
fn received(send: UrsulaSnapshot) -> (UrsulaSnapshot, Option<crate::types::SnapshotLease>) {
    let UrsulaSnapshot { meta, snapshot } = send;
    let (pointer, lease) = snapshot.into_parts();
    (
        UrsulaSnapshot {
            meta,
            snapshot: SnapshotData::new(pointer),
        },
        lease,
    )
}

/// Interleaving 3, copy on install: the leader replaces and deletes its
/// object right after the follower's GET. The follower's own copy survives
/// the leader's passes and a follower restart. The control commits the
/// leader's object itself: after the leader's pass it is gone, and a
/// restore of a pointer to it is refused before it reads anything.
#[tokio::test]
async fn an_install_copy_survives_the_leaders_passes_and_a_restart() {
    let root = tempfile::tempdir().unwrap();
    let bucket = bucket(SnapshotStoreFaults::none());
    let gates = SnapshotStoreGates::new();
    let leader_store = gated(&bucket, &gates, 1);
    let follower_store = gated(&bucket, &gates, 2);
    let mut leader = machine(&leader_store, &SnapshotInstallCoordinator::default(), None);
    leader.restore_persisted_snapshot().await.unwrap();
    let sent = apply_and_build(&mut leader, 1).await;

    let follower_install = SnapshotInstallCoordinator::default();
    let follower_path = root.path().join("follower.snapshot.json");
    let mut follower = machine(&follower_store, &follower_install, Some(&follower_path));
    follower.restore_persisted_snapshot().await.unwrap();
    let snapshots = GroupSnapshots {
        store: follower_store.clone(),
        install: follower_install.clone(),
        holds: follower.holds.clone(),
    };
    let (incoming, rpc_lease) = received(
        leader
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("a snapshot"),
    );
    let read = Gate::after(2, StoreOp::Get);
    gates.hold(read);
    let copying = tokio::spawn(async move { snapshots.prefetch(GROUP, incoming).await });
    gates.wait_arrived(read, 1).await;
    // The RPC deadline passed: the leader's send ended, and the leader
    // replaced its object and deleted it.
    drop(rpc_lease);
    let current = apply_and_build(&mut leader, 2).await;
    assert!(gone(leader_store.download(&sent).await));
    gates.open(read);
    let prefetched = copying
        .await
        .unwrap()
        .expect("the follower copied what it read");
    follower
        .install_snapshot(&meta(1), prefetched.snapshot.snapshot)
        .await
        .expect("the follower installs its copy");
    let copy = follower.holds.current().expect("the copy is current");
    assert!(copy.starts_with("snapshots/node-2/group-7/"), "{copy}");
    leader.holds.owe_pass(Instant::now());
    leader
        .holds
        .collect_garbage(&leader_store, Instant::now())
        .await
        .unwrap();
    let names = [named(1, [key(&current)]), named(2, [copy.clone()])];
    assert_eq!(check_named_keys_exist(&bucket.view(), &names), Ok(()));
    assert_eq!(check_no_foreign_delete(&bucket.view()), Ok(()));
    drop(follower);

    // A restart of the follower restores its own copy.
    let mut restarted = machine(
        &follower_store,
        &SnapshotInstallCoordinator::default(),
        Some(&follower_path),
    );
    restarted.restore_persisted_snapshot().await.unwrap();
    assert_eq!(restarted.holds.current(), Some(copy));
    assert_eq!(restarted.last_applied_log_id, Some(log_id(1)));

    // The control: a follower whose pointer named the leader's object.
    assert!(
        gone(follower_store.download(&sent).await),
        "control: the leader's object a follower would name is gone"
    );
    let foreign_path = root.path().join("foreign.snapshot.json");
    persist_snapshot_metadata(
        Some(&foreign_path),
        &meta(1),
        &SnapshotPointer {
            snapshot_id: group_snapshot_id(GROUP, Some(log_id(1))),
            location: sent.clone(),
        }
        .encode_binary()
        .unwrap(),
    )
    .unwrap();
    let mut foreign = machine(
        &follower_store,
        &SnapshotInstallCoordinator::default(),
        Some(&foreign_path),
    );
    let refused = foreign
        .restore_persisted_snapshot()
        .await
        .expect_err("a restored pointer to another node's object");
    assert!(
        matches!(refused, SnapshotRestoreError::NotOwned { .. }),
        "{refused:?}"
    );
    assert_eq!(
        GroupEngineError::from(refused),
        GroupEngineError::Infra(GroupInfraError::Snapshot(GroupSnapshotError::ForeignKey {
            raft_group_id: GROUP,
            key: key(&sent),
            directory: Some("snapshots/node-2/group-7/".to_owned()),
        }))
    );
}

/// Interleaving 4, a late install task: once the send's RPC deadline passed
/// and the leader deleted the object it replaced, the follower's GET finds
/// nothing, the install answers `UNAVAILABLE`, and the follower's current
/// snapshot is unchanged.
#[tokio::test]
async fn a_late_install_task_finds_nothing_and_changes_nothing() {
    let bucket = bucket(SnapshotStoreFaults::none());
    let leader_store: SharedSnapshotStore = Arc::new(bucket.store(1));
    let follower_store: SharedSnapshotStore = Arc::new(bucket.store(2));
    let mut leader = machine(&leader_store, &SnapshotInstallCoordinator::default(), None);
    leader.restore_persisted_snapshot().await.unwrap();
    let sent = apply_and_build(&mut leader, 1).await;
    let (incoming, rpc_lease) = received(
        leader
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("a snapshot"),
    );
    drop(rpc_lease);
    apply_and_build(&mut leader, 2).await;

    let install = SnapshotInstallCoordinator::default();
    let mut follower = machine(&follower_store, &install, None);
    follower.restore_persisted_snapshot().await.unwrap();
    let refused = GroupSnapshots {
        store: follower_store.clone(),
        install,
        holds: follower.holds.clone(),
    }
    .prefetch(GROUP, incoming)
    .await
    .expect_err("the object is gone");
    assert!(
        matches!(refused, crate::SnapshotInstallError::SourceMissing { .. }),
        "{refused:?}"
    );
    assert!(refused.peer_backs_off());
    assert!(follower.get_current_snapshot().await.unwrap().is_none());
    assert!(follower.holds.held_keys().is_empty());
    assert!(
        bucket
            .keys()
            .iter()
            .all(|key| !key.starts_with("snapshots/node-2/")),
        "the follower wrote nothing"
    );
    // The control: a follower that committed the leader's pointer instead
    // of a copy would name a missing object (I1).
    assert!(
        check_named_keys_exist(&bucket.view(), [&named(2, [key(&sent)])]).is_err(),
        "control: the late pointer names an object that is gone"
    );
}

/// Interleaving 5, a restart: an overtaken build's object, a failed
/// candidate and a PUT that lands after the restart go at the restore's
/// passes, the first two at once and the late one an install timeout later.
/// The restored current object stays.
#[tokio::test(start_paused = true)]
async fn a_restart_deletes_what_the_last_process_left_and_keeps_its_current() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("group-7.snapshot.json");
    let bucket = bucket(SnapshotStoreFaults::none());
    let store: SharedSnapshotStore = Arc::new(bucket.store(1));
    let mut before = machine(&store, &SnapshotInstallCoordinator::default(), Some(&path));
    before.restore_persisted_snapshot().await.unwrap();
    let current = apply_and_build(&mut before, 1).await;
    let write = |index: u64| {
        let store = store.clone();
        async move {
            let staged = store
                .stage(
                    GROUP,
                    index,
                    Box::new(std::iter::once(Ok(bytes::Bytes::from_static(b"left")))),
                )
                .await
                .unwrap();
            store
                .write_staged(staged)
                .await
                .map(ursula_runtime::OwnedLocation::into_location)
        }
    };
    // An overtaken build's object, which the crash left uncommitted.
    let overtaken = write(2).await.expect("an object");
    // A PUT that answers an error and lands after the restart.
    bucket.set_faults(SnapshotStoreFaults {
        late_put: PerMille::ALWAYS,
        late_put_delay: ursula_runtime::snapshot_harness::DelayRange::fixed(Duration::from_secs(
            60,
        )),
        ..SnapshotStoreFaults::none()
    });
    write(3).await.expect_err("the PUT answers an error");
    bucket.set_faults(SnapshotStoreFaults::none());
    // A failed candidate: its object is written and its record is not, as a
    // publication whose temporary file cannot be written.
    std::fs::create_dir(path.with_extension("json.tmp")).unwrap();
    before.engine.install_snapshot(group(4)).await.unwrap();
    before.last_applied_log_id = Some(log_id(4));
    before
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .expect_err("the publication fails");
    let candidate: Vec<String> = before
        .holds
        .held_keys()
        .into_iter()
        .filter(|held| *held != key(&current))
        .collect();
    assert_eq!(candidate.len(), 1, "the failed candidate stays held");
    assert!(bucket.keys().contains(&candidate[0]));
    drop(before);

    let mut after = machine(&store, &SnapshotInstallCoordinator::default(), Some(&path));
    after.restore_persisted_snapshot().await.unwrap();
    assert_eq!(after.holds.current(), Some(key(&current)));
    assert!(after.holds.gc_due(Instant::now()));
    after
        .holds
        .collect_garbage(&store, Instant::now())
        .await
        .unwrap();
    assert!(gone(store.download(&overtaken).await));
    assert!(
        !bucket.keys().contains(&candidate[0]),
        "the failed candidate goes at the first pass"
    );
    assert_eq!(bucket.pending_late_puts(), 1);
    bucket.wait_for_late_puts().await;
    assert_eq!(bucket.keys().len(), 2, "the late PUT landed");
    let late = Instant::now().checked_add(DEFAULT_LATE_WRITE_PASS).unwrap();
    tokio::time::sleep_until(late).await;
    assert!(
        after.holds.gc_due(Instant::now()),
        "the restore's second pass"
    );
    after
        .holds
        .collect_garbage(&store, Instant::now())
        .await
        .unwrap();
    assert_eq!(
        check_quiet_prefixes(&bucket.view(), [&named(1, [key(&current)])]),
        Ok(())
    );
}

/// Interleaving 6, stale LISTs: a LIST that leaves out recent keys and
/// lists deleted ones again never makes a held key go, and once the store
/// is quiet a pass leaves only what the node names.
#[tokio::test(start_paused = true)]
async fn a_stale_list_never_deletes_a_held_object() {
    let bucket = bucket(SnapshotStoreFaults::none().with_stale_lists(
        PerMille::ALWAYS,
        16,
        Duration::from_secs(5),
    ));
    let store: SharedSnapshotStore = Arc::new(bucket.store(1));
    let install = SnapshotInstallCoordinator::default();
    let mut node = machine(&store, &install, None);
    node.restore_persisted_snapshot().await.unwrap();
    for index in 1..=8 {
        apply_and_build(&mut node, index).await;
        let current = node.holds.current().expect("a current object");
        assert_eq!(
            check_named_keys_exist(&bucket.view(), [&named(1, [current])]),
            Ok(()),
            "build {index}"
        );
    }
    tokio::time::sleep(Duration::from_secs(10)).await;
    node.holds.owe_pass(Instant::now());
    node.holds
        .collect_garbage(&store, Instant::now())
        .await
        .unwrap();
    let current = node.holds.current().expect("a current object");
    assert_eq!(
        check_quiet_prefixes(&bucket.view(), [&named(1, [current])]),
        Ok(())
    );
    // The control: the LISTs were stale, so the case is not vacuous.
    assert!(
        bucket.events().iter().any(|event| matches!(
            event,
            ursula_runtime::snapshot_harness::SnapshotStoreEvent::List { omitted, repeated, .. }
                if !omitted.is_empty() || !repeated.is_empty()
        )),
        "control: a LIST left out a recent key or listed a deleted one"
    );
}

/// Interleaving 7, prefix isolation: node 1's passes never touch node 10's
/// directory (I2), whatever node 10 holds there.
#[tokio::test]
async fn a_pass_of_node_1_leaves_node_10_alone() {
    let bucket = bucket(SnapshotStoreFaults::none());
    let one: SharedSnapshotStore = Arc::new(bucket.store(1));
    let ten: SharedSnapshotStore = Arc::new(bucket.store(10));
    let mut node = machine(&one, &SnapshotInstallCoordinator::default(), None);
    node.restore_persisted_snapshot().await.unwrap();
    let mut tens = BTreeSet::new();
    for index in 1..=3 {
        let staged = ten
            .stage(
                GROUP,
                index,
                Box::new(std::iter::once(Ok(bytes::Bytes::from_static(b"ten")))),
            )
            .await
            .unwrap();
        tens.insert(key(&ten
            .write_staged(staged)
            .await
            .unwrap()
            .into_location()));
        apply_and_build(&mut node, index).await;
    }
    node.holds.owe_pass(Instant::now());
    node.holds
        .collect_garbage(&one, Instant::now())
        .await
        .unwrap();
    let view = bucket.view();
    assert_eq!(check_no_foreign_delete(&view), Ok(()));
    for key in &tens {
        assert!(view.objects.contains_key(key), "{key}");
    }
    assert_eq!(
        check_quiet_prefixes(&view, [&named(1, node.holds.current())]),
        Ok(())
    );

    // The control: a pass that listed the bare prefix `node-1`, without
    // the directory's slash, would list node 10's object too, and its
    // DELETE, which no ownership scopes, breaks I2. `list_own` lists the
    // directory `node-1/`, and `delete_own` takes only an object it listed.
    let recording =
        MemorySnapshotBucket::recording_foreign_deletes(7, prefix(), SnapshotStoreFaults::none());
    let ten: SharedSnapshotStore = Arc::new(recording.store(10));
    let staged = ten
        .stage(
            GROUP,
            1,
            Box::new(std::iter::once(Ok(bytes::Bytes::from_static(b"ten")))),
        )
        .await
        .unwrap();
    let tens = ten.write_staged(staged).await.unwrap().into_location();
    assert!(
        key(&tens).starts_with("snapshots/node-1"),
        "the bare prefix of node 1 matches node 10's object"
    );
    recording.store(1).delete_unscoped(&tens).await.unwrap();
    assert!(
        check_no_foreign_delete(&recording.view()).is_err(),
        "control: an unscoped delete breaks I2"
    );
    assert_eq!(recording.foreign_deletes().len(), 1);
}

/// Interleaving 8: a restore of a pointer outside the node's own
/// directory is refused with `ForeignKey` before anything is read, and a
/// build whose fresh name is taken falls back inline, and the name, which
/// may be its own write, is not held.
#[tokio::test]
async fn a_foreign_restore_is_refused_and_a_name_collision_falls_back_inline() {
    let root = tempfile::tempdir().unwrap();
    let bucket = bucket(SnapshotStoreFaults::none());
    let ten: SharedSnapshotStore = Arc::new(bucket.store(10));
    let staged = ten
        .stage(
            GROUP,
            1,
            Box::new(std::iter::once(Ok(bytes::Bytes::from_static(b"ten")))),
        )
        .await
        .unwrap();
    let foreign = ten.write_staged(staged).await.unwrap().into_location();
    let path = root.path().join("group-7.snapshot.json");
    persist_snapshot_metadata(
        Some(&path),
        &meta(1),
        &SnapshotPointer {
            snapshot_id: group_snapshot_id(GROUP, Some(log_id(1))),
            location: foreign.clone(),
        }
        .encode_binary()
        .unwrap(),
    )
    .unwrap();
    for (store, directory) in [
        (
            Arc::new(bucket.store(1)) as SharedSnapshotStore,
            Some("snapshots/node-1/group-7/".to_owned()),
        ),
        (ursula_runtime::default_snapshot_store(), None),
    ] {
        let mut node = machine(&store, &SnapshotInstallCoordinator::default(), Some(&path));
        let refused = node.restore_persisted_snapshot().await.unwrap_err();
        assert_eq!(
            GroupEngineError::from(refused),
            GroupEngineError::Infra(GroupInfraError::Snapshot(GroupSnapshotError::ForeignKey {
                raft_group_id: GROUP,
                key: key(&foreign),
                directory,
            }))
        );
        assert!(
            !node.holds.gc_due(Instant::now()),
            "a refused restore runs no pass"
        );
    }
    assert_eq!(
        bucket
            .events()
            .iter()
            .filter(|event| matches!(
                event,
                ursula_runtime::snapshot_harness::SnapshotStoreEvent::Get { .. }
            ))
            .count(),
        0,
        "the refusal read nothing"
    );
    // The control: node 10's own pass deletes the object, since node 10
    // holds nothing, so a node 1 that restored it would name a missing
    // object (I1).
    let ten_holds = Arc::new(SnapshotHolds::new(GROUP, DEFAULT_LATE_WRITE_PASS));
    ten_holds.mark_restored();
    ten_holds
        .collect_garbage(&ten, Instant::now())
        .await
        .unwrap();
    assert!(
        check_named_keys_exist(&bucket.view(), [&named(1, [key(&foreign)])]).is_err(),
        "control: the foreign object a restore would name is gone"
    );

    // Every stage draws one name: the second build collides with the first.
    let store: SharedSnapshotStore = Arc::new(bucket.store(1).with_nonces(|| 5));
    let holds = restored_holds(&store).await;
    let current = Arc::new(Mutex::new(None));
    let first = build(1, &store, &holds, &current).await;
    let second = build(1, &store, &holds, &current).await;
    assert!(
        matches!(second, SnapshotLocation::Inline { .. }),
        "{second:?}"
    );
    assert!(
        holds.held_keys().is_empty(),
        "nothing holds the collided name"
    );
    holds.collect_garbage(&store, Instant::now()).await.unwrap();
    assert!(
        gone(store.download(&first).await),
        "the replaced object went"
    );
    assert_eq!(
        check_quiet_prefixes(&bucket.view(), [&named(1, Vec::<String>::new())]),
        Ok(())
    );
}

/// Where a receiver's install copy is when its own GC pass lists and reads
/// the holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum CopyAt {
    /// The copy leased its object and has not sent its PUT.
    BeforePut,
    /// The copy is written and checked, and not installed yet.
    BeforeCommit,
    /// The copy is installed and current.
    AfterCommit,
}

/// The receiver's own GC pass against its install of a copy, in every order
/// of the copy's PUT and commit against the pass's LIST and its read of the
/// holds: the copy survives every order and is the receiver's current
/// object. The negative control drops the copy's lease before the commit:
/// a pass in between deletes the copy the receiver is about to commit.
#[tokio::test]
async fn a_receivers_own_pass_never_deletes_the_copy_it_installs() {
    use CopyAt::AfterCommit;
    use CopyAt::BeforeCommit;
    use CopyAt::BeforePut;

    const COPY_PUT: Gate = Gate::before(2, StoreOp::WriteStaged);
    const RECEIVER_LISTED: Gate = Gate::after(2, StoreOp::ListOwn);
    let orders = [
        (BeforePut, BeforePut, true),
        (BeforePut, BeforeCommit, true),
        (BeforePut, AfterCommit, true),
        (BeforeCommit, BeforeCommit, true),
        (BeforeCommit, AfterCommit, true),
        (AfterCommit, AfterCommit, true),
        // The control.
        (BeforeCommit, BeforeCommit, false),
    ];
    for (list_at, read_at, keep_the_lease) in orders {
        let bucket = bucket(SnapshotStoreFaults::none());
        let gates = SnapshotStoreGates::new();
        let leader_store: SharedSnapshotStore = Arc::new(bucket.store(1));
        let receiver_store = gated(&bucket, &gates, 2);
        let mut leader = machine(&leader_store, &SnapshotInstallCoordinator::default(), None);
        leader.restore_persisted_snapshot().await.unwrap();
        apply_and_build(&mut leader, 1).await;
        let (incoming, _rpc_lease) = received(
            leader
                .get_current_snapshot()
                .await
                .unwrap()
                .expect("a snapshot"),
        );
        let install = SnapshotInstallCoordinator::default();
        let mut receiver = machine(&receiver_store, &install, None);
        receiver.restore_persisted_snapshot().await.unwrap();
        let snapshots = GroupSnapshots {
            store: receiver_store.clone(),
            install: install.clone(),
            holds: receiver.holds.clone(),
        };
        gates.hold(COPY_PUT);
        let mut copying = Some(tokio::spawn(async move {
            snapshots.prefetch(GROUP, incoming).await
        }));
        gates.wait_arrived(COPY_PUT, 1).await;
        let mut at = BeforePut;
        let mut prefetched = None;
        // Set once, when the copy is written.
        let copy_key = std::cell::OnceCell::new();
        // Takes the copy on to `to`.
        macro_rules! advance {
            ($to:expr) => {
                while at < $to {
                    match at {
                        BeforePut => {
                            gates.open(COPY_PUT);
                            let copy = copying
                                .take()
                                .expect("the copy runs")
                                .await
                                .unwrap()
                                .expect("the receiver copied what it read");
                            let handed =
                                SnapshotPointer::decode(copy.snapshot.snapshot.pointer_bytes())
                                    .unwrap();
                            copy_key.set(key(&handed.location)).expect("one copy");
                            if !keep_the_lease {
                                assert!(
                                    install.drop_prefetched_lease(&handed),
                                    "the copy is leased until its commit"
                                );
                            }
                            prefetched = Some(copy);
                            at = BeforeCommit;
                        }
                        BeforeCommit => {
                            let copy = prefetched.take().expect("a copy");
                            receiver
                                .install_snapshot(&meta(1), copy.snapshot.snapshot)
                                .await
                                .expect("the receiver installs its copy");
                            at = AfterCommit;
                        }
                        AfterCommit => {}
                    }
                }
            };
        }
        advance!(list_at);
        let lists = gates.arrived(RECEIVER_LISTED);
        gates.hold(RECEIVER_LISTED);
        let pass = {
            let holds = receiver.holds.clone();
            let store = receiver_store.clone();
            tokio::spawn(async move {
                holds.owe_pass(Instant::now());
                holds.collect_garbage(&store, Instant::now()).await
            })
        };
        gates
            .wait_arrived(RECEIVER_LISTED, lists.saturating_add(1))
            .await;
        advance!(read_at);
        gates.open(RECEIVER_LISTED);
        pass.await.unwrap().unwrap();
        if !keep_the_lease {
            let copy = copy_key.get().cloned().expect("the copy's key");
            assert!(
                check_named_keys_exist(&bucket.view(), [&named(2, [copy])]).is_err(),
                "control: a copy without its lease goes before its commit"
            );
            continue;
        }
        advance!(AfterCommit);
        let copy = copy_key.get().cloned().expect("the copy's key");
        assert_eq!(
            receiver.holds.current().as_deref(),
            Some(copy.as_str()),
            "{list_at:?}/{read_at:?}"
        );
        assert_eq!(
            check_named_keys_exist(&bucket.view(), [&named(2, [copy])]),
            Ok(()),
            "{list_at:?}/{read_at:?}"
        );
    }
}
