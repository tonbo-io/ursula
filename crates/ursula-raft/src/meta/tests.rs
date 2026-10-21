//! Tests of the meta replica: the state machine's dispatch, its inline
//! snapshot record, the restore refusals, and one replica's life on a real
//! journal. Behaviour across replicas (elections, installs between nodes,
//! recovery plans) is exercised end to end by the meta DST family and the
//! integration tests.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use openraft::EntryPayload;
use openraft::LogId;
use openraft::alias::EntryOf;
use openraft::alias::EntryPayloadOf;
use openraft::alias::LogIdOf;
use openraft::alias::SnapshotMetaOf;
use openraft::alias::StoredMembershipOf;
use openraft::entry::RaftEntry;
use openraft::rt::WatchReceiver;
use openraft::storage::IOFlushed;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use openraft::vote::RaftLeaderId;
use ursula_config::WalFsync;
use ursula_control::ControlCommand;
use ursula_control::ControlPlaneState;
use ursula_control::HistoryCause;
use ursula_control::samples;
use ursula_proto::admin::RaftTargetKey;
use ursula_proto::admin::RecoveryGateStatus;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::ViewStage;
use ursula_runtime::GroupEngineMetrics;
use ursula_runtime::HistoryPublisher;
use ursula_runtime::SnapshotCompression;
use ursula_runtime::SnapshotLocation;
use ursula_runtime::SnapshotLocationKind;
use ursula_runtime::SnapshotPointer;
use ursula_shard::RaftGroupId;
use ursula_shard::StaticShardMap;
use ursula_stream::HistoryLeader;

use super::MetaCommand;
use super::MetaResponse;
use super::MetaState;
use super::handle::MetaWriteError;
use super::replica::MetaReplica;
use super::replica::MetaReplicaConfig;
use super::replica::MetaReplicaError;
use super::replica::MetaReplicaTasks;
use super::replica::WriteOrigin;
use super::snapshot::META_SNAPSHOT_MAX_STATE_BYTES;
use super::snapshot::MetaSnapshotError;
use super::state_machine::META_GROUP;
use super::state_machine::MetaStateMachine;
use super::state_machine::meta_log_id;
use super::watch::PlacementPublisher;
use crate::RaftGroupFileLogStore;
use crate::RaftGroupHandleRegistry;
use crate::RaftWal;
use crate::replica::ReplicaStateMachine;
use crate::state_machine::RecordSite;
use crate::state_machine::read_snapshot_record;
use crate::types::RaftCommand;
use crate::types::RaftGroupResponse;
use crate::types::SnapshotData;
use crate::types::UrsulaRaftTypeConfig as C;

type LeaderId = <C as openraft::RaftTypeConfig>::LeaderId;

fn metrics() -> GroupEngineMetrics {
    ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics()
}

pub(super) fn start_wal(root: &std::path::Path) -> RaftWal {
    RaftWal::start(root, WalFsync::Always, &StaticShardMap::new(1, 1).unwrap()).unwrap()
}

/// A log id of a leader other than node 1 in a term other than 1, so a
/// meta log id that drops or swaps a part shows.
fn log_id(index: u64) -> LogIdOf<C> {
    LogId {
        leader_id: LeaderId::new(4, 2),
        index,
    }
}

fn history_id(id: u64) -> NonZeroU64 {
    NonZeroU64::new(id).unwrap()
}

fn new_history(id: u64, previous: u64, cause: HistoryCause) -> MetaCommand {
    MetaCommand::NewHistory {
        history: history_id(id),
        previous,
        kept: 1,
        leader: HistoryLeader {
            term: 4,
            node_id: 2,
        },
        cause,
    }
}

pub(super) fn register(node: u64) -> ControlCommand {
    ursula_control::samples::register(node, 10_u64.saturating_add(node))
}

/// A meta state machine over the meta journal's store in `wal`, and the
/// publisher of its views.
/// `command` as the meta log carries it.
pub(super) fn control(command: ControlCommand) -> RaftCommand {
    RaftCommand::Meta(MetaCommand::Control(command))
}

fn state_machine(
    wal: &RaftWal,
) -> (
    Arc<RaftGroupFileLogStore>,
    MetaStateMachine,
    PlacementPublisher,
) {
    let store = wal.open_meta(metrics()).unwrap();
    let placement = PlacementPublisher::default();
    let machine = MetaStateMachine::new(
        store.snapshot_record(),
        HistoryPublisher::default(),
        placement.clone(),
        store.apply_stop_signal(),
    );
    (store, machine, placement)
}

/// Applies `entries` through OpenRaft's apply entry point.
async fn apply(
    machine: &mut MetaStateMachine,
    entries: Vec<(u64, EntryPayloadOf<C>)>,
) -> Result<(), std::io::Error> {
    let entries = entries
        .into_iter()
        .map(|(index, payload)| Ok((EntryOf::<C>::new(log_id(index), payload), None)));
    machine.apply(futures_util::stream::iter(entries)).await
}

fn meta(command: MetaCommand) -> EntryPayloadOf<C> {
    EntryPayload::Normal(RaftCommand::Meta(command))
}

/// Every control command applies through the kernel at its own entry's log
/// id, after the history event began the kernel's history at its entry's.
/// A dispatch that applied at another log id, or skipped `begin_history`,
/// leaves another state than the kernel's own run of the same commands.
#[tokio::test]
async fn control_commands_apply_through_the_kernel_at_their_entry_log_id() {
    let dir = tempfile::tempdir().unwrap();
    let wal = start_wal(dir.path());
    let (_store, mut machine, placement) = state_machine(&wal);
    let nodes = [1, 2, 3];
    let mut commands = nodes.map(register).to_vec();
    commands.push(ControlCommand::Bootstrap {
        params: samples::params(&nodes, 4, 3, 2),
        nodes: samples::node_set(&nodes),
        now_ms: 40,
    });

    let mut expected = ControlPlaneState::default();
    expected.begin_history(meta_log_id(log_id(2)), HistoryCause::Genesis);
    let mut expected_answers = Vec::new();
    for (index, command) in (3_u64..).zip(commands.iter().cloned()) {
        expected_answers.push(MetaResponse::Control(
            expected.apply(meta_log_id(log_id(index)), command),
        ));
    }

    assert_eq!(
        machine.apply_command(log_id(2), new_history(0xa1, 0, HistoryCause::Genesis)),
        MetaResponse::History
    );
    let answers = (3_u64..)
        .zip(commands)
        .map(|(index, command)| machine.apply_command(log_id(index), MetaCommand::Control(command)))
        .collect::<Vec<_>>();
    assert_eq!(answers, expected_answers);
    assert!(
        expected.cluster().is_some(),
        "the run ends bootstrapped, so its placements carry the log ids"
    );

    // Through OpenRaft's entry point, the batch publishes one view of it.
    apply(&mut machine, vec![(7, EntryPayload::Blank)])
        .await
        .unwrap();
    let view = placement.subscribe().current();
    assert_eq!(*view.state, expected);
    assert_eq!(view.stage, ViewStage::Applied {
        applied: meta_log_id(log_id(7)),
        history: meta_log_id(log_id(2)),
    });
}

/// The history guard: an event whose `previous` is not the current history
/// applies nothing, and in particular does not begin a history in the
/// kernel, so a stale plan's event cannot cut a formation. The driver reads
/// the refusal as one to propose again.
#[test]
fn a_history_event_whose_previous_is_not_current_applies_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let wal = start_wal(dir.path());
    let (_store, mut machine, _placement) = state_machine(&wal);
    machine.apply_command(log_id(1), new_history(0xa1, 0, HistoryCause::Genesis));

    let stale = machine.apply_command(log_id(2), new_history(0xb2, 0, HistoryCause::Plan));
    assert_eq!(stale, MetaResponse::HistoryMismatch { current: 0xa1 });
    assert!(MetaStateMachine::history_refused(&RaftGroupResponse::Meta(
        stale
    )));
    let mut forming = ControlPlaneState::default();
    forming.begin_history(meta_log_id(log_id(1)), HistoryCause::Genesis);
    let control = |machine: &MetaStateMachine| machine.control().clone();
    assert_eq!(control(&machine), forming, "the stale event began nothing");
    assert_eq!(
        machine.history().map(|event| event.id),
        Some(history_id(0xa1))
    );

    let planned = machine.apply_command(log_id(3), new_history(0xc3, 0xa1, HistoryCause::Plan));
    assert_eq!(planned, MetaResponse::History);
    assert!(!MetaStateMachine::history_refused(
        &RaftGroupResponse::Meta(planned)
    ));
    let event = machine.history().unwrap();
    assert_eq!(
        (event.id, event.event_index, event.previous, event.kept),
        (history_id(0xc3), 3, 0xa1, 1)
    );
    assert_eq!(control(&machine), ControlPlaneState::FormationCut);
}

/// A stream entry stops the meta apply: the replica records the stop at the
/// entry, signals its log store, and applies nothing after it.
#[tokio::test]
async fn a_stream_entry_stops_the_meta_apply() {
    let dir = tempfile::tempdir().unwrap();
    let wal = start_wal(dir.path());
    let (store, mut machine, _placement) = state_machine(&wal);
    let stream = EntryPayload::Normal(RaftCommand::Stream(
        ursula_stream::StreamCommand::NewHistory {
            history: history_id(0xd4),
            now_ms: 1,
            previous: 0,
            kept: 0,
            leader: HistoryLeader {
                term: 4,
                node_id: 2,
            },
        },
    ));
    apply(&mut machine, vec![
        (1, EntryPayload::Blank),
        (2, stream),
        (3, EntryPayload::Blank),
    ])
    .await
    .expect_err("a stream entry stops the apply");
    let stopped = machine
        .apply_health
        .stopped()
        .expect("the stop is recorded");
    assert_eq!(stopped.index, 2);
    assert_eq!(
        stopped.kind,
        ursula_runtime::ApplyStopKind::InvariantViolation
    );
    assert_eq!(stopped.last_applied, Some(log_id(1)));
    assert!(store.apply_stop_signal().load(Ordering::Acquire));
    assert_eq!(
        machine.applied_state().await.unwrap().0,
        Some(log_id(1)),
        "nothing after the stop applied"
    );
}

/// A build writes the snapshot record with an inline pointer in the meta
/// journal's directory, in the log's generation, after the journal synced,
/// and a new state machine over the record restores the state, the history
/// and the view.
#[tokio::test]
async fn a_meta_build_records_an_inline_snapshot_that_a_restore_reads_back() {
    let dir = tempfile::tempdir().unwrap();
    let wal = start_wal(dir.path());
    let (store, mut machine, _placement) = state_machine(&wal);
    apply(&mut machine, vec![
        (1, meta(new_history(0xa1, 0, HistoryCause::Genesis))),
        (2, meta(MetaCommand::Control(register(2)))),
    ])
    .await
    .unwrap();
    let built = machine
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    assert_eq!(built.meta.last_log_id, Some(log_id(2)));

    let record = store.snapshot_record();
    assert!(record.path().starts_with(dir.path().join("meta")));
    let persisted = read_snapshot_record(&RecordSite::Store(record.clone()), META_GROUP)
        .unwrap()
        .expect("the build wrote a record of the log's generation");
    assert_eq!(persisted.meta, built.meta);
    let SnapshotLocation::Inline { bytes } = &persisted.pointer.location else {
        panic!(
            "a meta snapshot is inline: {:?}",
            persisted.pointer.location
        );
    };
    let state: MetaState = ursula_stream::msgpack::decode(bytes).unwrap();
    assert_eq!(state.history, machine.history());

    let placement = PlacementPublisher::default();
    let history = HistoryPublisher::default();
    let mut restored = MetaStateMachine::new(
        record,
        history.clone(),
        placement.clone(),
        store.apply_stop_signal(),
    );
    restored.restore().unwrap();
    assert_eq!(history.subscribe().current(), machine.history());
    assert_eq!(*placement.subscribe().current(), machine.view_for_test());
    assert_eq!(
        restored
            .get_current_snapshot()
            .await
            .unwrap()
            .map(|snapshot| snapshot.meta),
        Some(built.meta)
    );
}

/// A build captured before an install of a newer snapshot writes nothing
/// when it ends: the record keeps the install's snapshot, and the build
/// answers with it.
#[tokio::test]
async fn a_meta_build_not_above_the_current_snapshot_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let wal = start_wal(dir.path());
    let (store, mut machine, _placement) = state_machine(&wal);
    apply(&mut machine, vec![(
        1,
        meta(new_history(0xa1, 0, HistoryCause::Genesis)),
    )])
    .await
    .unwrap();
    let mut older = machine.get_snapshot_builder().await;

    let newer = SnapshotMetaOf::<C> {
        last_log_id: Some(log_id(9)),
        last_membership: StoredMembershipOf::<C>::default(),
    };
    let state = MetaState {
        history: machine.history(),
        control: samples::sample_state(),
    };
    let pointer = super::snapshot::MetaSnapshotPointer::new("meta-newer".to_owned(), &state)
        .unwrap()
        .into_pointer()
        .encode_binary()
        .unwrap();
    machine
        .install_snapshot(&newer, SnapshotData::new(pointer))
        .await
        .unwrap();
    let record = store.snapshot_record();
    let installed = std::fs::read(record.path()).unwrap();

    let built = older.build_snapshot().await.unwrap();
    assert_eq!(built.meta, newer);
    assert_eq!(std::fs::read(record.path()).unwrap(), installed);
}

/// An install whose pointer is not the meta group's own (an object of a
/// snapshot store, or an inline state above the bound) is refused before
/// anything changes or is recorded.
#[tokio::test]
async fn a_meta_install_refuses_a_pointer_that_is_not_inline_or_within_the_bound() {
    let dir = tempfile::tempdir().unwrap();
    let wal = start_wal(dir.path());
    let (store, mut machine, _placement) = state_machine(&wal);
    let meta = SnapshotMetaOf::<C> {
        last_log_id: Some(log_id(9)),
        last_membership: StoredMembershipOf::<C>::default(),
    };
    let bound = usize::try_from(META_SNAPSHOT_MAX_STATE_BYTES).unwrap();
    for location in [s3_location(), SnapshotLocation::Inline {
        bytes: vec![0; bound.saturating_add(1)],
    }] {
        let pointer = SnapshotPointer {
            snapshot_id: "meta-4-2-9".to_owned(),
            location,
        }
        .encode_binary()
        .unwrap();
        let error = machine
            .install_snapshot(&meta, SnapshotData::new(pointer))
            .await
            .expect_err("the pointer is refused");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }
    assert_eq!(machine.applied_state().await.unwrap().0, None);
    assert!(!store.snapshot_record().path().exists());
}

fn s3_location() -> SnapshotLocation {
    SnapshotLocation::S3 {
        key: "snapshots/node-1/group-0/00000000000000000019-0123456789abcdef.snap".to_owned(),
        size_bytes: 4096,
        stored_size_bytes: 1234,
        compression: SnapshotCompression::Zstd,
    }
}

/// Plants a snapshot record through `log_id(19)` with `location`, over a
/// meta log whose entries 1 to 3 of term 1 lie below it in log-id order,
/// so the log contradicts the record.
async fn plant_record(wal: &RaftWal, location: SnapshotLocation) {
    let mut store = wal.open_meta(metrics()).unwrap();
    let below = |index| LogId {
        leader_id: LeaderId::new(1, 1),
        index,
    };
    store
        .append(
            (1..=3).map(|index| EntryOf::<C>::new(below(index), EntryPayload::Blank)),
            IOFlushed::noop(),
        )
        .await
        .unwrap();
    let record = store.snapshot_record();
    let pointer = SnapshotPointer {
        snapshot_id: "meta-4-2-2".to_owned(),
        location,
    }
    .encode_binary()
    .unwrap();
    let bytes = crate::state_machine::snapshot_record_bytes(
        record.generation(),
        SnapshotMetaOf::<C> {
            last_log_id: Some(log_id(2)),
            last_membership: StoredMembershipOf::<C>::default(),
        },
        pointer,
    );
    std::fs::write(record.path(), bytes).unwrap();
}

fn config() -> MetaReplicaConfig {
    MetaReplicaConfig {
        node: 1,
        engine: crate::RaftEngineConfig::default(),
        claim: samples::sample_genesis_claim(),
    }
}

async fn open(
    wal: &RaftWal,
    registry: &RaftGroupHandleRegistry,
) -> Result<MetaReplica, MetaReplicaError> {
    MetaReplica::open(
        &config(),
        wal,
        metrics(),
        crate::SingleNodeRaftNetworkFactory,
        registry,
        &MetaReplicaTasks::default(),
    )
    .await
}

/// On a node whose snapshot store is S3, the meta replica's restore still
/// takes only an inline record: a record naming an S3 object, which would
/// live in data group 0's owned directory, is refused as not inline. The
/// pointer is parsed before the record is checked against the log, so the
/// refusal names the pointer, not the log the record contradicts. A record
/// above the bound is refused the same way.
#[tokio::test]
async fn a_meta_restore_refuses_a_non_inline_record_on_a_node_with_an_s3_snapshot_store() {
    let bucket = ursula_runtime::snapshot_harness::MemorySnapshotBucket::new(
        1,
        ursula_runtime::SnapshotPrefix::try_from("snapshots").unwrap(),
        ursula_runtime::snapshot_harness::SnapshotStoreFaults::none(),
    );
    let registry = RaftGroupHandleRegistry::default();
    registry.set_snapshot_store(Some(Arc::new(bucket.store(1))));
    let bound = usize::try_from(META_SNAPSHOT_MAX_STATE_BYTES).unwrap();
    let cases = [
        (s3_location(), MetaSnapshotError::NotInline {
            kind: SnapshotLocationKind::S3,
        }),
        (
            SnapshotLocation::Inline {
                bytes: vec![0; bound.saturating_add(1)],
            },
            MetaSnapshotError::TooLarge {
                bytes: META_SNAPSHOT_MAX_STATE_BYTES.saturating_add(1),
                bound: META_SNAPSHOT_MAX_STATE_BYTES,
            },
        ),
    ];
    for (location, refusal) in cases {
        let dir = tempfile::tempdir().unwrap();
        let wal = start_wal(dir.path());
        plant_record(&wal, location).await;
        match open(&wal, &registry).await {
            Err(MetaReplicaError::Pointer(error)) => assert_eq!(error, refusal),
            other => panic!("the record is refused as not the meta group's: {other:?}"),
        }
        assert!(registry.meta().is_none());
    }
}

/// Waits until the placement view applied an entry above `above`.
async fn view_applied_above(
    registry: &RaftGroupHandleRegistry,
    above: u64,
) -> Arc<ursula_control::PlacementView> {
    let mut watch = registry.placement_watch();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let view = watch.current();
            if view
                .applied()
                .is_some_and(|applied| applied.get().index > above)
            {
                return view;
            }
            assert!(watch.changed().await, "the publisher outlives the test");
        }
    })
    .await
    .expect("the view moves")
}

/// Node 1's meta replica on `wal`, published in `registry` and formed
/// alone: its gate opened for a fresh bootstrap, its Raft initialized with
/// itself, and its genesis history event applied.
pub(super) async fn form_alone(wal: &RaftWal, registry: &RaftGroupHandleRegistry) -> MetaReplica {
    let replica = open(wal, registry).await.unwrap();
    let active = Arc::clone(replica.active());
    active
        .core
        .recovery
        .as_ref()
        .unwrap()
        .allow_fresh_bootstrap()
        .await
        .unwrap();
    active.refresh_elections();
    active
        .core
        .owner()
        .initialize(BTreeSet::from([1]))
        .await
        .unwrap();
    view_applied_above(registry, 0).await;
    replica
}

/// One meta replica on a real journal: it forms alone, its history driver
/// proposes the genesis, it writes, refuses a registration of another node
/// before proposing it, snapshots, stops, and a restart of the WAL brings
/// back its state, its history and its view from the record and the
/// committed log above it.
/// The meta group stays out of the registry's data groups and metrics
/// throughout. Its open gate shows in the registry's recovery gates as
/// `meta`, and the recovery report, which lists only gates that are not
/// open, stays empty.
#[tokio::test]
async fn a_meta_replica_forms_writes_and_restarts_from_its_journal() {
    let dir = tempfile::tempdir().unwrap();
    let registry = RaftGroupHandleRegistry::default();
    let wal = start_wal(dir.path());
    let replica = form_alone(&wal, &registry).await;
    let active = Arc::clone(replica.active());
    let genesis = registry.placement_watch().current();
    let ViewStage::Applied { history, .. } = genesis.stage else {
        panic!("the genesis applied");
    };

    let written = active
        .write(
            vec![control(register(1))],
            WriteOrigin::Local(NodeId::new(1)),
        )
        .await
        .unwrap();
    assert!(matches!(written.as_slice(), [Ok(Ok(_))]), "{written:?}");
    let last = replica.raft().metrics().borrow_watched().last_log_index;
    let refused = active
        .write(
            vec![control(register(2))],
            WriteOrigin::Local(NodeId::new(1)),
        )
        .await
        .expect_err("a node registers only itself");
    assert!(matches!(refused, MetaWriteError::OtherNode {
        origin,
        named,
    } if origin == NodeId::new(1) && named == NodeId::new(2)));
    assert_eq!(
        replica.raft().metrics().borrow_watched().last_log_index,
        last,
        "nothing was proposed"
    );
    let forwarded = active
        .write(vec![control(register(2))], WriteOrigin::Forwarded {
            admitted: NodeId::new(2),
        })
        .await
        .unwrap();
    assert!(matches!(forwarded.as_slice(), [Ok(Ok(_))]), "{forwarded:?}");

    active.core.owner().trigger().snapshot().await.unwrap();
    replica
        .raft()
        .wait(Some(Duration::from_secs(10)))
        .metrics(|metrics| metrics.snapshot.is_some(), "the meta snapshot")
        .await
        .unwrap();
    let forwarded = active
        .write(vec![control(register(3))], WriteOrigin::Forwarded {
            admitted: NodeId::new(3),
        })
        .await
        .unwrap();
    assert!(matches!(forwarded.as_slice(), [Ok(Ok(_))]), "{forwarded:?}");
    let before = registry.placement_watch().current();
    let applied = before.applied().unwrap().get().index;
    assert!(registry.is_empty());
    assert_eq!(
        registry.recovery_gates(),
        BTreeMap::from([(RaftTargetKey::Meta, RecoveryGateStatus::Open)])
    );
    assert!(registry.recovery_report().gated.is_empty());
    assert!(registry.metrics_snapshot().is_empty());
    drop(active);

    replica.shutdown().await.unwrap();
    assert!(registry.meta().is_none());
    wal.shutdown().await.unwrap();
    drop(wal);

    let wal = start_wal(dir.path());
    let replica = open(&wal, &registry).await.unwrap();
    // The restart restores the record and replays the committed log above
    // it. It does not campaign: its gate opens only through the gate
    // drivers, which the meta runtime starts.
    let after = view_applied_above(&registry, applied.saturating_sub(1)).await;
    assert_eq!(after.applied(), before.applied());
    assert_eq!(after.state, before.state);
    let ViewStage::Applied {
        history: restarted, ..
    } = after.stage
    else {
        panic!("the restart applied");
    };
    assert_eq!(restarted, history);
    replica.shutdown().await.unwrap();
}

/// The meta snapshot record lives in the meta journal's directory, apart
/// from data group 0's record in its core's directory, and data group 0's
/// engine opens and stops beside it leaving it byte for byte as it was.
/// Data group 0 builds no snapshot here and has no record to restore.
#[tokio::test]
async fn data_group_zero_never_touches_the_meta_record() {
    let dir = tempfile::tempdir().unwrap();
    let wal = start_wal(dir.path());
    let (meta_store, mut machine, _placement) = state_machine(&wal);
    apply(&mut machine, vec![(
        1,
        meta(new_history(0xa1, 0, HistoryCause::Genesis)),
    )])
    .await
    .unwrap();
    machine
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    let meta_record = meta_store.snapshot_record();
    let recorded = std::fs::read(meta_record.path()).unwrap();

    let placement = ursula_shard::ShardPlacement {
        core_id: ursula_shard::CoreId(0),
        shard_id: ursula_shard::ShardId(0),
        raft_group_id: RaftGroupId(0),
    };
    let bucket = ursula_runtime::snapshot_harness::MemorySnapshotBucket::new(
        1,
        ursula_runtime::SnapshotPrefix::try_from("snapshots").unwrap(),
        ursula_runtime::snapshot_harness::SnapshotStoreFaults::none(),
    );
    let snapshot_store: ursula_runtime::SharedSnapshotStore = Arc::new(bucket.store(1));
    let store = wal
        .open(placement, ursula_control::Admission::Create, metrics())
        .unwrap();
    assert_ne!(store.snapshot_record().path(), meta_record.path());
    // Data group 0 opens and stops beside the meta record.
    let factory = crate::engine::test_support::single_node_factory(&wal)
        .with_snapshot_store(Some(snapshot_store.clone()));
    let engine =
        crate::engine::test_support::start_single_voter_on(&factory, placement, store).await;
    engine.shutdown().await.unwrap();
    assert_eq!(std::fs::read(meta_record.path()).unwrap(), recorded);
}
