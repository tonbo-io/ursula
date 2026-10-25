//! The per-stream usage ledger across the Raft layer: the in-place upgrade
//! from a group snapshot of Ursula 0.10.1, the snapshot codec of a started
//! ledger, acknowledgement across a leader change, and the usage gate.

use std::sync::Arc;
#[cfg(not(madsim))]
use std::time::Duration;

use ursula_runtime::GroupSnapshot;
use ursula_shard::BucketStreamId;
use ursula_stream::StreamCommand;
use ursula_stream::StreamResponse;
use ursula_stream::StreamStateMachine;
use ursula_stream::UsageAck;
use ursula_stream::UsageLedgerProgress;
use ursula_stream::UsageLedgerSnapshot;

use crate::snapshot_codec::decode_group_snapshot;
use crate::snapshot_codec::group_snapshot_frames;

/// A group snapshot that the 0.10.1 tree (d01c57495) encoded from a state
/// machine that applied, in order: a genesis; buckets `tenant-a`,
/// `tenant-b` and `tenant-c`; `tenant-a/notes` (JSON, two records, then one
/// more), `tenant-a/blob` (20,000 bytes, then 100), `tenant-a/gone`
/// (created and deleted), `tenant-b/log` (two writes), a visible snapshot
/// of `tenant-a/notes`, and `tenant-c/purged`, whose bucket was purged.
const SNAPSHOT_0_10_1: &[u8] = include_bytes!("fixtures/group-snapshot-0.10.1.bin");

fn encode(snapshot: GroupSnapshot) -> Vec<u8> {
    group_snapshot_frames(Arc::new(snapshot))
        .collect::<Result<Vec<_>, _>>()
        .expect("encode frames")
        .into_iter()
        .flatten()
        .collect()
}

fn install(snapshot: GroupSnapshot) -> StreamStateMachine {
    let mut machine = StreamStateMachine::restore(snapshot.stream_snapshot).expect("restore");
    machine.restore_usage_ledger(snapshot.usage_ledger);
    machine
}

fn rebuild(machine: &StreamStateMachine, like: &GroupSnapshot) -> GroupSnapshot {
    GroupSnapshot {
        placement: like.placement,
        group_commit_index: like.group_commit_index.saturating_add(10),
        stream_snapshot: machine.snapshot(),
        stream_append_counts: Vec::new(),
        usage_ledger: machine.usage_ledger_snapshot(),
    }
}

fn totals(machine: &StreamStateMachine) -> Vec<(String, [u64; 5])> {
    machine
        .bucket_usage_report()
        .into_iter()
        .map(|row| {
            let usage = row.usage;
            (row.bucket_id, [
                usage.committed_append_bytes,
                usage.committed_records,
                usage.committed_write_units,
                usage.retained_bytes,
                usage.stream_count,
            ])
        })
        .collect()
}

/// The upgrade: a 0.10.1 group snapshot installs with its bucket totals
/// retired and no per-stream usage, reports 0.10.1's `/__ursula/usage`
/// values, and, while its ledger has not started, encodes back to the very
/// bytes 0.10.1 wrote, so a 0.10.1 replica installs what a 0.10.2 leader
/// sends during the rolling upgrade.
#[test]
fn a_group_snapshot_of_0_10_1_upgrades_in_place() {
    let decoded = decode_group_snapshot(SNAPSHOT_0_10_1).expect("a 0.10.1 snapshot decodes");
    assert_eq!(decoded.usage_ledger, UsageLedgerSnapshot::NotStarted);
    assert_eq!(encode(decoded.clone()), SNAPSHOT_0_10_1);

    let mut machine = install(decoded.clone());
    assert_eq!(totals(&machine), vec![
        ("tenant-a".to_owned(), [20_127, 6, 6, 20_124, 2]),
        ("tenant-b".to_owned(), [11, 2, 2, 11, 1]),
        ("tenant-c".to_owned(), [8, 1, 1, 0, 0]),
    ]);
    assert_eq!(
        machine.usage_ledger_progress(),
        UsageLedgerProgress::NotStarted
    );
    assert!(machine.stream_usage_changes(0, 100).changes.is_empty());
    // A 0.10.2 replica that applied writes but no acknowledgement still
    // writes a snapshot 0.10.1 reads.
    let notes = BucketStreamId::new("tenant-a", "notes");
    machine.apply(StreamCommand::Append {
        stream_id: notes.clone(),
        content_type: Some("application/json".to_owned()),
        payload: bytes::Bytes::from_static(b"{\"n\":4}\n"),
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms: 1_700_000_002_000,
    });
    let unstarted = encode(rebuild(&machine, &decoded));
    let reread = decode_group_snapshot(&unstarted).expect("decodes");
    assert_eq!(reread.usage_ledger, UsageLedgerSnapshot::NotStarted);
    assert_eq!(totals(&install(reread))[0].1, [20_135, 7, 7, 20_132, 2]);

    // The first acknowledgement starts the ledger over the live streams:
    // what they wrote before stays retired.
    let history = machine.history_id();
    assert_eq!(
        machine.apply(StreamCommand::AckStreamUsage {
            history,
            through_seq: 0,
        }),
        StreamResponse::StreamUsageAcked {
            ack: UsageAck::Started { usage_seq: 3 },
        }
    );
    let before = totals(&machine);
    machine.apply(StreamCommand::Append {
        stream_id: notes.clone(),
        content_type: Some("application/json".to_owned()),
        payload: bytes::Bytes::from_static(b"{\"n\":5}\n"),
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms: 1_700_000_003_000,
    });
    machine.apply(StreamCommand::DeleteStream {
        stream_id: BucketStreamId::new("tenant-a", "blob"),
        now_ms: 1_700_000_004_000,
    });
    let changes = machine.stream_usage_changes(0, 100).changes;
    let listed = changes
        .iter()
        .map(|change| {
            (
                change.stream_id.stream_id.as_str(),
                change.usage.committed_append_bytes,
                change.end,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(listed, vec![
        ("log", 0, None),
        ("notes", 8, None),
        ("blob", 0, Some(ursula_stream::StreamEnd::Deleted)),
    ]);
    let after = totals(&machine);
    assert_eq!(after[0].1[0], before[0].1[0].saturating_add(8));
    assert_eq!(after[0].1[4], 1);

    // A started ledger rides in the group snapshot and restores exactly.
    let started = rebuild(&machine, &decoded);
    let bytes = encode(started.clone());
    assert_ne!(bytes, unstarted);
    let reread = decode_group_snapshot(&bytes).expect("a started ledger decodes");
    assert_eq!(reread, started);
    let restored = install(reread);
    assert_eq!(
        restored.usage_ledger_snapshot(),
        machine.usage_ledger_snapshot()
    );
    assert_eq!(restored.stream_usage_changes(0, 100).changes, changes);
    assert_eq!(totals(&restored), after);
}

/// A started ledger whose parts do not sum to the bucket totals beside it,
/// or that misses a live stream, is refused before anything installs it.
#[test]
fn a_ledger_that_does_not_fit_its_snapshot_is_refused() {
    let decoded = decode_group_snapshot(SNAPSHOT_0_10_1).expect("decodes");
    let mut machine = install(decoded.clone());
    let history = machine.history_id();
    machine.apply(StreamCommand::AckStreamUsage {
        history,
        through_seq: 0,
    });
    let started = rebuild(&machine, &decoded);
    let UsageLedgerSnapshot::Started(ledger) = &started.usage_ledger else {
        panic!("the ledger started");
    };

    let mut inflated = ledger.clone();
    inflated.retired[0].usage.committed_records = inflated.retired[0]
        .usage
        .committed_records
        .saturating_add(1);
    let mut missing = ledger.clone();
    missing.streams.pop();
    for tampered in [inflated, missing] {
        let snapshot = GroupSnapshot {
            usage_ledger: UsageLedgerSnapshot::Started(tampered),
            ..started.clone()
        };
        let refused = decode_group_snapshot(&encode(snapshot)).expect_err("refused");
        assert!(
            matches!(&refused, ursula_runtime::SnapshotStoreError::Deserialize(message)
                if message.contains("usage ledger")),
            "{refused:?}"
        );
    }
}

#[cfg(not(madsim))]
mod cluster {
    use openraft::rt::WatchReceiver;
    use ursula_runtime::GroupEngine;
    use ursula_runtime::GroupEngineError;
    use ursula_runtime::GroupInfraError;
    use ursula_stream::StreamUsageChanges;

    use super::*;
    use crate::engine::RaftGroupEngine;
    use crate::tests::bsid;
    use crate::tests::build_three_node_cluster_sharing_peer_clusters;
    use crate::tests::create_stream_via_raft;
    use crate::tests::placement;
    use crate::tests::shutdown_all;
    use crate::tests::wait_history;

    async fn export(engine: &mut RaftGroupEngine) -> StreamUsageChanges {
        engine
            .stream_usage(0, 1_000, placement())
            .await
            .expect("export")
            .changes
    }

    /// Acknowledges through `engine`, retrying while the usage gate waits
    /// for the leader to hear every member.
    async fn ack(engine: &mut RaftGroupEngine, history: u64, through_seq: u64) -> UsageAck {
        let started = std::time::Instant::now();
        loop {
            match engine
                .ack_stream_usage(history, through_seq, placement())
                .await
            {
                Ok(response) => return response.ack,
                Err(GroupEngineError::Infra(GroupInfraError::Unsupported { .. }))
                    if started.elapsed() < Duration::from_secs(5) =>
                {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(error) => panic!("acknowledgement refused: {error:?}"),
            }
        }
    }

    async fn caught_up(engines: &[RaftGroupEngine], leader: usize) {
        let index = engines[leader].raft.metrics().borrow_watched().last_applied;
        for engine in engines {
            engine
                .raft
                .wait(Some(Duration::from_secs(5)))
                .applied_index_at_least(index.map(|log_id| log_id.index()), "caught up")
                .await
                .expect("replica catches up");
        }
    }

    fn index_of(node_id: u64) -> usize {
        usize::try_from(node_id.checked_sub(1).expect("node ids start at 1"))
            .expect("node id fits usize")
    }

    /// U-B: an ended stream waits for its acknowledgement through a leader
    /// change, the new leader folds it, and every replica holds the same
    /// ledger after the same entries.
    #[tokio::test]
    async fn an_acknowledgement_folds_ended_usage_across_a_leader_change() {
        let (_registry, mut engines, first_leader, _wal_roots) =
            build_three_node_cluster_sharing_peer_clusters("ursula-usage-ack-leader-change", None)
                .await;
        let first = index_of(first_leader);
        let history = wait_history(&engines[first]).await.id.get();

        assert!(matches!(
            ack(&mut engines[first], history, 0).await,
            UsageAck::Started { usage_seq: 0 }
        ));
        let kept = bsid("kept");
        let brief = bsid("brief");
        create_stream_via_raft(&engines[first], kept.clone()).await;
        create_stream_via_raft(&engines[first], brief.clone()).await;
        engines[first]
            .write(StreamCommand::DeleteStream {
                stream_id: brief.clone(),
                now_ms: 1,
            })
            .await
            .expect("delete");
        caught_up(&engines, first).await;
        let before = export(&mut engines[first]).await;
        let ended_seq = before
            .changes
            .iter()
            .find(|change| change.stream_id == brief)
            .filter(|change| change.end == Some(ursula_stream::StreamEnd::Deleted))
            .map(|change| change.usage.usage_seq)
            .expect("the deleted stream is exported as ended");
        for engine in &mut engines {
            assert_eq!(export(engine).await, before);
        }

        // Leadership moves to another member.
        let second_leader = (1..=3).find(|node| *node != first_leader).expect("a peer");
        let second = index_of(second_leader);
        engines[first]
            .raft
            .trigger()
            .transfer_leader(second_leader)
            .await
            .expect("transfer");
        engines[second]
            .raft
            .wait(Some(Duration::from_secs(10)))
            .metrics(
                |metrics| metrics.state == openraft::ServerState::Leader,
                "the new leader leads",
            )
            .await
            .expect("new leader");
        // The ended stream still waits on the new leader.
        assert_eq!(export(&mut engines[second]).await, before);

        assert_eq!(
            ack(&mut engines[second], history, ended_seq).await,
            UsageAck::Acknowledged {
                acked_through: ended_seq,
                folded: 1
            }
        );
        caught_up(&engines, second).await;
        let after = export(&mut engines[second]).await;
        assert!(after.changes.iter().all(|change| change.stream_id != brief));
        assert_eq!(after.progress, UsageLedgerProgress::Started {
            usage_seq: before.through,
            acked_through: ended_seq,
        });
        for engine in &mut engines {
            assert_eq!(export(engine).await, after);
            // The bucket keeps the folded usage.
            let usage = engine.bucket_usage(placement()).await.expect("usage");
            assert_eq!(usage.buckets.len(), 1);
            assert_eq!(usage.buckets[0].usage.committed_write_units, 2);
        }
        shutdown_all(&engines).await;
    }

    /// The usage gate: while a member has not advertised the ledger on its
    /// latest answer, the leader refuses an acknowledgement before it
    /// proposes it, and the log does not grow.
    #[tokio::test]
    async fn the_usage_gate_holds_while_a_member_lacks_the_ledger() {
        let policy = crate::InProcessRaftNetworkPolicy::default();
        let (_registry, mut engines, leader, _wal_roots) =
            build_three_node_cluster_sharing_peer_clusters(
                "ursula-usage-gate",
                Some(policy.clone()),
            )
            .await;
        let index = index_of(leader);
        let history = wait_history(&engines[index]).await.id.get();
        // Started once every member answered with the feature.
        ack(&mut engines[index], history, 0).await;

        // A member that answers as 0.10.1 does, without the feature, and
        // answers nothing after that.
        let lagging = (1..=3).find(|node| *node != leader).expect("a peer");
        for node in (1..=3).filter(|node| *node != lagging) {
            policy.partition_bidirectional(lagging, node);
        }
        let peers = engines[index]
            .peer_cluster
            .clone()
            .expect("the leader's peer cluster");
        peers.note_features(lagging, crate::incarnation::Stamp {
            node: Some(lagging),
            incarnation: None,
            features: crate::usage_gate::Features::default(),
        });
        let last_log = engines[index]
            .raft
            .metrics()
            .borrow_watched()
            .last_log_index;
        let refused = engines[index]
            .ack_stream_usage(history, 0, placement())
            .await
            .expect_err("refused");
        assert!(
            matches!(
                refused,
                GroupEngineError::Infra(GroupInfraError::Unsupported {
                    operation: ursula_runtime::EngineOperation::AckStreamUsage
                })
            ),
            "{refused:?}"
        );
        assert_eq!(
            engines[index]
                .raft
                .metrics()
                .borrow_watched()
                .last_log_index,
            last_log
        );
        // Ordinary writes pass: only the acknowledgement is gated.
        create_stream_via_raft(&engines[index], bsid("ungated")).await;
        shutdown_all(&engines).await;
    }
}
