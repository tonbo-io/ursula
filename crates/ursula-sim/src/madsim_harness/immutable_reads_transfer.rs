//! The transfer step of `marked_chunks_never_contradict` (#581 section 5.3):
//! a leader that hands its leadership to a follower and hears nothing of
//! the election that follows, read within its lease.
//!
//! A leadership-transfer election skips the voters' leader leases, so the
//! successor wins while the old leader is still inside its own. The old
//! leader's peers cannot reach it, and it sends nothing while it transfers,
//! so it stays in OpenRaft's leader state, and a quorum acknowledged it
//! within its lease: it leads by the leader predicate, lease and fence
//! included. The successor takes writes and marks chunks the old leader has
//! not applied. Only the old leader's read barrier keeps it from answering
//! them short: its ReadIndex round is refused while it transfers. This is
//! the case the barrier alone defends (`Defence::ImmutableLeaderBarrier`),
//! which the accepted-loss step does not show: there the cut-off leader's
//! lease ends first, and it then no longer leads.

use std::time::Duration;

use ursula_config::WalFsync;

use super::SimEvent;
use super::SimTrace;
use super::SplitMix64;
use super::accepted_loss::try_append;
use super::immutable_reads::GRID;
use super::immutable_reads::Observer;
use super::immutable_reads_loss::GROUP;
use super::immutable_reads_loss::WatchedReads;
use super::immutable_reads_loss::current_token;
use super::immutable_reads_loss::judged_read;
use super::immutable_reads_loss::sweep;
use super::journal_cluster::JournalCluster;
use super::journal_cluster::NODES;
use super::journal_cluster::metrics;
use super::journal_cluster::wait_leader;

/// The read through the old leader that only its barrier decides.
pub(super) const TRANSFER_READ: &str = "read_through_the_leader_in_transfer";
/// The step's name in [`SimEvent::DeposedLeaderReadsJudged`].
pub(super) const TRANSFER_STEP: &str = "leader_in_transfer";

/// Runs the step and returns what the old leader's reads decided.
pub(super) async fn transfer_phase(
    seed: u64,
    observer: &mut Observer,
    trace: &mut SimTrace,
) -> WatchedReads {
    let context = format!("seed {seed}: leadership transfer");
    let mut rng = SplitMix64::new(seed ^ 0x7472_616e_7366_6572);
    let mut cluster = JournalCluster::start_with_fsync(
        &format!("marked-chunks-transfer-{seed}"),
        WalFsync::Never,
    )
    .await;
    let before = 6_usize.saturating_add(usize::try_from(rng.next_bounded(8)).expect("small"));
    cluster.append(GROUP, before).await;
    cluster.verify_group_reads(GROUP).await;
    let token = current_token(&mut cluster, &context).await;
    sweep(
        &mut cluster,
        observer,
        trace,
        &mut rng,
        "before_transfer",
        token,
        12,
        None,
    )
    .await;

    let old = wait_leader(&cluster, GROUP, &context).await;
    let others: Vec<u64> = NODES.into_iter().filter(|node| *node != old).collect();
    let pick = usize::try_from(rng.next_bounded(2)).expect("an index");
    let successor = others.get(pick).copied().expect("a follower");
    let third = others
        .iter()
        .copied()
        .find(|node| *node != successor)
        .expect("another follower");
    let old_tail = cluster.acknowledged.get(&GROUP).map_or(0, |bytes| {
        u64::try_from(bytes.len()).expect("offset fits u64")
    });

    // Only the old leader's RPCs to its successor pass: its transfer
    // reaches the successor, and nothing tells the old leader of the
    // election that follows.
    cluster.policy.partition_one_way(successor, old);
    cluster.policy.partition_one_way(third, old);
    cluster.policy.partition_one_way(old, third);
    trace.push(SimEvent::FaultApplied {
        phase: "leader_hears_no_election".to_owned(),
    });
    cluster.engines[&(GROUP, old)]
        .raft_handle()
        .trigger()
        .transfer_leader(successor)
        .await
        .expect("transfer the leadership");
    let deadline = madsim::time::Instant::now() + Duration::from_secs(1);
    while metrics(&cluster, GROUP, successor).state != openraft::ServerState::Leader {
        assert!(
            madsim::time::Instant::now() < deadline,
            "{context}: node {successor} never took the leadership"
        );
        madsim::time::sleep(Duration::from_millis(1)).await;
    }
    trace.push(SimEvent::LeaderElected {
        leader_id: successor,
    });

    // The successor takes a write the old leader never sees.
    let payload: Vec<u8> = (0..GRID.saturating_mul(3))
        .map(|_| u8::try_from(rng.next_bounded(256)).expect("a byte"))
        .collect();
    assert!(
        try_append(&mut cluster, GROUP, successor, &payload).await,
        "{context}: node {successor} took no write"
    );
    cluster
        .acknowledged
        .entry(GROUP)
        .or_default()
        .extend_from_slice(&payload);
    let tail = old_tail.saturating_add(u64::try_from(payload.len()).expect("a length"));

    // Every chunk from the one that holds the old leader's tail, and one
    // more offset inside the write, read through the successor, which marks
    // it, and then through the old leader.
    let mut offsets: Vec<u64> = (old_tail.saturating_sub(old_tail % GRID)..tail)
        .step_by(usize::try_from(GRID).expect("the grid fits usize"))
        .collect();
    offsets.push(old_tail.saturating_add(rng.next_bounded(tail.saturating_sub(old_tail))));
    let mut deposed = WatchedReads::default();
    for offset in offsets {
        judged_read(
            &mut cluster,
            observer,
            trace,
            "read_through_the_successor",
            (token, offset),
            token,
            successor,
            None,
        )
        .await;
        judged_read(
            &mut cluster,
            observer,
            trace,
            TRANSFER_READ,
            (token, offset),
            token,
            old,
            Some((old, &mut deposed)),
        )
        .await;
    }
    deposed.record(trace, TRANSFER_STEP, old);

    // The network heals, and the old leader follows its successor.
    cluster.policy.heal_one_way(successor, old);
    cluster.policy.heal_one_way(third, old);
    cluster.policy.heal_one_way(old, third);
    trace.push(SimEvent::FaultApplied {
        phase: "transfer_healed".to_owned(),
    });
    let deadline = madsim::time::Instant::now() + Duration::from_secs(5);
    loop {
        let seen = metrics(&cluster, GROUP, old);
        if seen.state != openraft::ServerState::Leader && seen.current_leader == Some(successor) {
            break;
        }
        assert!(
            madsim::time::Instant::now() < deadline,
            "{context}: node {old} never followed node {successor}"
        );
        madsim::time::sleep(Duration::from_millis(5)).await;
    }
    sweep(
        &mut cluster,
        observer,
        trace,
        &mut rng,
        "after_transfer_healed",
        token,
        12,
        None,
    )
    .await;
    deposed
}
