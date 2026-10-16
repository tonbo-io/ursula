//! The accepted-loss step of `marked_chunks_never_contradict` (#581 section
//! 10, DST items 2 and 4): chunk reads through every replica of a
//! journal-backed three-node group under `raft.wal.fsync = never`, before,
//! during and after a recovery plan.
//!
//! The group's leader is cut off with a write that never commits; the others
//! elect a leader that acknowledges writes, and both lose power and every
//! unsynced page. The group stalls until the operator plans: the plan's
//! history event gives the stream a new token. Until it applies, every
//! replica serves only what it holds of the old history. The observer of the
//! scenario judges every answer, so a marked chunk of the old token never
//! meets other bytes under that token, and after the plan the old URLs
//! answer for another token.

use std::time::Duration;

use ursula_config::WalFsync;
use ursula_runtime::FollowerRead;
use ursula_runtime::GroupEngine;
use ursula_runtime::GroupReadRoute;
use ursula_runtime::ReadStreamRequest;

use super::SimEvent;
use super::SimTrace;
use super::SplitMix64;
use super::accepted_loss::try_append;
use super::immutable_reads::Observer;
use super::immutable_reads::Read;
use super::immutable_reads::chunk_request;
use super::journal_cluster::JournalCluster;
use super::journal_cluster::NODES;
use super::journal_cluster::group_placement;
use super::journal_cluster::group_stream;
use super::journal_cluster::lead_every_group;
use super::journal_cluster::leader_by_state;
use super::journal_cluster::metrics;
use super::journal_cluster::recover_group;
use super::journal_cluster::wait_leader;
use super::journal_cluster::wait_plan_satisfied;
use super::recovery_wiring::RECOVERY_STALL_AFTER;

/// The group the step reads.
const GROUP: u32 = 0;

/// `request` routed by `node_id`'s own replica of the group. A read it
/// forwards to the leader is not answered here: the leader's own replica
/// answers it when the step reads through the leader.
async fn read_through(
    cluster: &mut JournalCluster,
    node_id: u64,
    request: ReadStreamRequest,
) -> Read {
    let Some(engine) = cluster.engines.get_mut(&(GROUP, node_id)) else {
        return Read::Refused;
    };
    match engine
        .route_read_stream(request, group_placement(GROUP))
        .await
    {
        Ok(GroupReadRoute::Local(parts)) => match parts.into_response().await {
            Ok(response) => Read::Answered(response),
            Err(_unreadable) => Read::Refused,
        },
        Ok(GroupReadRoute::Leader(_forwarded)) => Read::Refused,
        Err(error) => Read::failed(error.code()),
    }
}

/// The stream's current token, from a read through the group's leader.
async fn current_token(cluster: &mut JournalCluster, context: &str) -> u64 {
    let deadline = madsim::time::Instant::now() + Duration::from_secs(10);
    loop {
        let leader = wait_leader(cluster, GROUP, context).await;
        let mut request = chunk_request(&group_stream(GROUP), 0);
        request.follower = FollowerRead::Prefix;
        request.max_len = 1;
        if let Read::Answered(response) = read_through(cluster, leader, request).await {
            return response.incarnation;
        }
        assert!(
            madsim::time::Instant::now() < deadline,
            "{context}: the leader never answered a read"
        );
        madsim::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Chunk reads at `reads` random offsets up to `tail`, through every
/// replica, and of every URL the observer marked under `current`.
async fn sweep(
    cluster: &mut JournalCluster,
    observer: &mut Observer,
    trace: &mut SimTrace,
    rng: &mut SplitMix64,
    phase: &str,
    current: u64,
    reads: u64,
) {
    let tail = cluster.acknowledged.get(&GROUP).map_or(0, |bytes| {
        u64::try_from(bytes.len()).expect("offset fits u64")
    });
    let mut urls: Vec<(u64, u64)> = (0..reads)
        .map(|_| (current, rng.next_bounded(tail.saturating_add(8))))
        .collect();
    urls.extend(observer.marked());
    for (token, offset) in urls {
        for node_id in NODES {
            let read = read_through(
                cluster,
                node_id,
                chunk_request(&group_stream(GROUP), offset),
            )
            .await;
            observer.observe(trace, phase, (token, offset), current, read);
        }
    }
}

/// Runs the step. Returns how many later answers it checked against a
/// marked one.
pub(super) async fn accepted_loss_phase(
    seed: u64,
    observer: &mut Observer,
    trace: &mut SimTrace,
) -> u64 {
    let checked_before = observer.checked;
    let context = format!("seed {seed}: accepted loss");
    let mut rng = SplitMix64::new(seed ^ 0x6c6f_7373_2121_2121);
    let mut cluster =
        JournalCluster::start_with_fsync(&format!("marked-chunks-loss-{seed}"), WalFsync::Never)
            .await;
    let before = 6_usize.saturating_add(usize::try_from(rng.next_bounded(8)).expect("small"));
    cluster.append(GROUP, before).await;
    cluster.verify_group_reads(GROUP).await;
    let old = current_token(&mut cluster, &context).await;
    sweep(
        &mut cluster,
        observer,
        trace,
        &mut rng,
        "before_loss",
        old,
        12,
    )
    .await;

    // The leader is cut off with a write that never commits.
    let cut_off = NODES
        .get(usize::try_from(seed % 3).expect("an index"))
        .copied()
        .unwrap_or(1);
    lead_every_group(&cluster, cut_off, &context).await;
    for node in NODES.into_iter().filter(|node| *node != cut_off) {
        cluster.policy.partition_bidirectional(cut_off, node);
    }
    trace.push(SimEvent::FaultApplied {
        phase: "leader_cut_off_before_loss".to_owned(),
    });
    let _uncommitted = try_append(&mut cluster, GROUP, cut_off, b"stale-write;").await;

    // The others elect a leader, which acknowledges writes the loss takes.
    let others: Vec<u64> = NODES.into_iter().filter(|node| *node != cut_off).collect();
    let deadline = madsim::time::Instant::now() + Duration::from_secs(10);
    let new_leader = loop {
        if let Some(leader) = others
            .iter()
            .copied()
            .find(|node| metrics(&cluster, GROUP, *node).state == openraft::ServerState::Leader)
        {
            break leader;
        }
        assert!(
            madsim::time::Instant::now() < deadline,
            "{context}: the others elected no leader"
        );
        madsim::time::sleep(Duration::from_millis(25)).await;
    };
    for index in 0..3 {
        let payload = format!("lost-{index};").into_bytes();
        if try_append(&mut cluster, GROUP, new_leader, &payload).await {
            cluster
                .acknowledged
                .entry(GROUP)
                .or_default()
                .extend_from_slice(&payload);
        }
    }
    sweep(
        &mut cluster,
        observer,
        trace,
        &mut rng,
        "before_power_loss",
        old,
        12,
    )
    .await;

    // Both lose power and every page written since their last fsync.
    for node in &others {
        cluster.stop_node(*node).await;
    }
    for node in &others {
        cluster.wals[node].power_loss_losing_unsynced().await;
    }
    for node in &others {
        cluster.start_node(*node).await;
    }
    for node in &others {
        cluster.policy.heal_bidirectional(cut_off, *node);
    }
    trace.push(SimEvent::FaultApplied {
        phase: "quorum_lost_power".to_owned(),
    });
    madsim::time::sleep(RECOVERY_STALL_AFTER.saturating_mul(2)).await;
    // Until a plan, every replica serves only what it holds of the old
    // history.
    sweep(
        &mut cluster,
        observer,
        trace,
        &mut rng,
        "stalled_after_loss",
        old,
        12,
    )
    .await;

    if leader_by_state(&cluster, GROUP).is_none() {
        let plan = recover_group(&mut cluster, GROUP, &NODES, &context).await;
        trace.push(SimEvent::FaultApplied {
            phase: "recovery_planned".to_owned(),
        });
        wait_leader(&cluster, GROUP, &context).await;
        wait_plan_satisfied(&cluster, GROUP, &NODES, plan.point, &context).await;
    }
    let new = current_token(&mut cluster, &context).await;
    // The old URLs answer for another token; new writes are judged under
    // the new one.
    sweep(
        &mut cluster,
        observer,
        trace,
        &mut rng,
        "after_plan",
        new,
        12,
    )
    .await;
    cluster.append(GROUP, 4).await;
    sweep(
        &mut cluster,
        observer,
        trace,
        &mut rng,
        "after_plan_writes",
        new,
        12,
    )
    .await;
    observer.checked.saturating_sub(checked_before)
}
