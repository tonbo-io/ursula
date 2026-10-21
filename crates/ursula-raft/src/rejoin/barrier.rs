//! Follower-side fresh quorum barrier driver.

use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use openraft::rt::WatchReceiver;
use openraft::rt::WatchSender;
use openraft::storage::RaftStateMachine;

use super::GroupRejoin;
use super::attach::wait_recovery_change;
use super::log_index;
use crate::owner::OwnerRaftHandle;
use crate::target::WireTarget;
use crate::types::SnapshotData;
use crate::types::UrsulaRaftTypeConfig;

/// Opens a gated replica's recovery gate from fresh outbound leader proofs.
/// It asks the group's configured voters (`nodes`) and the members of its
/// effective membership, by node id. The supplied probe must confirm a new
/// post-call ReadIndex with a quorum and return that leader's committed vote
/// and required local applied index ([`crate::BarrierAnswer`]). The driver
/// does not read the answer's memberships. A replica that for `stall_after` gets no barrier and applies
/// nothing reports its group stalled. Once the gate is open, here or through
/// an operator, the group's election policy is refreshed and the driver
/// returns. Transport-independent so simulation exercises the production
/// driver, and generic over the state machine so every plane's replica
/// runs it.
pub async fn run_rejoin_vote_barrier<SM, P, F, E>(
    raft: OwnerRaftHandle<SM>,
    rejoin: Arc<GroupRejoin>,
    election: crate::ElectionPolicy,
    nodes: BTreeSet<u64>,
    probe: P,
    probe_timeout: Duration,
    interval: Duration,
    stall_after: Duration,
) where
    SM: RaftStateMachine<UrsulaRaftTypeConfig, SnapshotData = SnapshotData>,
    P: Fn(u64) -> F,
    F: Future<Output = Result<crate::BarrierAnswer, E>>,
    E: fmt::Debug,
{
    let group = rejoin.group;
    let mut last_barrier_leader = None;
    let mut last_probe = None;
    let mut last_applied = None;
    let mut last_progress = crate::rt::time::Instant::now();
    let mut observed = raft.metrics();
    let mut changes = rejoin.changes.subscribe();
    loop {
        match rejoin.try_open().await {
            Ok(true) => break,
            Ok(false) => {}
            Err(err) => tracing::warn!(
                plane = group.plane().as_str_name(),
                raft_group_id = group.wire_group_id(),
                "recovery gate: could not record the open gate, retrying: {err}"
            ),
        }
        let metrics = raft.metrics().borrow_watched().clone();
        if metrics.running_state.is_err() {
            return;
        }
        let now = crate::rt::time::Instant::now();
        let applied = log_index(metrics.last_applied.as_ref());
        if applied > last_applied {
            last_applied = applied;
            last_progress = now;
        }
        if now.saturating_duration_since(last_progress) >= stall_after && rejoin.stall() {
            // The leader whose barrier led nowhere may be asked again.
            last_barrier_leader = None;
        }
        if last_barrier_leader != Some(metrics.vote)
            && last_probe.is_none_or(|(vote, at): (_, crate::rt::time::Instant)| {
                vote != metrics.vote || at.elapsed() >= interval
            })
        {
            last_probe = Some((metrics.vote, crate::rt::time::Instant::now()));
            let mut candidates = nodes.clone();
            candidates.extend(
                metrics
                    .membership_config
                    .membership()
                    .nodes()
                    .map(|(id, _)| *id),
            );
            candidates.remove(&metrics.id);
            for leader_id in candidates {
                if !rejoin.needs_vote_floor() && metrics.current_leader != Some(leader_id) {
                    continue;
                }
                let outcome = crate::rt::time::timeout(probe_timeout, probe(leader_id)).await;
                match outcome {
                    Ok(Ok(crate::BarrierAnswer {
                        vote: leader,
                        index,
                        memberships: _,
                    })) => {
                        if rejoin.needs_vote_floor() {
                            // This replica has refused every replication ACK. Therefore this fresh
                            // ReadIndex used a current quorum excluding it; a stale leader cannot
                            // manufacture a proof by counting the replica whose vote was lost.
                            let last_log_id = rejoin.last_log_id();
                            let result = raft
                                .call(move |raft| async move {
                                    raft.vote(crate::types::UrsulaVoteRequest::new(
                                        leader,
                                        last_log_id,
                                    ))
                                    .await
                                })
                                .await;
                            match result {
                                Ok(Ok(response)) if response.vote >= leader => {
                                    if let Err(error) =
                                        rejoin.establish_vote_floor(response.vote).await
                                    {
                                        tracing::warn!(%error, "could not persist recovery vote floor");
                                        continue;
                                    }
                                }
                                other => {
                                    tracing::debug!(?other, "could not adopt proven recovery vote");
                                    continue;
                                }
                            }
                        }
                        rejoin.confirm_barrier(leader, index);
                        if !rejoin.replication_allowed(leader) {
                            continue;
                        }
                        last_barrier_leader = Some(leader);
                        last_progress = crate::rt::time::Instant::now();
                        break;
                    }
                    other => tracing::debug!(
                        plane = group.plane().as_str_name(),
                        raft_group_id = group.wire_group_id(),
                        ?other,
                        "recovery barrier probe failed"
                    ),
                }
            }
        }
        if !wait_recovery_change(&mut observed, &mut changes, interval).await {
            return;
        }
    }
    // The gate never closes again in this run, so this refresh, after the
    // gate opened, is the one that lets the group campaign.
    election.refresh(&raft, Some(rejoin));
}
