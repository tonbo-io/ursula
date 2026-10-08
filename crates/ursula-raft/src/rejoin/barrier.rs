//! Follower-side fresh quorum barrier driver.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use openraft::rt::WatchReceiver;
use openraft::rt::WatchSender;

use super::GroupRejoin;
use super::attach::wait_recovery_change;
use super::log_index;
use crate::owner::OwnerRaftHandle;
use crate::types::UrsulaVote;

/// Opens a gated replica's recovery gate from fresh outbound leader proofs.
/// The supplied probe must confirm a new post-call ReadIndex with a quorum
/// and return that leader's committed vote and required local applied
/// index. A replica that for `stall_after` gets no barrier and applies
/// nothing reports its group stalled. Once the gate is open, here or through
/// an operator, the group's election policy is refreshed and the driver
/// returns. Transport-independent so simulation exercises the production
/// driver.
pub async fn run_rejoin_vote_barrier<P, F, E>(
    raft: OwnerRaftHandle,
    rejoin: Arc<GroupRejoin>,
    election: crate::ElectionPolicy,
    nodes: BTreeMap<u64, BasicNode>,
    probe: P,
    probe_timeout: Duration,
    interval: Duration,
    stall_after: Duration,
) where
    P: Fn(u64, String) -> F,
    F: Future<Output = Result<(UrsulaVote, u64), E>>,
    E: fmt::Debug,
{
    let raft_group_id = rejoin.raft_group_id;
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
                raft_group_id = raft_group_id.0,
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
                    .map(|(id, node)| (*id, node.clone())),
            );
            candidates.remove(&metrics.id);
            for (leader_id, node) in candidates {
                if !rejoin.needs_vote_floor() && metrics.current_leader != Some(leader_id) {
                    continue;
                }
                let outcome =
                    crate::rt::time::timeout(probe_timeout, probe(leader_id, node.addr)).await;
                match outcome {
                    Ok(Ok((leader, index))) => {
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
                        raft_group_id = raft_group_id.0,
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
