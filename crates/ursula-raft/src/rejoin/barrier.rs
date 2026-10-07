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
        if let Some(leader_id) = metrics.current_leader
            && last_barrier_leader != Some(metrics.vote)
            && last_probe.is_none_or(|(vote, at): (_, crate::rt::time::Instant)| {
                vote != metrics.vote || at.elapsed() >= interval
            })
            && let Some(node) = nodes.get(&leader_id)
        {
            // A failed probe can itself publish metrics. Wake on those changes
            // to observe progress, but do not let them create a retry loop.
            last_probe = Some((metrics.vote, crate::rt::time::Instant::now()));
            let outcome =
                crate::rt::time::timeout(probe_timeout, probe(leader_id, node.addr.clone())).await;
            match outcome {
                Ok(Ok((leader, index))) => {
                    // A proof can arrive before the independent vote-floor task.
                    // Do not suppress future probes until the gate accepted it.
                    if !rejoin.confirm_barrier(leader, index) {
                        continue;
                    }
                    last_barrier_leader = Some(leader);
                    last_progress = crate::rt::time::Instant::now();
                    tracing::info!(
                        node_id = metrics.id,
                        raft_group_id = raft_group_id.0,
                        barrier_index = index,
                        "recovery gate: a fresh leader barrier confirmed; catching up"
                    );
                    continue;
                }
                other => tracing::debug!(
                    raft_group_id = raft_group_id.0,
                    ?other,
                    "recovery barrier probe failed"
                ),
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
