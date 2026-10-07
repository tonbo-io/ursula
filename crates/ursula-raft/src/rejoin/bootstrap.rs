//! Empty-group bootstrap decisions and driver.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use openraft::BasicNode;
use openraft::rt::WatchReceiver;
use openraft::rt::WatchSender;

use super::GroupRejoin;
use super::attach::wait_recovery_change;
use super::log_index;
use crate::registry::RaftGroupHandle;
use crate::types::UrsulaVote;
use crate::types::UrsulaVoteRequest;
use crate::types::UrsulaVoteResponse;

/// What one probed voter reported about a group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerGroupLog {
    /// No committed entry and no leader.
    Empty,
    /// The peer holds committed entries or follows a leader.
    Initialized,
}

impl PeerGroupLog {
    /// Classify a peer's answer to the bootstrap probe vote.
    pub fn from_vote_response(response: &UrsulaVoteResponse) -> Self {
        let has_entries = log_index(response.last_log_id.as_ref()).is_some_and(|index| index >= 1);
        if has_entries || response.vote.is_committed() {
            Self::Initialized
        } else {
            Self::Empty
        }
    }
}

/// What an initializer does with a group it holds nothing of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapDecision {
    /// Every voter is empty: a fresh group. Run `Initialize`.
    Initialize,
    /// Some voter holds the group: wait to be replicated to.
    Rejoin,
    /// Not every voter answered yet and none holds the group.
    Wait,
}

/// Decide from the other voters' answers (`None`: no answer yet).
pub fn bootstrap_decision<'a>(
    answers: impl IntoIterator<Item = &'a Option<PeerGroupLog>>,
) -> BootstrapDecision {
    let mut all_answered = true;
    for answer in answers {
        match answer {
            Some(PeerGroupLog::Initialized) => return BootstrapDecision::Rejoin,
            Some(PeerGroupLog::Empty) => {}
            None => all_answered = false,
        }
    }
    if all_answered {
        BootstrapDecision::Initialize
    } else {
        BootstrapDecision::Wait
    }
}

/// The probe vote: the lowest vote a node can send, with no log. A peer that
/// holds the group refuses it. A peer with no vote yet may grant it, which
/// only records a term-0 vote: harmless, since that never counts as
/// initialized and `Initialize` overwrites it.
pub fn bootstrap_probe_vote(node_id: u64) -> UrsulaVoteRequest {
    UrsulaVoteRequest::new(UrsulaVote::new(0, node_id), None)
}

/// How a group bootstrap ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupBootstrap {
    /// The group was already initialized when the loop looked.
    AlreadyInitialized,
    Initialized,
    Rejoined,
    /// The Raft stopped, or `Initialize` failed.
    Stopped,
}

/// Membership bootstrap of a group's initializer whose replica holds nothing
/// of the group. It runs `Initialize` only once every other configured voter
/// answered that it is empty too and no leader was seen. `probe` asks one
/// peer (`(id, address)`) with the bootstrap probe vote.
pub async fn run_group_bootstrap<P, F>(
    node_id: u64,
    raft: RaftGroupHandle,
    rejoin: Arc<GroupRejoin>,
    nodes: BTreeMap<u64, BasicNode>,
    probe: P,
    interval: Duration,
    warn_every: Duration,
) -> GroupBootstrap
where
    P: Fn(u64, String) -> F,
    F: Future<Output = Option<PeerGroupLog>>,
{
    let group = rejoin.raft_group_id().0;
    let peers = nodes
        .iter()
        .filter(|(peer_id, _)| **peer_id != node_id)
        .map(|(peer_id, node)| (*peer_id, node.addr.clone()))
        .collect::<Vec<_>>();
    let mut last_warning = crate::rt::time::Instant::now();
    let mut observed = raft.metrics();
    let mut changes = rejoin.changes.subscribe();
    loop {
        match raft.is_initialized().await {
            Ok(true) => return GroupBootstrap::AlreadyInitialized,
            Ok(false) => {}
            Err(err) => {
                tracing::error!(
                    "raft bootstrap: node {node_id} group {group} failed to check initialization: {err}"
                );
                return GroupBootstrap::Stopped;
            }
        }
        if rejoin.holds_group_history() {
            return GroupBootstrap::AlreadyInitialized;
        }
        let leader_seen = raft.metrics().borrow_watched().current_leader.is_some();
        let answers = if leader_seen {
            Vec::new()
        } else {
            futures_util::future::join_all(
                peers
                    .iter()
                    .map(|(peer_id, address)| probe(*peer_id, address.clone())),
            )
            .await
        };
        let decision = if leader_seen {
            BootstrapDecision::Rejoin
        } else {
            bootstrap_decision(&answers)
        };
        match decision {
            BootstrapDecision::Initialize => {
                if let Err(err) = rejoin.allow_fresh_bootstrap().await {
                    tracing::error!(
                        "raft bootstrap: node {node_id} group {group} could not open its recovery gate: {err}"
                    );
                    return GroupBootstrap::Stopped;
                }
                if let Err(err) = raft.initialize(nodes).await {
                    tracing::error!(
                        "raft bootstrap: node {node_id} group {group} failed to initialize membership: {err}"
                    );
                    return GroupBootstrap::Stopped;
                }
                return GroupBootstrap::Initialized;
            }
            BootstrapDecision::Rejoin => {
                tracing::info!(
                    "raft bootstrap: node {node_id} group {group} holds nothing of a group another voter holds; not initializing, waiting to be replicated to"
                );
                return GroupBootstrap::Rejoined;
            }
            BootstrapDecision::Wait => {
                let now = crate::rt::time::Instant::now();
                if now.saturating_duration_since(last_warning) >= warn_every {
                    let answered = answers.iter().filter(|answer| answer.is_some()).count();
                    tracing::warn!(
                        "raft bootstrap: node {node_id} group {group} waits for every voter before initializing; {answered}/{} answered",
                        peers.len()
                    );
                    last_warning = now;
                }
            }
        }
        if !wait_recovery_change(&mut observed, &mut changes, interval).await {
            return GroupBootstrap::Stopped;
        }
    }
}
