//! Empty-group bootstrap decisions and driver.

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use openraft::Raft;
use openraft::rt::WatchReceiver;
use openraft::rt::WatchSender;
use openraft::storage::RaftStateMachine;
use ursula_proto::placement::GenesisClaim;

use super::GroupRejoin;
use super::attach::wait_recovery_change;
use super::log_index;
use crate::types::UrsulaRaftTypeConfig;
use crate::types::UrsulaVote;
use crate::types::UrsulaVoteRequest;
use crate::types::UrsulaVoteResponse;

/// What one probed voter reported about a group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerGroupLog {
    /// No history, and the minimum genesis vote floor is durable.
    Empty,
    /// No history, but genesis admission has not been persisted yet.
    Unprepared,
    /// The peer holds committed entries or follows a leader.
    Initialized,
    /// The peer would start the meta group with other values, `claim`: the
    /// meta group must not form until every node claims the same. Not an
    /// answer for the bootstrap decision, which waits, and not empty for
    /// the genesis vote floor.
    GenesisMismatch { claim: GenesisClaim },
}

impl PeerGroupLog {
    /// Classify a peer's answer to the bootstrap probe vote.
    pub fn from_vote_response(response: &UrsulaVoteResponse) -> Self {
        let has_entries = log_index(response.last_log_id.as_ref()).is_some_and(|index| index >= 1);
        if has_entries || response.vote.is_committed() || response.vote.leader_id.term > 0 {
            Self::Initialized
        } else if response.vote_granted {
            Self::Empty
        } else {
            Self::Unprepared
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
            None | Some(PeerGroupLog::Unprepared | PeerGroupLog::GenesisMismatch { .. }) => {
                all_answered = false;
            }
        }
    }
    if all_answered {
        BootstrapDecision::Initialize
    } else {
        BootstrapDecision::Wait
    }
}

/// The reserved minimum vote is a read-only probe on a recovery-gated peer.
/// Its grant bit reports durable genesis readiness without changing the vote.
pub fn bootstrap_probe_vote() -> UrsulaVoteRequest {
    UrsulaVoteRequest::new(UrsulaVote::new(0, 0), None)
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
/// of the group. It runs `Initialize` with `members` only once every other
/// node of `probed` answered that it is empty too and no leader was seen.
/// `probe` asks one peer, by node id, with the bootstrap probe. A data
/// group probes and initializes its configured voters. Any state machine's
/// replica runs it.
pub async fn run_group_bootstrap<SM, P, F>(
    node_id: u64,
    raft: Raft<UrsulaRaftTypeConfig, SM>,
    rejoin: Arc<GroupRejoin>,
    probed: BTreeSet<u64>,
    members: BTreeSet<u64>,
    probe: P,
    interval: Duration,
    warn_every: Duration,
) -> GroupBootstrap
where
    SM: RaftStateMachine<UrsulaRaftTypeConfig>,
    P: Fn(u64) -> F,
    F: Future<Output = Option<PeerGroupLog>>,
{
    let group = rejoin.raft_group_id().0;
    let peers = probed
        .iter()
        .copied()
        .filter(|peer_id| *peer_id != node_id)
        .collect::<Vec<_>>();
    let mut last_probe = None;
    let mut last_warning = crate::rt::time::Instant::now();
    let mut observed = raft.server_metrics();
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
        if !leader_seen && let Some(at) = last_probe {
            let remaining = interval
                .saturating_sub(crate::rt::time::Instant::now().saturating_duration_since(at));
            if !remaining.is_zero() {
                if !wait_recovery_change(&mut observed, &mut changes, remaining).await {
                    return GroupBootstrap::Stopped;
                }
                continue;
            }
        }
        last_probe = Some(crate::rt::time::Instant::now());
        let answers = if leader_seen {
            Vec::new()
        } else {
            futures_util::future::join_all(peers.iter().map(|peer_id| probe(*peer_id))).await
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
                if let Err(err) = raft.initialize(members).await {
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
