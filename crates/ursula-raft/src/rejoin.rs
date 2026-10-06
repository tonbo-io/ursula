//! Memory-WAL rejoin: a replica that restarted with an empty Raft log
//! heals itself, and never votes before it holds the group's log again.
//!
//! A memory-WAL replica loses its Raft log when its process restarts. Three
//! server-side pieces protect acknowledged writes. Recovery uses a fresh
//! outbound ReadIndex proof; replicated commands and persisted formats are
//! unchanged. During a rolling upgrade, the probe can use the 0.6.2 read and
//! vote RPCs until every peer supports the explicit recovery-barrier RPC.
//!
//! - **Bootstrap decision** ([`bootstrap_decision`]): a group's initializer
//!   runs `Initialize` only when every configured voter answers a probe
//!   `Vote` with an empty log and no leader. The probe carries the lowest
//!   possible vote (term 0, the prober's id) and no log. A peer that holds
//!   the group refuses it; a peer with no vote yet may grant it, which only
//!   records a term-0 vote that `Initialize` overwrites and that never
//!   counts as initialized. One peer that holds committed entries or
//!   follows a leader means the group exists: the replica waits to be
//!   replicated to instead of founding a second group.
//! - **Vote gate** ([`VoteGate`]): from start until it holds the log that a
//!   leader confirmed in a fresh post-start ReadIndex round, the replica
//!   refuses every candidate whose log is non-empty, and every candidate
//!   once it has seen that the group has committed entries. A fresh group's
//!   first election (candidates whose log is only the membership entry at
//!   index 0) goes through untouched.
//! - **Self-heal** ([`run_rejoin_heal`]): a leader whose follower answers
//!   `Conflict` at or below the index that follower had already matched in
//!   this leadership knows the follower lost its log. OpenRaft never rewinds
//!   that progress, so the network layer hands OpenRaft an error instead of
//!   the conflict, and the leader rebuilds the follower the way `ursulactl
//!   repair-restarted-voter` does: remove the voter, add it back as a
//!   learner, wait for catch-up, promote. Every step is read off the current
//!   membership, so a leader change, a second restart or `ursulactl` doing
//!   the same repair at the same time all converge.
//!
//! A group whose majority restarted empty has no quorum of healthy voters:
//! the leader never removes a voter then, and the empty replicas never vote
//! for a candidate. The group stops accepting writes until an operator picks
//! the survivor ([`GroupRejoin::adopt_survivor`]). A group whose every voter
//! restarted empty is stopped by its "initialized" marker in object storage
//! instead (`crate::restart_guard`).

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::PoisonError;
use std::time::Duration;

use openraft::BasicNode;
use openraft::ChangeMembers;
use openraft::RaftMetrics;
use openraft::ServerState;
use openraft::alias::LogIdOf;
use openraft::rt::WatchReceiver;
use openraft::type_config::alias::WatchReceiverOf;
use openraft::vote::RaftLeaderId;
use ursula_shard::RaftGroupId;

use crate::registry::RaftGroupHandle;
use crate::registry::RaftGroupHandleRegistry;
use crate::restart_guard::InitMarkerStore;
use crate::restart_guard::RestartGuard;
use crate::types::UrsulaAppendEntriesRequest;
use crate::types::UrsulaAppendEntriesResponse;
use crate::types::UrsulaRaftTypeConfig;
use crate::types::UrsulaVote;
use crate::types::UrsulaVoteRequest;
use crate::types::UrsulaVoteResponse;

type MetricsReceiver = WatchReceiverOf<UrsulaRaftTypeConfig, RaftMetrics<UrsulaRaftTypeConfig>>;

/// How often the leader-side heal driver re-reads its group.
pub const REJOIN_HEAL_INTERVAL: Duration = Duration::from_millis(500);

/// How long one membership step of the heal driver may take.
const REJOIN_HEAL_STEP_TIMEOUT: Duration = Duration::from_secs(10);

fn log_index(log_id: Option<&LogIdOf<UrsulaRaftTypeConfig>>) -> Option<u64> {
    log_id.map(|log_id| log_id.index())
}

fn local_replica(metrics: &RaftMetrics<UrsulaRaftTypeConfig>) -> LocalReplica {
    LocalReplica {
        last_applied: log_index(metrics.last_applied.as_ref()),
    }
}

/// What this replica knows about itself when it screens a vote.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct LocalReplica {
    pub last_applied: Option<u64>,
}

/// The vote gate's answer for one vote request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VoteScreen {
    /// Hand the request to OpenRaft.
    Pass,
    /// Answer it with a non-granting response, without touching OpenRaft.
    Refuse,
}

/// The catch-up point from a fresh outbound quorum-confirmed probe. The legacy
/// RPC bridge uses the leader's last log as a conservative bound.
#[derive(Debug, Clone)]
struct CatchUpTarget {
    leader: UrsulaVote,
    commit_index: u64,
}

/// Per-group vote gate of a memory-WAL replica (pure state; see the module
/// docs).
#[derive(Debug, Default)]
pub(crate) struct VoteGate {
    open: bool,
    /// The group has committed entries somewhere: a leader reported a
    /// commit index of 1 or more, or a candidate's log reached index 1.
    initialized_seen: bool,
    catch_up: Option<CatchUpTarget>,
    /// Operator recovery: the one candidate this replica may vote for while
    /// it is still behind.
    released_for: Option<u64>,
}

impl VoteGate {
    pub(crate) fn is_open(&self) -> bool {
        self.open
    }

    /// Record an inbound AppendEntries from `leader`.
    pub(crate) fn observe_append(&mut self, _leader: &UrsulaVote, leader_commit: Option<u64>) {
        if self.open {
            return;
        }
        if leader_commit.is_some_and(|index| index >= 1) {
            self.initialized_seen = true;
        }
    }

    /// Only this process's outbound recovery probe may establish a catch-up
    /// target. Inbound replication may have been delayed across the restart.
    pub(crate) fn confirm_barrier(&mut self, leader: UrsulaVote, commit_index: u64) {
        if self.open || !leader.is_committed() {
            return;
        }
        self.initialized_seen |= commit_index >= 1;
        let newer_leader = match &self.catch_up {
            None => true,
            Some(target) => leader > target.leader,
        };
        if newer_leader {
            self.catch_up = Some(CatchUpTarget {
                leader,
                commit_index,
            });
        } else if let Some(target) = &mut self.catch_up
            && target.leader == leader
        {
            target.commit_index = target.commit_index.max(commit_index);
        }
    }

    fn allow_fresh_bootstrap(&mut self) {
        self.open = true;
    }

    /// Open the gate if the replica has caught up.
    pub(crate) fn refresh(&mut self, local: LocalReplica) {
        if !self.open && self.caught_up(local) {
            self.open = true;
        }
    }

    fn caught_up(&self, local: LocalReplica) -> bool {
        self.catch_up.as_ref().is_some_and(|target| {
            local
                .last_applied
                .is_some_and(|applied| applied >= target.commit_index)
        })
    }

    /// Decide one vote request from `candidate`, whose last log index is
    /// `candidate_last_log_index`.
    pub(crate) fn screen(
        &mut self,
        candidate: u64,
        candidate_last_log_index: Option<u64>,
        local: LocalReplica,
    ) -> VoteScreen {
        self.refresh(local);
        if self.open {
            return VoteScreen::Pass;
        }
        if self.released_for == Some(candidate) {
            return VoteScreen::Pass;
        }
        if candidate_last_log_index.is_some_and(|index| index >= 1) {
            self.initialized_seen = true;
            return VoteScreen::Refuse;
        }
        if self.initialized_seen {
            return VoteScreen::Refuse;
        }
        VoteScreen::Pass
    }

    fn release_for(&mut self, candidate: u64) {
        self.released_for = Some(candidate);
    }
}

/// Followers that answered a conflict below what they had matched, per
/// leadership.
#[derive(Debug, Default)]
struct RevertedFollowers {
    leader: Option<UrsulaVote>,
    targets: BTreeSet<u64>,
    /// Operator-authorized rewinds, until Raft replication metrics show the
    /// old matched point was reset. ReadIndex also sends Append RPCs; its
    /// Conflict confirms leadership but does not reset replication progress.
    /// Consuming permission on that response would strand operator recovery.
    allowed_reverts: BTreeMap<u64, u64>,
}

impl RevertedFollowers {
    fn operator_reset_pending(
        &mut self,
        target: u64,
        leader: &UrsulaVote,
        matched: Option<u64>,
        confirmed: Option<u64>,
    ) -> bool {
        if self.leader.as_ref() != Some(leader) {
            self.allowed_reverts.clear();
            return false;
        }
        let Some(previous) = self.allowed_reverts.get(&target).copied() else {
            return false;
        };
        // A fast rebuild can reset and advance metrics between our samples.
        // A successful RPC through the former matched point also ends the
        // authorization, so a second loss never inherits this override.
        if matched.is_none_or(|matched| matched < previous)
            || confirmed.is_some_and(|confirmed| confirmed >= previous)
        {
            self.allowed_reverts.remove(&target);
            false
        } else {
            true
        }
    }
}

/// One memory-WAL replica's rejoin state for one group: the inbound vote
/// gate and, while it leads, the followers it saw lose their log.
pub struct GroupRejoin {
    node_id: u64,
    raft_group_id: RaftGroupId,
    metrics: OnceLock<MetricsReceiver>,
    gate: Mutex<VoteGate>,
    reverted: Mutex<RevertedFollowers>,
    restart_guard: RestartGuard,
}

impl fmt::Debug for GroupRejoin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupRejoin")
            .field("node_id", &self.node_id)
            .field("raft_group_id", &self.raft_group_id)
            .field("bound", &self.metrics.get().is_some())
            .finish_non_exhaustive()
    }
}

impl GroupRejoin {
    pub fn new(node_id: u64, raft_group_id: RaftGroupId) -> Self {
        Self {
            node_id,
            raft_group_id,
            metrics: OnceLock::new(),
            gate: Mutex::new(VoteGate::default()),
            reverted: Mutex::new(RevertedFollowers::default()),
            restart_guard: RestartGuard::new(raft_group_id, None),
        }
    }

    /// Keep the group's "initialized" marker in `store` (object storage), so
    /// a restart of every voter stops the group instead of re-initializing
    /// it (see `crate::restart_guard`).
    pub fn with_init_markers(mut self, store: Option<Arc<dyn InitMarkerStore>>) -> Self {
        self.restart_guard = RestartGuard::new(self.raft_group_id, store);
        self
    }

    pub fn raft_group_id(&self) -> RaftGroupId {
        self.raft_group_id
    }

    /// The group's full-restart guard.
    pub fn restart_guard(&self) -> &RestartGuard {
        &self.restart_guard
    }

    /// Attach the group's Raft metrics. Call once the Raft exists and before
    /// it is reachable from the network.
    pub fn bind(&self, raft: &RaftGroupHandle) {
        raft.runtime_config().elect(false);
        if self.metrics.set(raft.metrics()).is_err() {
            tracing::warn!(
                raft_group_id = self.raft_group_id.0,
                "memory-WAL rejoin metrics were already bound"
            );
        }
    }

    fn metrics(&self) -> Option<RaftMetrics<UrsulaRaftTypeConfig>> {
        self.metrics
            .get()
            .map(|metrics| metrics.borrow_watched().clone())
    }

    /// Whether the vote gate is open: the replica caught up once.
    pub fn vote_gate_open(&self) -> bool {
        let local = self.metrics().map(|metrics| local_replica(&metrics));
        let mut gate = self.gate.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(local) = local {
            gate.refresh(local);
        }
        gate.is_open()
    }

    /// Called only after every peer was proven empty and the initialized
    /// marker was absent (or the operator explicitly accepted data loss).
    pub(crate) fn allow_fresh_bootstrap(&self) {
        self.gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .allow_fresh_bootstrap();
    }

    pub(crate) fn confirm_barrier(&self, leader: UrsulaVote, commit_index: u64) {
        self.gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .confirm_barrier(leader, commit_index);
    }

    /// Campaigning is subject to both this barrier and the node's shed policy.
    pub(crate) fn may_campaign(&self) -> bool {
        self.vote_gate_open()
            || self
                .gate
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .released_for
                == Some(self.node_id)
    }

    /// Follower side: screen an inbound vote request. `Some` is the refusal
    /// to send back instead of handing the request to OpenRaft.
    pub fn screen_vote(&self, request: &UrsulaVoteRequest) -> Option<UrsulaVoteResponse> {
        let metrics = self.metrics()?;
        let local = local_replica(&metrics);
        let candidate = *request.vote.leader_id().node_id();
        let candidate_index = log_index(request.last_log_id.as_ref());
        let screen = self
            .gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .screen(candidate, candidate_index, local);
        match screen {
            VoteScreen::Pass => None,
            VoteScreen::Refuse => {
                tracing::info!(
                    node_id = self.node_id,
                    raft_group_id = self.raft_group_id.0,
                    candidate,
                    candidate_last_log_index = ?candidate_index,
                    last_applied = ?local.last_applied,
                    "memory-WAL rejoin: refusing a vote until this replica has caught up"
                );
                Some(UrsulaVoteResponse::new(
                    metrics.vote,
                    metrics.last_applied,
                    false,
                ))
            }
        }
    }

    /// Follower side: record an inbound AppendEntries.
    pub fn observe_inbound_append(&self, request: &UrsulaAppendEntriesRequest) {
        self.gate
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .observe_append(&request.vote, log_index(request.leader_commit.as_ref()));
    }

    /// Leader side: whether `response` from `target` shows that the target
    /// lost entries it had acknowledged in this leadership (a conflict at or
    /// below its matched index). The caller then hands OpenRaft a network
    /// error instead of the conflict, which OpenRaft cannot act on.
    pub fn follower_lost_log(
        &self,
        target: u64,
        leader: &UrsulaVote,
        prev_log_id: Option<&LogIdOf<UrsulaRaftTypeConfig>>,
        sent_last_log_id: Option<&LogIdOf<UrsulaRaftTypeConfig>>,
        response: &UrsulaAppendEntriesResponse,
    ) -> bool {
        let Some(metrics) = self.metrics() else {
            return false;
        };
        if &metrics.vote != leader {
            return false;
        }
        let matched = metrics
            .replication
            .as_ref()
            .and_then(|replication| replication.get(&target))
            .and_then(|matched| log_index(matched.as_ref()));
        let mut reverted = self.reverted.lock().unwrap_or_else(PoisonError::into_inner);
        let confirmed = match response {
            UrsulaAppendEntriesResponse::Success => log_index(sent_last_log_id.or(prev_log_id)),
            UrsulaAppendEntriesResponse::PartialSuccess(matching) => log_index(matching.as_ref()),
            UrsulaAppendEntriesResponse::Conflict | UrsulaAppendEntriesResponse::HigherVote(_) => {
                None
            }
        };
        let operator_reset_pending =
            reverted.operator_reset_pending(target, leader, matched, confirmed);
        if !matches!(response, UrsulaAppendEntriesResponse::Conflict) {
            return false;
        }
        let Some(prev) = log_index(prev_log_id) else {
            return false;
        };
        if !matched.is_some_and(|matched| prev <= matched) {
            return false;
        }
        if operator_reset_pending {
            return false;
        }
        if reverted.leader.as_ref() != Some(leader) {
            reverted.leader = Some(*leader);
            reverted.targets.clear();
        }
        if reverted.targets.insert(target) {
            tracing::warn!(
                node_id = self.node_id,
                raft_group_id = self.raft_group_id.0,
                target,
                conflict_index = prev,
                matched_index = ?matched,
                "memory-WAL rejoin: follower lost Raft log entries it had acknowledged; \
                 rebuilding it through learner catch-up"
            );
        }
        true
    }

    /// The followers that lost their log under `leader`.
    pub fn reverted_followers(&self, leader: &UrsulaVote) -> BTreeSet<u64> {
        let reverted = self.reverted.lock().unwrap_or_else(PoisonError::into_inner);
        if reverted.leader.as_ref() == Some(leader) {
            reverted.targets.clone()
        } else {
            BTreeSet::new()
        }
    }

    /// Whether `target` lost its log under this replica's current leadership.
    pub fn is_reverted_follower(&self, target: u64) -> bool {
        self.metrics()
            .is_some_and(|metrics| self.reverted_followers(&metrics.vote).contains(&target))
    }

    fn clear_reverted(&self, target: u64) {
        self.reverted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .targets
            .remove(&target);
    }

    /// Operator recovery after a majority restart, run on every replica of
    /// the group with the same `survivor`:
    ///
    /// - on an empty replica, it lets this replica vote for `survivor` while
    ///   it is still behind;
    /// - on the survivor while it leads, it lets OpenRaft rewind its
    ///   progress for every follower that lost its log, so it replicates
    ///   them from the start.
    ///
    /// Acknowledged writes the survivor does not hold are lost; that is the
    /// operator's decision.
    pub async fn adopt_survivor(
        &self,
        raft: &RaftGroupHandle,
        survivor: u64,
    ) -> Result<AdoptSurvivorOutcome, String> {
        if survivor != self.node_id {
            self.gate
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .release_for(survivor);
            tracing::warn!(
                node_id = self.node_id,
                raft_group_id = self.raft_group_id.0,
                survivor,
                "memory-WAL rejoin: operator released the vote gate for the survivor"
            );
            return Ok(AdoptSurvivorOutcome::VoteReleased);
        }
        let Some(metrics) = self.metrics() else {
            return Err("group metrics are not bound yet".to_owned());
        };
        if metrics.state != ServerState::Leader {
            self.gate
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .release_for(survivor);
            return Ok(AdoptSurvivorOutcome::NotLeader);
        }
        let targets = self.reverted_followers(&metrics.vote);
        for target in &targets {
            raft.trigger()
                .allow_next_revert(target, true)
                .await
                .map_err(|err| format!("allow next revert for node {target}: {err}"))?
                .map_err(|err| format!("allow next revert for node {target}: {err}"))?;
        }
        {
            let mut reverted = self.reverted.lock().unwrap_or_else(PoisonError::into_inner);
            for target in &targets {
                reverted.targets.remove(target);
                if let Some(matched) = metrics
                    .replication
                    .as_ref()
                    .and_then(|replication| replication.get(target))
                    .and_then(|matched| log_index(matched.as_ref()))
                {
                    reverted.allowed_reverts.insert(*target, matched);
                }
            }
        }
        tracing::warn!(
            node_id = self.node_id,
            raft_group_id = self.raft_group_id.0,
            followers = ?targets,
            "memory-WAL rejoin: operator adopted this leader's log; replicating emptied followers from the start"
        );
        Ok(AdoptSurvivorOutcome::FollowersReset(targets))
    }
}

/// What [`GroupRejoin::adopt_survivor`] did on one replica.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdoptSurvivorOutcome {
    /// This replica may now vote for the survivor.
    VoteReleased,
    /// This replica is the survivor and leads: these followers are
    /// replicated from the start.
    FollowersReset(BTreeSet<u64>),
    /// This replica is the survivor but does not lead (it campaigns once the
    /// other replicas released their vote).
    NotLeader,
}

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

/// What a memory-WAL initializer does with a group it has no log for.
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

/// The leader's view of one group, as the heal driver reads it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct HealView {
    pub is_leader: bool,
    /// The effective membership is a single (non-joint) config.
    pub uniform: bool,
    /// The joint config was already the effective one on the previous
    /// tick: nothing is flattening it.
    pub stale_joint: bool,
    pub voters: BTreeSet<u64>,
    pub learners: BTreeSet<u64>,
    pub reverted: BTreeSet<u64>,
    pub matched: BTreeMap<u64, Option<u64>>,
    pub committed: Option<u64>,
    /// The group's configured (static) voters.
    pub configured: BTreeSet<u64>,
}

/// One membership step of the heal driver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HealStep {
    /// Remove a voter that lost its log; `voters` is the new voter set.
    RemoveVoter { target: u64, voters: BTreeSet<u64> },
    /// Remove a learner that lost its log, so it is added back fresh.
    RemoveLearner { target: u64 },
    /// Add a configured voter that is missing as a learner.
    AddLearner { target: u64 },
    /// Promote a caught-up learner: `voters` is the current voters plus it.
    Promote { target: u64, voters: BTreeSet<u64> },
    /// Flatten a joint config whose second step was never proposed (the
    /// proposing future timed out or its leader stepped down).
    FinishJoint,
}

fn quorum(voter_count: usize) -> usize {
    (voter_count / 2).saturating_add(1)
}

/// The next heal step for a group this node leads, if any.
pub(crate) fn plan_heal_step(view: &HealView) -> Option<HealStep> {
    if !view.is_leader {
        return None;
    }
    if !view.uniform {
        return view.stale_joint.then_some(HealStep::FinishJoint);
    }
    if let Some(target) = view.voters.intersection(&view.reverted).next() {
        // The removal commits only with a quorum of the current voters that
        // still hold their log. Without one (a majority restarted empty)
        // nothing is proposed: the group waits for an operator.
        let healthy_voters = view.voters.difference(&view.reverted).count();
        if healthy_voters < quorum(view.voters.len()) {
            return None;
        }
        let mut voters = view.voters.clone();
        voters.remove(target);
        return Some(HealStep::RemoveVoter {
            target: *target,
            voters,
        });
    }
    if let Some(target) = view.learners.intersection(&view.reverted).next() {
        return Some(HealStep::RemoveLearner { target: *target });
    }
    if !view.voters.is_subset(&view.configured) {
        return None;
    }
    // Several configured voters can be missing at once (two overlapping
    // restarts in a group of five): promote whichever caught up first, and
    // add the others back as learners one step at a time.
    let missing = view.configured.difference(&view.voters);
    let caught_up = missing.clone().find(|target| {
        view.learners.contains(target)
            && view.matched.get(target).copied().flatten() >= view.committed
    });
    if let Some(target) = caught_up {
        let mut voters = view.voters.clone();
        voters.insert(*target);
        return Some(HealStep::Promote {
            target: *target,
            voters,
        });
    }
    missing
        .clone()
        .find(|target| !view.learners.contains(target))
        .map(|target| HealStep::AddLearner { target: *target })
}

fn heal_view(
    metrics: &RaftMetrics<UrsulaRaftTypeConfig>,
    rejoin: &GroupRejoin,
    configured: &BTreeSet<u64>,
) -> HealView {
    let membership = metrics.membership_config.membership();
    HealView {
        is_leader: metrics.state == ServerState::Leader,
        uniform: membership.get_joint_config().len() == 1,
        stale_joint: false,
        voters: membership.voter_ids().collect(),
        learners: membership.learner_ids().collect(),
        reverted: rejoin.reverted_followers(&metrics.vote),
        matched: metrics
            .replication
            .as_ref()
            .map(|replication| {
                replication
                    .iter()
                    .map(|(node_id, matched)| (*node_id, log_index(matched.as_ref())))
                    .collect()
            })
            .unwrap_or_default(),
        committed: log_index(metrics.committed.as_ref()),
        configured: configured.clone(),
    }
}

/// Leader-side heal driver for one group of a memory-WAL node: rebuilds a
/// voter that lost its log through remove / learner / catch-up / promote,
/// and finishes a rebuild another leader or `ursulactl` started. Returns
/// once the Raft stops.
pub async fn run_rejoin_heal(
    raft: RaftGroupHandle,
    rejoin: std::sync::Arc<GroupRejoin>,
    configured: BTreeMap<u64, BasicNode>,
    interval: Duration,
) {
    let configured_ids = configured.keys().copied().collect::<BTreeSet<_>>();
    // The log id of the joint config seen on the previous tick, if any.
    let mut last_joint = None;
    loop {
        crate::rt::time::sleep(interval).await;
        let step = {
            let metrics = raft.metrics().borrow_watched().clone();
            if metrics.running_state.is_err() {
                return;
            }
            let mut view = heal_view(&metrics, &rejoin, &configured_ids);
            let joint = (!view.uniform).then(|| *metrics.membership_config.log_id());
            view.stale_joint = joint.is_some() && joint == last_joint;
            last_joint = joint;
            plan_heal_step(&view)
        };
        let Some(step) = step else {
            continue;
        };
        let group = rejoin.raft_group_id.0;
        let node_id = rejoin.node_id;
        tracing::warn!(
            node_id,
            raft_group_id = group,
            step = ?step,
            "memory-WAL rejoin: healing a replica that restarted empty"
        );
        let result = match &step {
            HealStep::RemoveVoter { voters, .. } => crate::rt::time::timeout(
                REJOIN_HEAL_STEP_TIMEOUT,
                raft.change_membership(voters.clone(), false),
            )
            .await
            .map(|result| result.map(|_| ()).map_err(|err| err.to_string())),
            HealStep::RemoveLearner { target } => crate::rt::time::timeout(
                REJOIN_HEAL_STEP_TIMEOUT,
                raft.change_membership(
                    ChangeMembers::RemoveNodes(BTreeSet::from([*target])),
                    false,
                ),
            )
            .await
            .map(|result| result.map(|_| ()).map_err(|err| err.to_string())),
            HealStep::AddLearner { target } => {
                let Some(node) = configured.get(target).cloned() else {
                    continue;
                };
                crate::rt::time::timeout(
                    REJOIN_HEAL_STEP_TIMEOUT,
                    raft.add_learner(*target, node, false),
                )
                .await
                .map(|result| result.map(|_| ()).map_err(|err| err.to_string()))
            }
            HealStep::Promote { voters, .. } => crate::rt::time::timeout(
                REJOIN_HEAL_STEP_TIMEOUT,
                raft.change_membership(voters.clone(), false),
            )
            .await
            .map(|result| result.map(|_| ()).map_err(|err| err.to_string())),
            // A no-op change on a joint config is OpenRaft's own second step:
            // it commits the new config alone.
            HealStep::FinishJoint => crate::rt::time::timeout(
                REJOIN_HEAL_STEP_TIMEOUT,
                raft.change_membership(ChangeMembers::AddVoterIds(BTreeSet::new()), false),
            )
            .await
            .map(|result| result.map(|_| ()).map_err(|err| err.to_string())),
        };
        match result {
            Ok(Ok(())) => match &step {
                HealStep::RemoveVoter { target, .. } | HealStep::RemoveLearner { target } => {
                    rejoin.clear_reverted(*target);
                }
                HealStep::AddLearner { .. } | HealStep::FinishJoint => {}
                HealStep::Promote { target, .. } => tracing::info!(
                    node_id,
                    raft_group_id = group,
                    target,
                    "memory-WAL rejoin: the replica is a caught-up voter again"
                ),
            },
            Ok(Err(err)) => tracing::warn!(
                node_id,
                raft_group_id = group,
                step = ?step,
                "memory-WAL rejoin: heal step failed, retrying: {err}"
            ),
            Err(_) => tracing::warn!(
                node_id,
                raft_group_id = group,
                step = ?step,
                "memory-WAL rejoin: heal step timed out, retrying"
            ),
        }
    }
}

/// Drive memory-WAL participation from fresh outbound leader proofs. The
/// supplied probe must confirm a new post-call ReadIndex with a quorum and
/// return that leader's committed vote and required local applied index.
/// Transport-independent so simulation exercises the production gate driver.
pub async fn run_rejoin_vote_barrier<P, F>(
    raft: RaftGroupHandle,
    rejoin: Arc<GroupRejoin>,
    registry: RaftGroupHandleRegistry,
    nodes: BTreeMap<u64, BasicNode>,
    probe: P,
    probe_timeout: Duration,
    interval: Duration,
) where
    P: Fn(u64, String) -> F,
    F: Future<Output = Result<(UrsulaVote, u64), String>>,
{
    let raft_group_id = rejoin.raft_group_id;
    let mut last_barrier_leader = None;
    loop {
        registry.refresh_group_elections(raft_group_id);
        if rejoin.vote_gate_open() {
            return;
        }
        let metrics = raft.metrics().borrow_watched().clone();
        if metrics.running_state.is_err() {
            return;
        }
        if let Some(leader_id) = metrics.current_leader
            && last_barrier_leader != Some(metrics.vote)
            && let Some(node) = nodes.get(&leader_id)
        {
            let outcome =
                crate::rt::time::timeout(probe_timeout, probe(leader_id, node.addr.clone())).await;
            let (leader, index) = match outcome {
                Ok(Ok(proof)) => proof,
                other => {
                    tracing::debug!(
                        raft_group_id = raft_group_id.0,
                        ?other,
                        "recovery barrier probe failed"
                    );
                    crate::rt::time::sleep(interval).await;
                    continue;
                }
            };
            rejoin.confirm_barrier(leader, index);
            last_barrier_leader = Some(leader);
            registry.refresh_group_elections(raft_group_id);
            if rejoin.vote_gate_open() {
                tracing::info!(
                    node_id = metrics.id,
                    raft_group_id = raft_group_id.0,
                    barrier_index = index,
                    "memory-WAL rejoin: fresh quorum barrier applied; participation restored"
                );
                return;
            }
        }
        crate::rt::time::sleep(interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vote(term: u64, node_id: u64) -> UrsulaVote {
        UrsulaVote::new(term, node_id)
    }

    fn leader(term: u64, node_id: u64) -> UrsulaVote {
        UrsulaVote::new_committed(term, node_id)
    }

    fn log_id(term: u64, node_id: u64, index: u64) -> LogIdOf<UrsulaRaftTypeConfig> {
        type LeaderId = <UrsulaRaftTypeConfig as openraft::RaftTypeConfig>::LeaderId;
        openraft::LogId::new(LeaderId::new(term, node_id), index)
    }

    fn follower(last_applied: Option<u64>) -> LocalReplica {
        LocalReplica { last_applied }
    }

    #[test]
    fn operator_rewind_survives_read_index_conflicts_but_ends_after_repair() {
        let leader = leader(3, 1);
        let mut reverted = RevertedFollowers {
            leader: Some(leader),
            allowed_reverts: BTreeMap::from([(2, 20), (3, 20)]),
            ..Default::default()
        };
        // ReadIndex conflicts leave Raft's replication progress untouched.
        assert!(reverted.operator_reset_pending(2, &leader, Some(20), None));
        assert!(reverted.operator_reset_pending(2, &leader, Some(20), None));
        // A real rewind resets the progress; permission cannot cover another
        // loss even after this follower has caught up again.
        assert!(!reverted.operator_reset_pending(2, &leader, None, None));
        assert!(!reverted.operator_reset_pending(2, &leader, Some(30), None));
        // A fast catch-up can happen between metrics samples. Its successful
        // Append still proves that the prior prefix has been restored.
        assert!(reverted.operator_reset_pending(3, &leader, Some(20), Some(19)));
        assert!(!reverted.operator_reset_pending(3, &leader, Some(30), Some(20)));
        assert!(!reverted.operator_reset_pending(3, &leader, Some(30), None));
    }

    #[test]
    fn operator_rewind_permission_does_not_cross_a_leader_change() {
        let mut reverted = RevertedFollowers {
            leader: Some(leader(3, 1)),
            allowed_reverts: BTreeMap::from([(2, 20)]),
            ..Default::default()
        };
        assert!(!reverted.operator_reset_pending(2, &leader(4, 3), Some(20), None));
        assert!(reverted.allowed_reverts.is_empty());
    }

    #[test]
    fn a_fresh_replica_votes_in_the_first_election() {
        let mut gate = VoteGate::default();
        // The initializer's candidate log is only the membership entry.
        assert_eq!(gate.screen(1, Some(0), follower(None)), VoteScreen::Pass);
        assert_eq!(gate.screen(1, None, follower(None)), VoteScreen::Pass);
    }

    #[test]
    fn an_empty_replica_refuses_a_candidate_with_entries_until_it_caught_up() {
        let mut gate = VoteGate::default();
        assert_eq!(gate.screen(2, Some(11), follower(None)), VoteScreen::Refuse);
        // Having seen entries, it refuses an index-0 candidate too: that can
        // only be another emptied replica.
        assert_eq!(gate.screen(3, Some(0), follower(None)), VoteScreen::Refuse);

        gate.observe_append(&leader(2, 1), Some(11));
        gate.confirm_barrier(leader(2, 1), 11);
        assert_eq!(
            gate.screen(2, Some(12), follower(Some(10))),
            VoteScreen::Refuse
        );
        assert!(!gate.is_open());
        assert_eq!(
            gate.screen(2, Some(12), follower(Some(11))),
            VoteScreen::Pass
        );
        assert!(gate.is_open());
        // Once open it stays open.
        assert_eq!(gate.screen(3, Some(0), follower(None)), VoteScreen::Pass);
    }

    #[test]
    fn the_catch_up_point_follows_the_highest_leader() {
        let mut gate = VoteGate::default();
        gate.confirm_barrier(leader(2, 1), 5);
        // A deposed leader's lower commit does not lower the target.
        gate.confirm_barrier(leader(1, 3), 1);
        gate.confirm_barrier(leader(3, 2), 9);
        assert_eq!(
            gate.screen(2, Some(9), follower(Some(5))),
            VoteScreen::Refuse
        );
        assert_eq!(gate.screen(2, Some(9), follower(Some(9))), VoteScreen::Pass);
    }

    #[test]
    fn fresh_bootstrap_opens_its_gate_and_an_operator_release_names_one_candidate() {
        let mut gate = VoteGate::default();
        gate.allow_fresh_bootstrap();
        assert_eq!(gate.screen(2, Some(4), follower(None)), VoteScreen::Pass);

        let mut gate = VoteGate::default();
        gate.observe_append(&leader(4, 1), Some(20));
        gate.release_for(3);
        assert_eq!(gate.screen(2, Some(20), follower(None)), VoteScreen::Refuse);
        assert_eq!(gate.screen(3, Some(20), follower(None)), VoteScreen::Pass);
    }

    #[test]
    fn delayed_replication_never_releases_a_restarted_vote_gate() {
        let mut gate = VoteGate::default();
        gate.observe_append(&leader(2, 1), Some(5));
        gate.observe_append(&leader(2, 1), Some(12));
        assert_eq!(
            gate.screen(3, Some(5), follower(Some(5))),
            VoteScreen::Refuse
        );
        // Even applying every inbound entry is not a post-start proof.
        assert_eq!(
            gate.screen(3, Some(12), follower(Some(12))),
            VoteScreen::Refuse
        );
        gate.confirm_barrier(leader(2, 1), 12);
        assert_eq!(
            gate.screen(3, Some(5), follower(Some(5))),
            VoteScreen::Refuse
        );
        assert_eq!(
            gate.screen(3, Some(12), follower(Some(12))),
            VoteScreen::Pass
        );
    }

    #[test]
    fn heartbeat_without_commit_and_uncommitted_proof_never_open_the_gate() {
        let mut gate = VoteGate::default();
        gate.observe_append(&leader(2, 1), None);
        gate.refresh(follower(None));
        assert!(!gate.is_open());
        gate.confirm_barrier(vote(3, 1), 20);
        gate.refresh(follower(Some(20)));
        assert!(!gate.is_open());
    }

    #[test]
    fn bootstrap_initializes_only_when_every_voter_is_empty() {
        let empty = Some(PeerGroupLog::Empty);
        let initialized = Some(PeerGroupLog::Initialized);
        assert_eq!(
            bootstrap_decision(&[empty, empty]),
            BootstrapDecision::Initialize
        );
        assert_eq!(
            bootstrap_decision(&Vec::new()),
            BootstrapDecision::Initialize
        );
        // A silent voter might hold the group: wait for it.
        assert_eq!(bootstrap_decision(&[empty, None]), BootstrapDecision::Wait);
        // One voter with entries or a leader is enough to rejoin.
        assert_eq!(
            bootstrap_decision(&[None, initialized]),
            BootstrapDecision::Rejoin
        );
        assert_eq!(
            bootstrap_decision(&[empty, initialized]),
            BootstrapDecision::Rejoin
        );
    }

    #[test]
    fn a_probe_answer_with_entries_or_a_leader_means_initialized() {
        let empty = UrsulaVoteResponse::new(vote(0, 2), None, false);
        assert_eq!(
            PeerGroupLog::from_vote_response(&empty),
            PeerGroupLog::Empty
        );
        let candidate = UrsulaVoteResponse::new(vote(1, 2), None, false);
        assert_eq!(
            PeerGroupLog::from_vote_response(&candidate),
            PeerGroupLog::Empty
        );
        let following = UrsulaVoteResponse::new(leader(3, 1), None, false);
        assert_eq!(
            PeerGroupLog::from_vote_response(&following),
            PeerGroupLog::Initialized
        );
        let holding = UrsulaVoteResponse::new(vote(2, 2), Some(log_id(2, 2, 5)), false);
        assert_eq!(
            PeerGroupLog::from_vote_response(&holding),
            PeerGroupLog::Initialized
        );
    }

    fn view(voters: &[u64], learners: &[u64], reverted: &[u64]) -> HealView {
        HealView {
            is_leader: true,
            uniform: true,
            stale_joint: false,
            voters: voters.iter().copied().collect(),
            learners: learners.iter().copied().collect(),
            reverted: reverted.iter().copied().collect(),
            matched: BTreeMap::new(),
            committed: Some(10),
            configured: BTreeSet::from([1, 2, 3]),
        }
    }

    #[test]
    fn the_heal_driver_walks_remove_learner_catch_up_promote() {
        assert_eq!(
            plan_heal_step(&view(&[1, 2, 3], &[], &[3])),
            Some(HealStep::RemoveVoter {
                target: 3,
                voters: BTreeSet::from([1, 2]),
            })
        );
        assert_eq!(
            plan_heal_step(&view(&[1, 2], &[], &[])),
            Some(HealStep::AddLearner { target: 3 })
        );
        let mut catching_up = view(&[1, 2], &[3], &[]);
        catching_up.matched.insert(3, Some(9));
        assert_eq!(plan_heal_step(&catching_up), None);
        catching_up.matched.insert(3, Some(10));
        assert_eq!(
            plan_heal_step(&catching_up),
            Some(HealStep::Promote {
                target: 3,
                voters: BTreeSet::from([1, 2, 3]),
            })
        );
        // A learner that restarted again mid-heal is dropped and re-added.
        assert_eq!(
            plan_heal_step(&view(&[1, 2], &[3], &[3])),
            Some(HealStep::RemoveLearner { target: 3 })
        );
        assert_eq!(plan_heal_step(&view(&[1, 2, 3], &[], &[])), None);
    }

    #[test]
    fn the_heal_driver_never_acts_without_a_healthy_quorum_or_leadership() {
        // A majority lost its log: no removal could commit.
        assert_eq!(plan_heal_step(&view(&[1, 2, 3], &[], &[2, 3])), None);
        let mut follower_view = view(&[1, 2, 3], &[], &[3]);
        follower_view.is_leader = false;
        assert_eq!(plan_heal_step(&follower_view), None);
        // A voter outside the static config: not a restart repair, leave it
        // to the operator.
        assert_eq!(plan_heal_step(&view(&[1, 2, 4], &[], &[])), None);
    }

    #[test]
    fn the_heal_driver_rebuilds_two_overlapping_restarts_in_a_group_of_five() {
        let five = |voters: &[u64], learners: &[u64], reverted: &[u64]| HealView {
            configured: BTreeSet::from([1, 2, 3, 4, 5]),
            ..view(voters, learners, reverted)
        };
        // 4 and 5 restarted: both are removed while three healthy voters
        // remain a quorum.
        assert!(matches!(
            plan_heal_step(&five(&[1, 2, 3, 4, 5], &[], &[4, 5])),
            Some(HealStep::RemoveVoter { .. })
        ));
        assert!(matches!(
            plan_heal_step(&five(&[1, 2, 3, 4], &[], &[4])),
            Some(HealStep::RemoveVoter { target: 4, .. })
        ));
        assert_eq!(
            plan_heal_step(&five(&[1, 2, 3], &[], &[])),
            Some(HealStep::AddLearner { target: 4 })
        );
        // 4 caught up while 5 is not back yet: promote 4 alone.
        let mut partly = five(&[1, 2, 3], &[4], &[]);
        partly.matched.insert(4, Some(10));
        assert_eq!(
            plan_heal_step(&partly),
            Some(HealStep::Promote {
                target: 4,
                voters: BTreeSet::from([1, 2, 3, 4]),
            })
        );
        partly.matched.insert(4, Some(9));
        assert_eq!(
            plan_heal_step(&partly),
            Some(HealStep::AddLearner { target: 5 })
        );
        let mut last = five(&[1, 2, 3, 4], &[5], &[]);
        last.matched.insert(5, Some(10));
        assert_eq!(
            plan_heal_step(&last),
            Some(HealStep::Promote {
                target: 5,
                voters: BTreeSet::from([1, 2, 3, 4, 5]),
            })
        );
    }

    #[test]
    fn the_heal_driver_flattens_only_a_joint_config_nothing_else_finishes() {
        let mut joint = view(&[1, 2, 3], &[], &[3]);
        joint.uniform = false;
        // Just entered: OpenRaft's own second step is still on its way.
        assert_eq!(plan_heal_step(&joint), None);
        joint.stale_joint = true;
        assert_eq!(plan_heal_step(&joint), Some(HealStep::FinishJoint));
        joint.is_leader = false;
        assert_eq!(plan_heal_step(&joint), None);
    }
}
