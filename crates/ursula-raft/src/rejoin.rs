//! The recovery gate: a replica whose Raft log may be missing entries it
//! acknowledged never helps elect a leader that lacks them, and rejoins once
//! it holds the group's log again.
//!
//! A replica can lose acknowledged entries in two ways: its host crashes
//! under `raft.wal.fsync = never` (or a journal I/O failure poisons it) and
//! it keeps only the verified prefix of its journal, or its journal is wiped
//! or replaced. The same pieces cover both.
//!
//! - **Entering the gate** ([`GroupRejoin`]): a replica starts gated when its
//!   durable log state ([`GroupLogState`]) says `Recovering` (the node
//!   started after a host crash or an I/O failure, a previous run left the
//!   group recovering, or the journal lost the log of an initialized group)
//!   or `Empty` (it never held the group here, or held it on a disk it lost).
//!   An empty replica's first entry records the group `Recovering` while the
//!   gate is closed, so a restart before the gate opens comes back gated.
//! - **While gated** ([`VoteGate`]): the replica does not campaign and does
//!   not take a leadership transfer. A replica that led the group starts as
//!   a follower instead of restoring its leadership, which would append new
//!   entries under the log ids of the ones it lost. That demotion is
//!   recorded before the Raft core starts, so no later start restores the
//!   leadership either, after a clean shutdown or once the gate opened. It
//!   refuses every vote once it knows the group holds entries: its log state
//!   says so, a leader reported a commit index of 1 or more, or a
//!   candidate's log reached index 1. Before that a new group's first
//!   election (candidates whose log is only the membership entry at index 0)
//!   goes through. It still accepts appends from any leader whose vote is
//!   not lower than its persisted vote; the WAL restores that vote before
//!   the Raft core starts, so a leader of an older term is refused. A
//!   replica that lost its vote refuses all replication ACKs until a fresh
//!   ReadIndex quorum excluding it proves a vote that is durably adopted.
//!   Older votes receive HigherVote so stale leaders step down.
//! - **Opening the gate** ([`run_rejoin_vote_barrier`]): the replica asks
//!   the current leader for a fresh outbound ReadIndex barrier and opens the
//!   gate once it has applied the barrier's committed index. Inbound
//!   replication alone never opens it: it may have been delayed across the
//!   restart. The replica records the open gate (`Initialized`) before it
//!   votes again. Then the node's election policy is refreshed.
//! - **Self-heal** ([`run_rejoin_heal`]): a leader whose follower answers
//!   `Conflict` at or below the index that follower had already matched in
//!   this leadership knows the follower lost entries. OpenRaft never rewinds
//!   that progress, so the network layer hands OpenRaft an error instead of
//!   the conflict, and the leader rebuilds the follower: remove the voter,
//!   add it back as a learner, wait for catch-up, promote. Every step is
//!   read off the current membership, so a leader change or a second restart
//!   in the middle converges. When the followers that lost entries are a
//!   majority, no removal can commit; the leader holds every committed
//!   entry, so it rewinds their replication instead and sends them its log
//!   again (an idle group gets an unchanged membership entry to carry the
//!   rewind).
//! - **Bootstrap** ([`run_group_bootstrap`]): a group's initializer whose
//!   replica holds nothing of the group runs `Initialize` only when every
//!   configured voter answers a probe `Vote` with an empty log and no
//!   leader and has persisted its genesis floor. The reserved T0-N0 probe
//!   is read-only; its grant bit reports this readiness. A replica that ever
//!   held the group never runs
//!   `Initialize`.
//!
//! If a majority of a group's voters are gated, no leader can confirm a
//! barrier: the group has no leader and refuses writes, and its gated
//! replicas report it ([`RecoveryGateStatus::Stalled`]). An operator who
//! accepts the loss of the unsynced tail sends a recovery plan to every
//! reachable member ([`GroupRejoin::accept_recovery_plan`], #581 section
//! 4.5). The plan names one leader and a point: the leader's gate opens,
//! and every other member votes only for that leader
//! ([`VoteGate::Planned`]), across restarts, until it applied a history
//! event above the point. The leader proposes that event first and serves
//! nothing before it (`history`). A member that knows more than the point,
//! follows a live leader, or no longer holds the log the tool read refuses
//! the plan and changes nothing. When the leader's last log term is below a
//! vote term a gated member holds, the plan cuts every member's log back to
//! the entries it knew were committed, so no stale-term entry can commit.
//! The cut runs inside the accept job: the replica closes
//! ([`VoteGate::Cutting`]: no vote, no campaign, no replication), records
//! the plan, and its group restarts ([`ReplicaRestart`]), cutting the log
//! before the new Raft core starts. A member of a cutting plan grants the
//! plan's leader its vote only once the leader shows it was cut. A planned
//! member's satisfied plan closes its gate until a leader's barrier opens it,
//! so a member that lost entries in a crash never votes with them missing.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::PoisonError;
use std::sync::Weak;
use std::time::Duration;

use openraft::Instant as _;
use openraft::RaftMetrics;
use openraft::alias::LogIdOf;
use openraft::rt::WatchReceiver;
use openraft::rt::WatchSender;
use openraft::type_config::TypeConfigExt;
use openraft::type_config::alias::WatchReceiverOf;
use openraft::type_config::alias::WatchSenderOf;
use openraft::vote::RaftLeaderId;
use ursula_proto::admin::RecoveryPlanOutcome;
use ursula_proto::admin::RecoveryPlanReport;
use ursula_proto::admin::RecoveryPlanRequest;
use ursula_proto::admin::ReplicaLogView;
use ursula_shard::RaftGroupId;

use crate::log_store::CoreJournalError;
use crate::log_store::GroupLogState;
use crate::log_store::RaftGroupFileLogStore;
use crate::log_store::RecoveryRecord;
use crate::registry::RaftGroupHandle;
use crate::types::UrsulaAppendEntriesRequest;
use crate::types::UrsulaAppendEntriesResponse;
use crate::types::UrsulaRaftTypeConfig;
use crate::types::UrsulaVote;
use crate::types::UrsulaVoteRequest;
use crate::types::UrsulaVoteResponse;

type MetricsReceiver = WatchReceiverOf<UrsulaRaftTypeConfig, RaftMetrics<UrsulaRaftTypeConfig>>;

/// How often the leader-side heal driver re-reads its group.
pub const REJOIN_HEAL_INTERVAL: Duration = Duration::from_millis(500);

/// How long a gated replica goes without a leader barrier and without
/// applying anything before it reports its group stalled: a majority of the
/// group's voters may be gated, and the group waits for an operator.
pub const RECOVERY_STALL_AFTER: Duration = Duration::from_secs(30);

/// A leader whose quorum acknowledged it within this long is live: a
/// recovery plan refuses to fence its group (#577 N7).
pub(crate) const LIVE_LEADER_QUORUM_ACK: Duration = Duration::from_secs(3);

/// How long one membership step of the heal driver may take.
pub(crate) const RECOVERY_BARRIER_TIMEOUT: Duration = Duration::from_secs(3);

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

/// The catch-up point from a fresh outbound quorum-confirmed barrier: the
/// leader's committed vote and the commit index the replica must apply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatchUpTarget {
    leader: UrsulaVote,
    commit_index: u64,
}

/// What a gated replica knows about its group's history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GroupEvidence {
    /// Nothing yet: a new group's first election may still need this
    /// replica's vote.
    Unknown,
    /// The group holds entries: every candidate is refused.
    Initialized,
}

/// How far a gated replica got towards opening its gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GateRecovery {
    /// No leader has confirmed a barrier yet.
    AwaitingBarrier,
    /// No leader confirmed a barrier and nothing was applied for a long
    /// time: a majority of the group's voters may be gated, and the group
    /// waits for an operator.
    Stalled,
    /// A leader confirmed `target`; the replica catches up to it.
    CatchingUp(CatchUpTarget),
}

/// Per-group vote gate (pure state; see the module docs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum VoteGate {
    /// The replica holds every entry it acknowledged: it votes and
    /// campaigns.
    Open,
    /// The replica may be missing entries it acknowledged.
    Closed {
        group: GroupEvidence,
        recovery: GateRecovery,
    },
    /// A recovery plan elects `leader`: the replica votes only for it and
    /// does not campaign, until the plan's history event applies here. When
    /// the plan cuts every log, `uncut_leader_term` is the leader's last log
    /// term before its cut, and the replica grants the leader its vote only
    /// once the leader shows it was cut: its log ends at most at `point`, or
    /// its last entry has a later term. Every entry the leader appends after
    /// the plan does, since it leads in a term above its current one; its
    /// stale suffix ends at exactly `uncut_leader_term`.
    Planned {
        leader: u64,
        point: u64,
        uncut_leader_term: Option<u64>,
    },
    /// The replica took a plan that cuts its log, and its group restarts to
    /// run the cut: it votes for no one, does not campaign and takes no
    /// replication, so its uncut log helps no leader.
    Cutting,
}

pub use ursula_proto::admin::RecoveryGateStatus;

impl VoteGate {
    pub(crate) fn closed(group: GroupEvidence) -> Self {
        Self::Closed {
            group,
            recovery: GateRecovery::AwaitingBarrier,
        }
    }

    pub(crate) fn is_open(&self) -> bool {
        matches!(self, Self::Open)
    }

    pub(crate) fn status(&self) -> RecoveryGateStatus {
        match self {
            Self::Open => RecoveryGateStatus::Open,
            Self::Closed { recovery, .. } => match recovery {
                GateRecovery::AwaitingBarrier => RecoveryGateStatus::AwaitingBarrier,
                GateRecovery::Stalled => RecoveryGateStatus::Stalled,
                GateRecovery::CatchingUp(_) => RecoveryGateStatus::CatchingUp,
            },
            Self::Planned { .. } => RecoveryGateStatus::Planned,
            Self::Cutting => RecoveryGateStatus::Cutting,
        }
    }

    /// The gate of a member that holds `record` and is not its leader.
    pub(crate) fn planned(record: &RecoveryRecord) -> Self {
        Self::Planned {
            leader: record.leader,
            point: record.point,
            uncut_leader_term: record.uncut_leader_term,
        }
    }

    /// Record an inbound AppendEntries that carried `leader_commit`.
    pub(crate) fn observe_append(&mut self, leader_commit: Option<u64>) {
        if let Self::Closed { group, .. } = self
            && leader_commit.is_some_and(|index| index >= 1)
        {
            *group = GroupEvidence::Initialized;
        }
    }

    /// Only this process's outbound recovery probe may establish a catch-up
    /// target. Inbound replication may have been delayed across the restart.
    pub(crate) fn confirm_barrier(&mut self, leader: UrsulaVote, commit_index: u64) {
        let Self::Closed { group, recovery } = self else {
            return;
        };
        if !leader.is_committed() {
            return;
        }
        if commit_index >= 1 {
            *group = GroupEvidence::Initialized;
        }
        match recovery {
            GateRecovery::CatchingUp(target) if target.leader == leader => {
                target.commit_index = target.commit_index.max(commit_index);
            }
            // A deposed (or incomparable) leader does not move the target.
            GateRecovery::CatchingUp(target)
                if leader.partial_cmp(&target.leader) != Some(std::cmp::Ordering::Greater) => {}
            GateRecovery::AwaitingBarrier | GateRecovery::Stalled | GateRecovery::CatchingUp(_) => {
                *recovery = GateRecovery::CatchingUp(CatchUpTarget {
                    leader,
                    commit_index,
                });
            }
        }
    }

    /// No barrier and no progress for long enough: report the group
    /// stalled. A target that led nowhere for that long is dropped; the next
    /// barrier sets a new one. Returns whether the gate just stalled.
    pub(crate) fn stall(&mut self) -> bool {
        match self {
            Self::Closed { recovery, .. } if *recovery != GateRecovery::Stalled => {
                *recovery = GateRecovery::Stalled;
                true
            }
            Self::Open | Self::Closed { .. } | Self::Planned { .. } | Self::Cutting => false,
        }
    }

    /// Whether the replica applied the barrier it was given.
    pub(crate) fn caught_up(&self, local: LocalReplica) -> bool {
        match self {
            Self::Open => true,
            // A planned replica's gate closes when its plan is satisfied,
            // and a cutting replica restarts.
            Self::Planned { .. } | Self::Cutting => false,
            Self::Closed { recovery, .. } => match recovery {
                GateRecovery::CatchingUp(target) => local
                    .last_applied
                    .is_some_and(|applied| applied >= target.commit_index),
                GateRecovery::AwaitingBarrier | GateRecovery::Stalled => false,
            },
        }
    }

    pub(crate) fn open(&mut self) {
        *self = Self::Open;
    }

    /// Decide one vote request of `candidate`, whose last log id is
    /// `candidate_last` (index and term).
    pub(crate) fn screen(
        &mut self,
        candidate: u64,
        candidate_last: Option<(u64, u64)>,
    ) -> VoteScreen {
        let candidate_last_log_index = candidate_last.map(|(index, _term)| index);
        let group = match self {
            Self::Open => return VoteScreen::Pass,
            // B3 (#577): a planned replica's vote opens for one candidate,
            // and, when the plan cuts, only once that candidate was cut.
            Self::Planned {
                leader,
                point,
                uncut_leader_term,
            } if *leader == candidate => {
                let shows_cut = |uncut_term: u64| {
                    candidate_last.is_none_or(|(index, term)| index <= *point || term > uncut_term)
                };
                return if uncut_leader_term.is_none_or(shows_cut)
                    || ursula_stream::sim_controls::off(
                        ursula_stream::sim_controls::Defence::RecoveryCut,
                    ) {
                    VoteScreen::Pass
                } else {
                    VoteScreen::Refuse
                };
            }
            Self::Planned { .. } | Self::Cutting => return VoteScreen::Refuse,
            Self::Closed { group, .. } => group,
        };
        if candidate_last_log_index.is_some_and(|index| index >= 1) {
            *group = GroupEvidence::Initialized;
        }
        match group {
            GroupEvidence::Unknown => VoteScreen::Pass,
            GroupEvidence::Initialized => VoteScreen::Refuse,
        }
    }
}

/// Followers that answered a conflict below what they had matched, per
/// leadership.
#[derive(Debug, Default)]
struct RevertedFollowers {
    leader: Option<UrsulaVote>,
    targets: BTreeSet<u64>,
    /// Rewinds this leader allowed, until Raft replication metrics show the
    /// old matched point was reset. ReadIndex also sends Append RPCs; its
    /// Conflict confirms leadership but does not reset replication progress.
    /// Consuming the allowance on that response would strand the rewind.
    allowed_reverts: BTreeMap<u64, u64>,
}

impl RevertedFollowers {
    /// The rewinds `leader` allowed, by target, with the index each target
    /// had matched.
    fn allowed(&self, leader: &UrsulaVote) -> BTreeMap<u64, u64> {
        if self.leader.as_ref() == Some(leader) {
            self.allowed_reverts.clone()
        } else {
            BTreeMap::new()
        }
    }

    fn rewind_pending(
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
        // allowance, so a second loss never inherits it.
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

/// Why a gate opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GateOpening {
    /// The replica applied a fresh leader barrier.
    CaughtUp,
    /// Every voter reported an empty group: this replica initializes it.
    FreshBootstrap,
    /// A recovery plan elects this replica.
    PlanLeader,
}

/// A replica's log: what a recovery plan expects each member to hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReplicaLog {
    pub(crate) last_log_index: Option<u64>,
    pub(crate) last_log_term: Option<u64>,
    pub(crate) current_term: u64,
}

impl ReplicaLog {
    /// The log `store` holds, and the term of the vote it runs with.
    fn of(store: &RaftGroupFileLogStore) -> Self {
        let last = store.last_log_id();
        Self {
            last_log_index: log_index(last.as_ref()),
            last_log_term: last.map(|last| last.leader_id.term),
            current_term: store.vote().map_or(0, |vote| vote.leader_id().term()),
        }
    }
}

impl From<ReplicaLogView> for ReplicaLog {
    fn from(view: ReplicaLogView) -> Self {
        Self {
            last_log_index: view.last_log_index,
            last_log_term: view.last_log_term,
            current_term: view.current_term,
        }
    }
}

impl From<ReplicaLog> for ReplicaLogView {
    fn from(log: ReplicaLog) -> Self {
        Self {
            last_log_index: log.last_log_index,
            last_log_term: log.last_log_term,
            current_term: log.current_term,
        }
    }
}

/// Failure to open a recovery gate.
#[derive(Debug, thiserror::Error)]
pub enum RecoveryGateError {
    #[error("raft group {} owner stopped: {source}", .raft_group_id.0)]
    OwnerStopped {
        raft_group_id: RaftGroupId,
        #[source]
        source: openraft::error::Fatal<UrsulaRaftTypeConfig>,
    },
    #[error("raft group {} is not registered on this node", .raft_group_id.0)]
    NotRegistered { raft_group_id: RaftGroupId },
    #[error("raft group {} has stopped: its log store is closed", .raft_group_id.0)]
    StoreClosed { raft_group_id: RaftGroupId },
    #[error("raft group {} lost its vote history; accepting an unsynced log tail cannot restore it", .raft_group_id.0)]
    MissingVoteFloor { raft_group_id: RaftGroupId },
    #[error("record that raft group {}'s recovery gate opened: {source}", .raft_group_id.0)]
    Record {
        raft_group_id: RaftGroupId,
        #[source]
        source: CoreJournalError,
    },
    #[error(
        "record that this replica of raft group {}, which led it, starts as a follower: {source}",
        .raft_group_id.0
    )]
    StartAsFollower {
        raft_group_id: RaftGroupId,
        #[source]
        source: CoreJournalError,
    },
    #[error(
        "raft group {} on this replica follows the live leader {leader}; a recovery plan is \
         only for a group without one",
        .raft_group_id.0
    )]
    FollowsLeader {
        raft_group_id: RaftGroupId,
        leader: u64,
    },
    #[error(
        "raft group {}'s recovery gate on this replica is {status:?}, not stalled: a leader's \
         barrier may still open it without losing anything; plan only a group whose gated \
         replicas report it stalled",
        .raft_group_id.0
    )]
    NotStalled {
        raft_group_id: RaftGroupId,
        status: RecoveryGateStatus,
    },
    #[error(
        "raft group {} on this replica holds {actual:?}, not the expected {expected:?}; \
         observe the group again",
        .raft_group_id.0
    )]
    ReplicaChanged {
        raft_group_id: RaftGroupId,
        expected: ReplicaLogView,
        actual: ReplicaLogView,
    },
    #[error(
        "raft group {} on this replica committed index {committed:?} and applied index \
         {applied:?}, above the plan's point {point}; the plan picked the wrong leader",
        .raft_group_id.0
    )]
    RecoveryPointBehind {
        raft_group_id: RaftGroupId,
        point: u64,
        committed: Option<u64>,
        applied: Option<u64>,
    },
    #[error(
        "raft group {} on this replica knows no committed entry, so a plan cannot cut its log",
        .raft_group_id.0
    )]
    NothingCommitted { raft_group_id: RaftGroupId },
    #[error(
        "raft group {} on this replica recorded {committed:?} committed but holds {held:?} at \
         that index; observe the group again",
        .raft_group_id.0
    )]
    CommittedEntryMissing {
        raft_group_id: RaftGroupId,
        committed: LogIdOf<UrsulaRaftTypeConfig>,
        held: Option<LogIdOf<UrsulaRaftTypeConfig>>,
    },
    #[error(
        "raft group {}'s recovery plan elects node {leader}, which is not one of its voters",
        .raft_group_id.0
    )]
    LeaderNotVoter {
        raft_group_id: RaftGroupId,
        leader: u64,
    },
    #[error(
        "raft group {}'s recovery plan elects this replica with point {point}, but its log \
         continues from {expected} ({basis}); observe the group again",
        .raft_group_id.0
    )]
    PointMismatch {
        raft_group_id: RaftGroupId,
        point: u64,
        expected: u64,
        basis: &'static str,
    },
    #[error(
        "raft group {}'s recovery plan names {named:?} as its leader's last log term, but the \
         leader holds {held:?}; a cutting plan must name it; observe the group again",
        .raft_group_id.0
    )]
    LeaderLogMismatch {
        raft_group_id: RaftGroupId,
        named: Option<u64>,
        held: Option<u64>,
    },
    #[error("raft group {}'s replica on this node has not started", .raft_group_id.0)]
    NotStarted { raft_group_id: RaftGroupId },
    #[error("run the recovery cut of raft group {}: {source}", .raft_group_id.0)]
    Cut {
        raft_group_id: RaftGroupId,
        #[source]
        source: crate::log_store::RecoveryCutError,
    },
    #[error("restart raft group {}'s replica to run its recovery cut: {source}", .raft_group_id.0)]
    Restart {
        raft_group_id: RaftGroupId,
        #[source]
        source: Box<ursula_runtime::RuntimeError>,
    },
}

/// Restarts one group's replica on this node: stops its engine, so its Raft
/// core and log store close, and starts it again from its log store. A
/// recovery plan's cut runs in between (#581 section 4.5). The node's
/// runtime implements it; simulations restart their own replicas.
pub trait ReplicaRestart: Send {
    fn restart_replica(
        &mut self,
        raft_group_id: RaftGroupId,
    ) -> futures_util::future::BoxFuture<'_, Result<(), ursula_runtime::RuntimeError>>;
}

/// What a replica that took a recovery plan does next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PlanTaken {
    /// The plan holds: the replica leads or follows as `report` says.
    Holds(RecoveryPlanReport),
    /// The plan cuts this replica's log: its group restarts, and then the
    /// replica leads or follows as `report` says.
    Restart(RecoveryPlanReport),
}

/// One replica's recovery state for one group: the inbound vote gate and,
/// while it leads, the followers it saw lose entries.
pub struct GroupRejoin {
    node_id: u64,
    raft_group_id: RaftGroupId,
    metrics: OnceLock<MetricsReceiver>,
    gate: Mutex<VoteGate>,
    vote_floor: Mutex<Option<UrsulaVote>>,
    reverted: Mutex<RevertedFollowers>,
    /// The group's log store, which keeps its log state and where the gate
    /// records that it opened. Weak, so a stopped group's store closes even
    /// while its gate is still registered.
    store: Weak<RaftGroupFileLogStore>,
    changes: WatchSenderOf<UrsulaRaftTypeConfig, ()>,
    /// The committed leader whose AppendEntries reached this replica last,
    /// and when.
    last_leader_append: Mutex<
        Option<(
            UrsulaVote,
            openraft::type_config::alias::InstantOf<UrsulaRaftTypeConfig>,
        )>,
    >,
}

impl fmt::Debug for GroupRejoin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupRejoin")
            .field("node_id", &self.node_id)
            .field("raft_group_id", &self.raft_group_id)
            .field("bound", &self.metrics.get().is_some())
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

impl GroupRejoin {
    /// The gate of a replica, from the group's durable log state in `store`.
    /// Create it before the group's Raft core starts. A recovering replica
    /// that led the group records that it starts as a follower first; when
    /// that write fails, the group does not start.
    pub async fn durable(
        node_id: u64,
        raft_group_id: RaftGroupId,
        store: &Arc<RaftGroupFileLogStore>,
    ) -> Result<Self, RecoveryGateError> {
        store
            .prevent_crashed_leader_resume(node_id)
            .await
            .map_err(|source| RecoveryGateError::StartAsFollower {
                raft_group_id,
                source,
            })?;
        // A recovery plan's cut runs before the Raft core starts, so the
        // core never sees the entries it removes.
        if store
            .run_recovery_cut()
            .await
            .map_err(|source| RecoveryGateError::Cut {
                raft_group_id,
                source,
            })?
        {
            tracing::warn!(
                node_id,
                raft_group_id = raft_group_id.0,
                "recovery plan: this replica's log now ends at the last entry it knew was \
                 committed"
            );
        }
        let gate = match (store.recovery_record(), store.log_state()) {
            // M1 (#581 review): a member that may have lost entries keeps a
            // restricted vote: a plan's leader closes its gate, a member
            // stays planned, and its satisfied plan closes the gate too.
            (Some(record), GroupLogState::Recovering) => {
                tracing::warn!(
                    node_id,
                    raft_group_id = raft_group_id.0,
                    leader = record.leader,
                    "recovery gate: this replica holds a recovery plan and may be missing Raft \
                     log entries it acknowledged; it keeps its vote restricted"
                );
                store.start_as_follower(node_id).await.map_err(|source| {
                    RecoveryGateError::StartAsFollower {
                        raft_group_id,
                        source,
                    }
                })?;
                if record.leader == node_id {
                    VoteGate::closed(GroupEvidence::Initialized)
                } else {
                    VoteGate::planned(&record)
                }
            }
            // The plan holds across restarts until it is satisfied.
            (Some(record), _) if record.leader == node_id => VoteGate::Open,
            (Some(record), _) => VoteGate::planned(&record),
            (None, GroupLogState::Initialized) => VoteGate::Open,
            (None, GroupLogState::Recovering) => {
                tracing::warn!(
                    node_id,
                    raft_group_id = raft_group_id.0,
                    "recovery gate: this replica may be missing Raft log entries it \
                     acknowledged; it stays out of elections until it has applied a fresh \
                     leader barrier"
                );
                store.start_as_follower(node_id).await.map_err(|source| {
                    RecoveryGateError::StartAsFollower {
                        raft_group_id,
                        source,
                    }
                })?;
                VoteGate::closed(GroupEvidence::Initialized)
            }
            (None, GroupLogState::Empty) => {
                tracing::debug!(
                    node_id,
                    raft_group_id = raft_group_id.0,
                    "recovery gate: this replica holds nothing of the group yet; it joins only a \
                     new group's first election until it has applied a leader barrier"
                );
                store.hold_unknown_history();
                VoteGate::closed(GroupEvidence::Unknown)
            }
        };
        Ok(Self {
            node_id,
            raft_group_id,
            metrics: OnceLock::new(),
            gate: Mutex::new(gate),
            // Any durable vote is a floor, the genesis `(0, 0)` included:
            // granting a vote or acknowledging a leader persists a higher
            // vote first, so a durable `(0, 0)` proves neither happened.
            vote_floor: Mutex::new(store.vote()),
            reverted: Mutex::new(RevertedFollowers::default()),
            store: Arc::downgrade(store),
            changes: UrsulaRaftTypeConfig::watch_channel(()).0,
            last_leader_append: Mutex::new(None),
        })
    }

    pub fn raft_group_id(&self) -> RaftGroupId {
        self.raft_group_id
    }

    fn gate(&self) -> std::sync::MutexGuard<'_, VoteGate> {
        self.gate.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Attach the group's Raft metrics. Call once the Raft exists and before
    /// it is reachable from the network.
    pub(crate) fn bind(
        &self,
        raft: &RaftGroupHandle,
    ) -> Result<(), ursula_runtime::GroupEngineError> {
        let expected = raft.metrics().borrow_watched().id;
        if self.node_id != expected {
            return Err(ursula_runtime::GroupEngineError::Infra(
                ursula_runtime::GroupInfraError::RecoveryNodeMismatch {
                    raft_group_id: self.raft_group_id,
                    expected,
                    actual: self.node_id,
                },
            ));
        }
        self.metrics.set(raft.metrics()).map_err(|_already_bound| {
            ursula_runtime::GroupEngineError::Infra(
                ursula_runtime::GroupInfraError::RecoveryAlreadyBound {
                    raft_group_id: self.raft_group_id,
                },
            )
        })?;
        if !self.vote_gate_open() {
            raft.runtime_config().elect(false);
        }
        Ok(())
    }

    fn metrics(&self) -> Option<RaftMetrics<UrsulaRaftTypeConfig>> {
        self.metrics
            .get()
            .map(|metrics| metrics.borrow_watched().clone())
    }

    /// Whether the vote gate is open.
    pub fn vote_gate_open(&self) -> bool {
        self.gate().is_open() && !self.needs_vote_floor()
    }

    /// The gate as status reports show it.
    pub fn status(&self) -> RecoveryGateStatus {
        self.gate().status()
    }

    /// Whether this replica ever held the group's log, as far as it knows:
    /// a replica that did never runs `Initialize` again.
    pub fn holds_group_history(&self) -> bool {
        self.store
            .upgrade()
            .is_some_and(|store| store.log_state().is_initialized())
    }

    pub(crate) fn needs_vote_floor(&self) -> bool {
        self.vote_floor
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_none()
    }

    /// A replica that lost its vote must not help a stale leader prove a quorum.
    /// This includes empty heartbeats and snapshot acknowledgements.
    pub(crate) fn replication_allowed(&self, vote: UrsulaVote) -> bool {
        self.recovery_vote().is_some_and(|floor| vote >= floor)
    }

    /// The higher of the floor and the Raft core's current vote, read on
    /// every inbound AppendEntries and snapshot: only the vote is copied out
    /// of the metrics.
    pub(crate) fn recovery_vote(&self) -> Option<UrsulaVote> {
        let floor = (*self
            .vote_floor
            .lock()
            .unwrap_or_else(PoisonError::into_inner))?;
        let current = self
            .metrics
            .get()
            .map(|metrics| metrics.borrow_watched().vote);
        Some(match current {
            Some(vote) if vote > floor => vote,
            Some(_) | None => floor,
        })
    }

    pub(crate) async fn establish_vote_floor(
        &self,
        vote: UrsulaVote,
    ) -> Result<(), RecoveryGateError> {
        let store = self.store.upgrade().ok_or(RecoveryGateError::StoreClosed {
            raft_group_id: self.raft_group_id,
        })?;
        let vote = store.persist_recovery_vote(vote).await.map_err(|source| {
            RecoveryGateError::Record {
                raft_group_id: self.raft_group_id,
                source,
            }
        })?;
        let mut floor = self
            .vote_floor
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if floor.is_none_or(|current| vote > current) {
            *floor = Some(vote);
        }
        drop(floor);
        self.changes.send_if_modified(|()| true);
        Ok(())
    }

    pub(crate) fn confirm_barrier(&self, leader: UrsulaVote, commit_index: u64) {
        if self.replication_allowed(leader) {
            self.gate().confirm_barrier(leader, commit_index);
        }
    }

    /// Campaigning is subject to both this gate and the node's shed policy.
    pub(crate) fn may_campaign(&self) -> bool {
        self.vote_gate_open()
    }

    /// Opens the gate, recording it first.
    async fn open(&self, why: GateOpening) -> Result<(), RecoveryGateError> {
        let store = self.store.upgrade().ok_or(RecoveryGateError::StoreClosed {
            raft_group_id: self.raft_group_id,
        })?;
        if self.needs_vote_floor() && matches!(why, GateOpening::FreshBootstrap) {
            self.establish_vote_floor(self.metrics().map_or(UrsulaVote::new(0, 0), |m| m.vote))
                .await?;
        }
        store
            .record_recovered()
            .await
            .map_err(|source| RecoveryGateError::Record {
                raft_group_id: self.raft_group_id,
                source,
            })?;
        self.gate().open();
        self.changes.send_if_modified(|()| true);
        match why {
            GateOpening::CaughtUp => tracing::info!(
                node_id = self.node_id,
                raft_group_id = self.raft_group_id.0,
                "recovery gate opened: this replica applied a fresh leader barrier"
            ),
            GateOpening::FreshBootstrap => tracing::info!(
                node_id = self.node_id,
                raft_group_id = self.raft_group_id.0,
                "recovery gate opened: every voter reported an empty group"
            ),
            GateOpening::PlanLeader => tracing::warn!(
                node_id = self.node_id,
                raft_group_id = self.raft_group_id.0,
                "recovery gate opened by a recovery plan that elects this replica; it \
                 campaigns with the log it holds and starts a new history once elected"
            ),
        }
        Ok(())
    }

    /// Opens the gate once the replica has applied its barrier. Returns
    /// whether the gate is open.
    pub(crate) async fn try_open(&self) -> Result<bool, RecoveryGateError> {
        if self.needs_vote_floor() {
            return Ok(false);
        }
        let Some(metrics) = self.metrics() else {
            return Ok(self.vote_gate_open());
        };
        {
            let gate = self.gate();
            if gate.is_open() {
                return Ok(true);
            }
            if !gate.caught_up(local_replica(&metrics)) {
                return Ok(false);
            }
        }
        self.open(GateOpening::CaughtUp).await?;
        Ok(true)
    }

    /// Called only after every peer was proven empty: this replica
    /// initializes the group.
    pub(crate) async fn allow_fresh_bootstrap(&self) -> Result<(), RecoveryGateError> {
        if self.vote_gate_open() {
            return Ok(());
        }
        self.open(GateOpening::FreshBootstrap).await
    }

    /// The recovery plan this replica holds, from its log store.
    pub(crate) fn recovery_record(&self) -> Option<RecoveryRecord> {
        self.store.upgrade()?.recovery_record()
    }

    /// Whether a recovery cut closed this replica until its group restarts:
    /// it takes no replication.
    pub(crate) fn cutting(&self) -> bool {
        matches!(*self.gate(), VoteGate::Cutting)
    }

    /// Whether this replica follows `leader`, a committed leader whose
    /// AppendEntries reached it within [`LIVE_LEADER_QUORUM_ACK`].
    fn follows_live_leader(&self, metrics: &RaftMetrics<UrsulaRaftTypeConfig>) -> Option<u64> {
        let leader = metrics
            .current_leader
            .filter(|leader| *leader != self.node_id)?;
        let last = *self
            .last_leader_append
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let (vote, at) = last?;
        (*vote.leader_id().node_id() == leader
            && vote.is_committed()
            && at.elapsed() < LIVE_LEADER_QUORUM_ACK)
            .then_some(leader)
    }

    /// Takes a recovery plan (#581 section 4.5), as one owner job of its
    /// group; `disable_elections` stops the group's Raft core campaigning.
    /// Refused, with nothing changed, when the replica leads or follows a
    /// live leader, the plan's leader is not a voter, the replica no longer
    /// holds the log the tool read, it committed or applied above the plan's
    /// point, or it is the plan's leader and its log does not continue from
    /// the point. Otherwise the plan is recorded durably. Without a cut the
    /// gate opens on the plan's leader and opens for that leader only on
    /// every other member. With a cut the replica closes first
    /// ([`VoteGate::Cutting`]), and its group must restart
    /// ([`PlanTaken::Restart`]) to run the cut before the answer.
    pub(crate) async fn accept_recovery_plan(
        &self,
        plan: &RecoveryPlanRequest,
        disable_elections: impl FnOnce(),
    ) -> Result<PlanTaken, RecoveryGateError> {
        let raft_group_id = self.raft_group_id;
        if self.needs_vote_floor() {
            return Err(RecoveryGateError::MissingVoteFloor { raft_group_id });
        }
        let store = self
            .store
            .upgrade()
            .ok_or(RecoveryGateError::StoreClosed { raft_group_id })?;
        let metrics = self
            .metrics()
            .ok_or(RecoveryGateError::NotStarted { raft_group_id })?;
        // #577 N7 and M2 (#581 review): a plan never fences a group that
        // has a live leader, whether this replica leads it with a recent
        // quorum or follows it.
        if metrics.state == openraft::ServerState::Leader
            && metrics
                .last_quorum_acked
                .as_ref()
                .is_some_and(|acked| acked.elapsed() < LIVE_LEADER_QUORUM_ACK)
        {
            return Err(RecoveryGateError::FollowsLeader {
                raft_group_id,
                leader: self.node_id,
            });
        }
        if let Some(leader) = self.follows_live_leader(&metrics) {
            return Err(RecoveryGateError::FollowsLeader {
                raft_group_id,
                leader,
            });
        }
        if !metrics
            .membership_config
            .membership()
            .voter_ids()
            .any(|voter| voter == plan.leader)
        {
            return Err(RecoveryGateError::LeaderNotVoter {
                raft_group_id,
                leader: plan.leader,
            });
        }
        let status = self.status();
        if matches!(
            status,
            RecoveryGateStatus::AwaitingBarrier
                | RecoveryGateStatus::CatchingUp
                | RecoveryGateStatus::Cutting
        ) {
            return Err(RecoveryGateError::NotStalled {
                raft_group_id,
                status,
            });
        }
        let actual = ReplicaLog::of(&store);
        if actual != ReplicaLog::from(plan.expected) {
            return Err(RecoveryGateError::ReplicaChanged {
                raft_group_id,
                expected: plan.expected,
                actual: actual.into(),
            });
        }
        let committed_id = metrics.local_committed.or_else(|| store.committed());
        let committed = log_index(committed_id.as_ref());
        let applied = log_index(metrics.last_applied.as_ref());
        if committed.is_some_and(|index| index > plan.point)
            || applied.is_some_and(|index| index > plan.point)
        {
            return Err(RecoveryGateError::RecoveryPointBehind {
                raft_group_id,
                point: plan.point,
                committed,
                applied,
            });
        }
        // The plan's leader continues from the point: its last entry, or
        // with a cut its committed one; and the plan names its log.
        if plan.leader == self.node_id {
            if plan.leader_last_log_term != actual.last_log_term {
                return Err(RecoveryGateError::LeaderLogMismatch {
                    raft_group_id,
                    named: plan.leader_last_log_term,
                    held: actual.last_log_term,
                });
            }
            let (expected, basis) = if plan.cut {
                (committed.unwrap_or(0), "its committed index")
            } else {
                (actual.last_log_index.unwrap_or(0), "its last log index")
            };
            if plan.point != expected {
                return Err(RecoveryGateError::PointMismatch {
                    raft_group_id,
                    point: plan.point,
                    expected,
                    basis,
                });
            }
        }
        let cut_after = if plan.cut {
            let committed =
                committed_id.ok_or(RecoveryGateError::NothingCommitted { raft_group_id })?;
            let held = store.log_id_at(committed.index());
            if held != Some(committed) {
                return Err(RecoveryGateError::CommittedEntryMissing {
                    raft_group_id,
                    committed,
                    held,
                });
            }
            Some(committed)
        } else {
            None
        };
        let uncut_leader_term = if plan.cut {
            Some(
                plan.leader_last_log_term
                    .ok_or(RecoveryGateError::LeaderLogMismatch {
                        raft_group_id,
                        named: None,
                        held: None,
                    })?,
            )
        } else {
            None
        };
        let record = RecoveryRecord {
            leader: plan.leader,
            point: plan.point,
            uncut_leader_term,
            cut_after,
        };
        let restart = plan.cut
            && !ursula_stream::sim_controls::off(ursula_stream::sim_controls::Defence::RecoveryCut);
        let leads = plan.leader == self.node_id;
        if restart && leads {
            // The plan accepts the leader's log as the group's: its restart
            // opens its gate. A later crash records it recovering again.
            store
                .record_recovered()
                .await
                .map_err(|source| RecoveryGateError::Record {
                    raft_group_id,
                    source,
                })?;
        }
        if restart {
            // B1 (#581 review): the uncut log never votes, campaigns or
            // acknowledges an entry again: closed before the record names
            // the entry the cut keeps.
            *self.gate() = VoteGate::Cutting;
            disable_elections();
            self.changes.send_if_modified(|()| true);
        }
        store
            .record_recovery(Some(record))
            .await
            .map_err(|source| RecoveryGateError::Record {
                raft_group_id,
                source,
            })?;
        drop(store);
        let report = RecoveryPlanReport {
            raft_group_id: raft_group_id.0,
            node_id: self.node_id,
            outcome: if leads {
                RecoveryPlanOutcome::Leads
            } else {
                RecoveryPlanOutcome::Follows
            },
            leader: plan.leader,
            point: plan.point,
        };
        if restart {
            tracing::warn!(
                node_id = self.node_id,
                raft_group_id = raft_group_id.0,
                leader = plan.leader,
                point = plan.point,
                cut_after = ?cut_after,
                "recovery plan: this replica's group restarts to cut its log back to the last \
                 entry it knew was committed"
            );
            return Ok(PlanTaken::Restart(report));
        }
        if leads {
            self.open(GateOpening::PlanLeader).await?;
        } else {
            *self.gate() = VoteGate::planned(&record);
            self.changes.send_if_modified(|()| true);
            tracing::warn!(
                node_id = self.node_id,
                raft_group_id = raft_group_id.0,
                leader = plan.leader,
                point = plan.point,
                "recovery plan: this replica votes only for the plan's leader until the plan's \
                 history event applies"
            );
        }
        Ok(PlanTaken::Holds(report))
    }

    /// The history event of `record` applied here: the plan is satisfied.
    /// The record is removed, and a planned gate closes until a leader's
    /// barrier opens it (M1, #581 review): the event proves nothing about the
    /// entries this replica acknowledged after it. Runs outside OpenRaft's
    /// state-machine worker. A record that a newer plan replaced stays.
    pub(crate) async fn retire_recovery_plan(
        &self,
        record: RecoveryRecord,
    ) -> Result<(), RecoveryGateError> {
        let raft_group_id = self.raft_group_id;
        let store = self
            .store
            .upgrade()
            .ok_or(RecoveryGateError::StoreClosed { raft_group_id })?;
        if store.recovery_record() != Some(record) {
            return Ok(());
        }
        store
            .record_recovery(None)
            .await
            .map_err(|source| RecoveryGateError::Record {
                raft_group_id,
                source,
            })?;
        drop(store);
        let mut gate = self.gate();
        if matches!(*gate, VoteGate::Planned { .. }) {
            *gate = VoteGate::closed(GroupEvidence::Initialized);
            drop(gate);
            self.changes.send_if_modified(|()| true);
            tracing::info!(
                node_id = self.node_id,
                raft_group_id = raft_group_id.0,
                "recovery plan satisfied: this replica applied the plan's history event; it \
                 votes again once it has applied a fresh leader barrier"
            );
        }
        Ok(())
    }

    /// Returns whether the gate just stalled.
    fn stall(&self) -> bool {
        let stalled = self.gate().stall();
        if stalled && self.needs_vote_floor() {
            tracing::error!(
                node_id = self.node_id,
                raft_group_id = self.raft_group_id.0,
                "recovery stalled with lost vote history; a fresh quorum proof is required; accept-unsynced-loss cannot open this gate"
            );
        } else if stalled {
            tracing::error!(
                node_id = self.node_id,
                raft_group_id = self.raft_group_id.0,
                "recovery gate: no leader confirmed a barrier for this gated replica and it \
                 applied nothing; a majority of the group's voters may be gated, so the group \
                 has no leader and refuses writes. To accept the loss of writes acknowledged \
                 after the last fsync, send a recovery plan to every reachable member with \
                 POST /__ursula/raft/{}/recovery/accept-unsynced-loss",
                self.raft_group_id.0
            );
        }
        stalled
    }

    /// Follower side: screen an inbound vote request. `Some` is the refusal
    /// to send back instead of handing the request to OpenRaft.
    pub fn screen_vote(&self, request: &UrsulaVoteRequest) -> Option<UrsulaVoteResponse> {
        let metrics = self.metrics()?;
        let candidate = *request.vote.leader_id().node_id();
        let candidate_index = log_index(request.last_log_id.as_ref());
        let floor = self.recovery_vote();
        if request.vote == UrsulaVote::new(0, 0) && request.last_log_id.is_none() {
            return Some(UrsulaVoteResponse::new(
                floor.unwrap_or(metrics.vote),
                self.last_log_id().or(metrics.last_applied),
                floor.is_some(),
            ));
        }
        let last_applied = metrics.last_applied.as_ref().map(|log_id| log_id.index());
        match floor {
            None => tracing::info!(
                node_id = self.node_id,
                raft_group_id = self.raft_group_id.0,
                candidate,
                "recovery gate: refusing a vote until a quorum proves this replica's lost vote"
            ),
            // Any replica, gated or not, refuses a candidate whose vote is
            // older than its own; the answer carries the newer vote.
            Some(floor) if request.vote < floor => tracing::debug!(
                node_id = self.node_id,
                raft_group_id = self.raft_group_id.0,
                candidate,
                candidate_vote = %request.vote,
                vote = %floor,
                "refusing a vote request older than this replica's vote"
            ),
            Some(_) => match self.gate().screen(
                candidate,
                request
                    .last_log_id
                    .as_ref()
                    .map(|last| (last.index, last.leader_id.term)),
            ) {
                VoteScreen::Pass => return None,
                VoteScreen::Refuse => tracing::info!(
                    node_id = self.node_id,
                    raft_group_id = self.raft_group_id.0,
                    candidate,
                    candidate_last_log_index = ?candidate_index,
                    last_applied = ?last_applied,
                    "recovery gate: refusing a vote until this replica has caught up"
                ),
            },
        }
        Some(UrsulaVoteResponse::new(
            floor.unwrap_or(metrics.vote),
            self.last_log_id().or(metrics.last_applied),
            false,
        ))
    }

    /// The last entry this replica holds, which a refusal reports so that a
    /// bootstrap probe sees the group exists here.
    fn last_log_id(&self) -> Option<LogIdOf<UrsulaRaftTypeConfig>> {
        self.store.upgrade()?.last_log_id()
    }

    /// Follower side: record an inbound AppendEntries.
    pub fn observe_inbound_append(&self, request: &UrsulaAppendEntriesRequest) {
        self.gate()
            .observe_append(log_index(request.leader_commit.as_ref()));
        if request.vote.is_committed() {
            *self
                .last_leader_append
                .lock()
                .unwrap_or_else(PoisonError::into_inner) =
                Some((request.vote, UrsulaRaftTypeConfig::now()));
        }
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
        let rewind_pending = reverted.rewind_pending(target, leader, matched, confirmed);
        if !matches!(response, UrsulaAppendEntriesResponse::Conflict) {
            return false;
        }
        let Some(prev) = log_index(prev_log_id) else {
            return false;
        };
        if !matched.is_some_and(|matched| prev <= matched) {
            return false;
        }
        // A rewind is handed to OpenRaft only through the conflict of a request
        // that carried entries, so it ends the replication stream it belongs
        // to. A heartbeat's conflict would reset the progress under a stream
        // whose acknowledgements are still on their way, which OpenRaft does
        // not survive; it stays an error until then.
        if rewind_pending {
            return sent_last_log_id.is_none();
        }
        if reverted.leader.as_ref() != Some(leader) {
            reverted.leader = Some(*leader);
            reverted.targets.clear();
        }
        if reverted.targets.insert(target) {
            self.changes.send_if_modified(|()| true);
            tracing::warn!(
                node_id = self.node_id,
                raft_group_id = self.raft_group_id.0,
                target,
                conflict_index = prev,
                matched_index = ?matched,
                "recovery: follower lost Raft log entries it had acknowledged; rebuilding it"
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

    /// The rewinds this replica allowed under `leader`, by target, with the
    /// index each target had matched.
    fn allowed_rewinds(&self, leader: &UrsulaVote) -> BTreeMap<u64, u64> {
        self.reverted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .allowed(leader)
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

    /// Leader side: let OpenRaft rewind its progress for `targets`, so it
    /// replicates them from where their logs really end. OpenRaft's one-shot
    /// permission goes in first; only then do their conflicts reach it.
    async fn rewind_followers(
        &self,
        raft: &RaftGroupHandle,
        targets: &BTreeSet<u64>,
    ) -> Result<(), String> {
        let metrics = raft.metrics().borrow_watched().clone();
        for target in targets {
            raft.trigger()
                .allow_next_revert(target, true)
                .await
                .map_err(|err| format!("allow next revert for node {target}: {err}"))?
                .map_err(|err| format!("allow next revert for node {target}: {err}"))?;
        }
        let mut reverted = self.reverted.lock().unwrap_or_else(PoisonError::into_inner);
        if reverted.leader.as_ref() != Some(&metrics.vote) {
            reverted.leader = Some(metrics.vote);
            reverted.targets.clear();
            reverted.allowed_reverts.clear();
        }
        for target in targets {
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;

    use openraft::alias::LogIdOf;
    use openraft::vote::RaftLeaderId;

    use super::BootstrapDecision;
    use super::GateRecovery;
    use super::GroupEvidence;
    use super::HealStep;
    use super::HealView;
    use super::LocalReplica;
    use super::PeerGroupLog;
    use super::RecoveryGateStatus;
    use super::RevertedFollowers;
    use super::VoteGate;
    use super::VoteScreen;
    use super::bootstrap_decision;
    use super::plan_heal_step;
    use crate::types::UrsulaRaftTypeConfig;
    use crate::types::UrsulaVote;
    use crate::types::UrsulaVoteResponse;

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
    fn a_rewind_survives_read_index_conflicts_but_ends_after_repair() {
        let leader = leader(3, 1);
        let mut reverted = RevertedFollowers {
            leader: Some(leader),
            allowed_reverts: BTreeMap::from([(2, 20), (3, 20)]),
            ..Default::default()
        };
        // ReadIndex conflicts leave Raft's replication progress untouched.
        assert!(reverted.rewind_pending(2, &leader, Some(20), None));
        assert!(reverted.rewind_pending(2, &leader, Some(20), None));
        // A real rewind resets the progress; the allowance cannot cover
        // another loss even after this follower has caught up again.
        assert!(!reverted.rewind_pending(2, &leader, None, None));
        assert!(!reverted.rewind_pending(2, &leader, Some(30), None));
        // A fast catch-up can happen between metrics samples. Its successful
        // Append still proves that the prior prefix has been restored.
        assert!(reverted.rewind_pending(3, &leader, Some(20), Some(19)));
        assert!(!reverted.rewind_pending(3, &leader, Some(30), Some(20)));
        assert!(!reverted.rewind_pending(3, &leader, Some(30), None));
    }

    #[test]
    fn a_rewind_allowance_does_not_cross_a_leader_change() {
        let mut reverted = RevertedFollowers {
            leader: Some(leader(3, 1)),
            allowed_reverts: BTreeMap::from([(2, 20)]),
            ..Default::default()
        };
        assert!(!reverted.rewind_pending(2, &leader(4, 3), Some(20), None));
        assert!(reverted.allowed_reverts.is_empty());
    }

    #[test]
    fn an_unknown_history_gate_votes_in_a_new_groups_first_election() {
        let mut gate = VoteGate::closed(GroupEvidence::Unknown);
        // The initializer's candidate log is only the membership entry.
        assert_eq!(gate.screen(9, Some((0, 1))), VoteScreen::Pass);
        assert_eq!(gate.screen(9, None), VoteScreen::Pass);
        assert!(!gate.is_open(), "passing a first election opens nothing");
    }

    #[test]
    fn a_gated_replica_refuses_every_vote_once_the_group_holds_entries() {
        let mut gate = VoteGate::closed(GroupEvidence::Unknown);
        assert_eq!(gate.screen(9, Some((11, 1))), VoteScreen::Refuse);
        // Having seen entries, it refuses an index-0 candidate too: that can
        // only be another emptied replica.
        assert_eq!(gate.screen(9, Some((0, 1))), VoteScreen::Refuse);

        let mut gate = VoteGate::closed(GroupEvidence::Unknown);
        gate.observe_append(Some(1));
        assert_eq!(gate.screen(9, None), VoteScreen::Refuse);

        // A replica that lost its unsynced tail knows from the start.
        let mut gate = VoteGate::closed(GroupEvidence::Initialized);
        assert_eq!(gate.screen(9, Some((0, 1))), VoteScreen::Refuse);
        assert_eq!(gate.screen(9, None), VoteScreen::Refuse);
    }

    #[test]
    fn the_gate_opens_only_after_applying_a_fresh_barrier() {
        let mut gate = VoteGate::closed(GroupEvidence::Initialized);
        assert_eq!(gate.status(), RecoveryGateStatus::AwaitingBarrier);
        gate.observe_append(Some(11));
        assert!(
            !gate.caught_up(follower(Some(11))),
            "inbound replication is no proof"
        );
        gate.confirm_barrier(leader(2, 1), 11);
        assert_eq!(gate.status(), RecoveryGateStatus::CatchingUp);
        assert!(!gate.caught_up(follower(Some(10))));
        assert!(gate.caught_up(follower(Some(11))));
        // Catching up never opens the gate by itself: the driver records it
        // first, so screening keeps refusing until then.
        assert_eq!(gate.screen(9, Some((12, 1))), VoteScreen::Refuse);
        gate.open();
        assert!(gate.is_open());
        assert_eq!(gate.status(), RecoveryGateStatus::Open);
        assert_eq!(gate.screen(9, Some((12, 1))), VoteScreen::Pass);
        // An open gate stays open and ignores later evidence.
        gate.confirm_barrier(leader(3, 2), 40);
        gate.observe_append(Some(40));
        assert!(!gate.stall());
        assert!(gate.is_open());
    }

    #[test]
    fn the_catch_up_point_follows_the_highest_leader() {
        let mut gate = VoteGate::closed(GroupEvidence::Unknown);
        gate.confirm_barrier(leader(2, 1), 5);
        // A deposed leader's lower commit does not lower the target.
        gate.confirm_barrier(leader(1, 3), 1);
        gate.confirm_barrier(leader(3, 2), 9);
        assert!(!gate.caught_up(follower(Some(5))));
        assert!(gate.caught_up(follower(Some(9))));
        // The same leader's later barrier only raises the target.
        gate.confirm_barrier(leader(3, 2), 7);
        assert!(gate.caught_up(follower(Some(9))));
        gate.confirm_barrier(leader(3, 2), 12);
        assert!(!gate.caught_up(follower(Some(9))));
    }

    #[test]
    fn uncommitted_proofs_never_count() {
        let mut gate = VoteGate::closed(GroupEvidence::Unknown);
        gate.observe_append(None);
        gate.confirm_barrier(vote(3, 1), 20);
        assert_eq!(gate.status(), RecoveryGateStatus::AwaitingBarrier);
        assert!(!gate.caught_up(follower(Some(20))));
    }

    #[test]
    fn a_gate_without_progress_stalls_until_a_barrier_arrives() {
        let mut gate = VoteGate::closed(GroupEvidence::Initialized);
        assert!(gate.stall());
        assert!(!gate.stall(), "it stalls once");
        assert_eq!(gate.status(), RecoveryGateStatus::Stalled);
        gate.confirm_barrier(leader(2, 1), 3);
        assert_eq!(gate.status(), RecoveryGateStatus::CatchingUp);
        // The barrier led nowhere for too long: its target is dropped, and a
        // new barrier is needed.
        assert!(gate.stall());
        assert_eq!(gate, VoteGate::Closed {
            group: GroupEvidence::Initialized,
            recovery: GateRecovery::Stalled,
        });
        assert!(!gate.caught_up(follower(Some(3))));
        gate.confirm_barrier(leader(2, 1), 3);
        assert!(gate.caught_up(follower(Some(3))));
        gate.open();
        assert!(!gate.stall(), "an open gate never stalls");
    }

    /// A recovery plan opens a replica's vote for the plan's leader only
    /// (#577 B3), and the replica does not campaign or stall meanwhile.
    #[test]
    fn a_planned_gate_votes_only_for_the_plans_leader() {
        let planned = VoteGate::Planned {
            leader: 2,
            point: 10,
            uncut_leader_term: None,
        };
        let mut gate = planned.clone();
        assert_eq!(gate.status(), RecoveryGateStatus::Planned);
        assert!(!gate.is_open(), "a planned replica does not campaign");
        assert_eq!(gate.screen(2, Some((40, 1))), VoteScreen::Pass);
        assert_eq!(gate.screen(2, None), VoteScreen::Pass);
        assert_eq!(gate.screen(3, Some((40, 1))), VoteScreen::Refuse);
        assert_eq!(gate.screen(3, None), VoteScreen::Refuse);
        // Barriers, replication and time change nothing: only the plan's
        // history event closes it.
        gate.observe_append(Some(41));
        gate.confirm_barrier(leader(5, 3), 41);
        assert!(!gate.stall());
        assert!(!gate.caught_up(follower(Some(41))));
        assert_eq!(gate, planned);
    }

    /// B1 (#581 review): a member of a cutting plan never elects the plan's
    /// leader while that leader holds its uncut stale suffix.
    #[test]
    fn a_cutting_plans_member_votes_for_its_leader_only_once_the_leader_was_cut() {
        let mut gate = VoteGate::Planned {
            leader: 2,
            point: 10,
            uncut_leader_term: Some(5),
        };
        // The uncut leader's log runs past the point in its old term:
        // refused.
        assert_eq!(gate.screen(2, Some((12, 5))), VoteScreen::Refuse);
        // Cut back to the point: granted.
        assert_eq!(gate.screen(2, Some((10, 4))), VoteScreen::Pass);
        assert_eq!(gate.screen(2, Some((8, 4))), VoteScreen::Pass);
        // Past the point in a later term: entries it appended as leader after
        // the cut, granted again after its restart.
        assert_eq!(gate.screen(2, Some((11, 9))), VoteScreen::Pass);
        assert_eq!(gate.screen(3, Some((10, 4))), VoteScreen::Refuse);
    }

    #[test]
    fn a_cutting_gate_votes_for_no_one_and_never_campaigns() {
        let mut gate = VoteGate::Cutting;
        assert_eq!(gate.status(), RecoveryGateStatus::Cutting);
        assert!(!gate.is_open());
        assert_eq!(gate.screen(2, Some((0, 1))), VoteScreen::Refuse);
        assert_eq!(gate.screen(2, None), VoteScreen::Refuse);
        assert!(!gate.stall());
        assert!(!gate.caught_up(follower(Some(100))));
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
        // An empty voter must finish its durable floor before initialization.
        assert_eq!(
            bootstrap_decision(&[empty, Some(PeerGroupLog::Unprepared)]),
            BootstrapDecision::Wait
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
        let empty = UrsulaVoteResponse::new(vote(0, 0), None, true);
        assert_eq!(
            PeerGroupLog::from_vote_response(&empty),
            PeerGroupLog::Empty
        );
        let candidate = UrsulaVoteResponse::new(vote(1, 2), None, false);
        assert_eq!(
            PeerGroupLog::from_vote_response(&candidate),
            PeerGroupLog::Initialized
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
            answering: voters.iter().copied().collect(),
            reverted: reverted.iter().copied().collect(),
            awaiting_rewind: BTreeSet::new(),
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
    fn the_heal_driver_removes_a_voter_only_with_a_quorum_that_answered() {
        // 3 lost entries and 2 has not answered since it restarted: 2 may
        // have lost entries too, so a removal could need it and never
        // commit. Rewind 3 instead.
        let mut silent = view(&[1, 2, 3], &[], &[3]);
        silent.answering = BTreeSet::from([1]);
        assert_eq!(
            plan_heal_step(&silent),
            Some(HealStep::RewindVoters {
                targets: BTreeSet::from([3]),
            })
        );
        // Once 2 answers, it holds its log and the removal can commit.
        silent.answering.insert(2);
        assert_eq!(
            plan_heal_step(&silent),
            Some(HealStep::RemoveVoter {
                target: 3,
                voters: BTreeSet::from([1, 2]),
            })
        );
    }

    #[test]
    fn the_heal_driver_rewinds_a_majority_that_lost_entries_and_needs_leadership() {
        // A majority lost entries: no removal could commit, so this leader
        // replicates its log to them again.
        assert_eq!(
            plan_heal_step(&view(&[1, 2, 3], &[], &[2, 3])),
            Some(HealStep::RewindVoters {
                targets: BTreeSet::from([2, 3]),
            })
        );
        // Until OpenRaft rewinds them, an idle group is given an entry to
        // replicate.
        let mut rewinding = view(&[1, 2, 3], &[], &[]);
        rewinding.awaiting_rewind = BTreeSet::from([2, 3]);
        assert_eq!(plan_heal_step(&rewinding), Some(HealStep::ReplicateRewound));
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
        // Three of five lost entries: rewind all three.
        assert_eq!(
            plan_heal_step(&five(&[1, 2, 3, 4, 5], &[], &[3, 4, 5])),
            Some(HealStep::RewindVoters {
                targets: BTreeSet::from([3, 4, 5]),
            })
        );
    }

    /// A gate follows the group's durable log state: closed with an
    /// unknown history while the replica holds nothing, open once the group
    /// is initialized, closed again when a run starts without knowing how
    /// the previous one ended. A recovering replica that led the group starts
    /// as a follower, and an operator's acceptance is durable.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn a_durable_gate_follows_the_group_log_state_across_restarts() {
        use openraft::entry::RaftEntry;
        use openraft::storage::IOFlushed;
        use openraft::storage::RaftLogReader;
        use openraft::storage::RaftLogStorage;
        use ursula_config::WalFsync;
        use ursula_proto::admin::RecoveryPlanRequest;
        use ursula_proto::admin::ReplicaLogView;
        use ursula_shard::CoreId;
        use ursula_shard::RaftGroupId;
        use ursula_shard::ShardId;
        use ursula_shard::ShardPlacement;

        use super::GroupRejoin;
        use super::RecoveryGateError;
        use crate::RaftWal;
        use crate::log_store::GroupLogState;
        use crate::log_store::RUN_STATE_FILE;
        use crate::log_store::RaftGroupFileLogStore;
        use crate::log_store::RecoveryRecord;

        let dir = tempfile::tempdir().expect("temp dir");
        let placement = ShardPlacement {
            core_id: CoreId(0),
            shard_id: ShardId(0),
            raft_group_id: RaftGroupId(0),
        };
        let metrics = ursula_runtime::RuntimeMetrics::new(1, 1).group_engine_metrics();
        let open = |wal: &RaftWal| -> std::sync::Arc<RaftGroupFileLogStore> {
            wal.open(placement, metrics.clone())
                .expect("open the store")
        };
        let entry = |index| {
            openraft::alias::EntryOf::<UrsulaRaftTypeConfig>::new(
                log_id(1, 1, index),
                openraft::EntryPayload::Blank,
            )
        };

        // A new replica: its history is unknown, so the gate is closed and
        // the replica may bootstrap the group.
        let wal = RaftWal::start(
            dir.path(),
            WalFsync::Never,
            &ursula_shard::StaticShardMap::new(1, 1).expect("valid topology"),
        )
        .expect("start");
        let mut store = open(&wal);
        let gate = GroupRejoin::durable(1, placement.raft_group_id, &store)
            .await
            .expect("open the gate");
        assert_eq!(gate.status(), RecoveryGateStatus::AwaitingBarrier);
        assert!(!gate.may_campaign());
        assert!(!gate.holds_group_history());
        assert!(gate.stall());
        let plan =
            |leader, point, cut, last_log_index, last_log_term, current_term| RecoveryPlanRequest {
                leader,
                point,
                cut,
                leader_last_log_term: last_log_term,
                expected: ReplicaLogView {
                    last_log_index,
                    last_log_term,
                    current_term,
                },
            };
        let missing_floor = gate
            .accept_recovery_plan(&plan(1, 0, false, None, None, 0), || {})
            .await
            .expect_err("accepting lost entries cannot replace a lost vote history");
        assert!(matches!(
            missing_floor,
            RecoveryGateError::MissingVoteFloor {
                raft_group_id: RaftGroupId(0)
            }
        ));
        assert!(!gate.replication_allowed(leader(1, 2)));
        assert_eq!(store.read_vote().await.expect("read unchanged vote"), None);
        // Every voter reported an empty group: the gate opens and the first
        // entry records the group initialized.
        gate.allow_fresh_bootstrap().await.expect("fresh bootstrap");
        assert!(gate.vote_gate_open());
        store.save_vote(&leader(1, 1)).await.expect("vote");
        let (flushed, result) =
            <UrsulaRaftTypeConfig as openraft::type_config::TypeConfigExt>::oneshot();
        store
            .append([entry(1)], IOFlushed::signal(flushed))
            .await
            .expect("submit append");
        result.await.expect("flush callback").expect("append");
        assert_eq!(store.log_state(), GroupLogState::Initialized);
        assert!(gate.holds_group_history());
        drop((gate, store));
        wal.shutdown().await.expect("clean shutdown");

        // A clean restart: the replica holds every entry it acknowledged.
        let wal = RaftWal::start(
            dir.path(),
            WalFsync::Never,
            &ursula_shard::StaticShardMap::new(1, 1).expect("valid topology"),
        )
        .expect("start");
        let store = open(&wal);
        assert!(
            GroupRejoin::durable(1, placement.raft_group_id, &store)
                .await
                .expect("open the gate")
                .vote_gate_open()
        );
        drop(store);
        wal.shutdown().await.expect("clean shutdown");

        // The run state is gone while the journal holds records: the group
        // recovers. It refuses every vote, and the replica that led it starts
        // as a follower.
        std::fs::remove_file(dir.path().join(RUN_STATE_FILE)).expect("remove the run state");
        let wal = RaftWal::start(
            dir.path(),
            WalFsync::Never,
            &ursula_shard::StaticShardMap::new(1, 1).expect("valid topology"),
        )
        .expect("start");
        let mut store = open(&wal);
        assert_eq!(store.log_state(), GroupLogState::Recovering);
        let gate = GroupRejoin::durable(1, placement.raft_group_id, &store)
            .await
            .expect("open the gate");
        assert_eq!(gate.status(), RecoveryGateStatus::AwaitingBarrier);
        assert!(gate.holds_group_history(), "it never initializes again");
        assert_eq!(gate.gate().screen(9, Some((0, 1))), VoteScreen::Refuse);
        // Its refusals report the log it holds, so a bootstrap probe sees
        // that the group exists here.
        assert_eq!(gate.last_log_id(), Some(log_id(1, 1, 1)));
        assert_eq!(
            PeerGroupLog::from_vote_response(&UrsulaVoteResponse::new(
                vote(1, 1),
                gate.last_log_id(),
                false
            )),
            PeerGroupLog::Initialized
        );
        assert_eq!(
            store.read_vote().await.expect("vote"),
            Some(vote(1, 1)),
            "its vote for itself is no longer a committed leadership"
        );
        assert!(gate.stall());
        drop((gate, store));
        wal.shutdown().await.expect("clean shutdown");

        // A plan electing another member holds across restarts: the replica
        // votes for that leader only. (The accept job's refusals run against
        // a started group, in `tests.rs` and the simulation.)
        let record = RecoveryRecord {
            leader: 2,
            point: 1,
            uncut_leader_term: None,
            cut_after: None,
        };
        let wal = RaftWal::start(
            dir.path(),
            WalFsync::Never,
            &ursula_shard::StaticShardMap::new(1, 1).expect("valid topology"),
        )
        .expect("start");
        let store = open(&wal);
        store
            .record_recovery(Some(record))
            .await
            .expect("record the plan");
        drop(store);
        wal.shutdown().await.expect("clean shutdown");
        let wal = RaftWal::start(
            dir.path(),
            WalFsync::Never,
            &ursula_shard::StaticShardMap::new(1, 1).expect("valid topology"),
        )
        .expect("start");
        let store = open(&wal);
        // M1 (#581 review): the log is still recovering, and the plan keeps
        // the vote restricted to its leader.
        assert_eq!(store.log_state(), GroupLogState::Recovering);
        let gate = GroupRejoin::durable(1, placement.raft_group_id, &store)
            .await
            .expect("open the gate");
        assert_eq!(gate.status(), RecoveryGateStatus::Planned);
        assert_eq!(gate.gate().screen(2, Some((1, 1))), VoteScreen::Pass);
        assert_eq!(gate.gate().screen(3, Some((9, 1))), VoteScreen::Refuse);
        assert!(!gate.may_campaign());
        // Satisfied: the record goes, and the gate closes until a leader's
        // barrier opens it. The event proves nothing about entries this
        // replica acknowledged after it.
        gate.retire_recovery_plan(record)
            .await
            .expect("retire the plan");
        assert_eq!(store.recovery_record(), None);
        assert_eq!(gate.status(), RecoveryGateStatus::AwaitingBarrier);
        assert!(!gate.vote_gate_open());
        assert_eq!(gate.gate().screen(2, Some((1, 1))), VoteScreen::Refuse);
        drop((gate, store));
        wal.shutdown().await.expect("clean shutdown");

        // A plan electing this replica, on a recovering log: it keeps its
        // vote restricted and does not campaign; a new plan must elect
        // another member.
        let wal = RaftWal::start(
            dir.path(),
            WalFsync::Never,
            &ursula_shard::StaticShardMap::new(1, 1).expect("valid topology"),
        )
        .expect("start");
        let store = open(&wal);
        store
            .record_recovery(Some(RecoveryRecord {
                leader: 1,
                ..record
            }))
            .await
            .expect("record the plan");
        let gate = GroupRejoin::durable(1, placement.raft_group_id, &store)
            .await
            .expect("open the gate");
        assert_eq!(gate.status(), RecoveryGateStatus::AwaitingBarrier);
        assert!(!gate.may_campaign());
        store.record_recovery(None).await.expect("drop the plan");
        // The barrier this replica now needs opens it as before.
        gate.gate().confirm_barrier(leader(2, 2), 1);
        gate.gate().open();
        store
            .record_recovered()
            .await
            .expect("record the open gate");
        drop((gate, store));
        wal.shutdown().await.expect("clean shutdown");

        // The replica that led the group restarts with its gate open and its
        // vote for itself uncommitted: it does not restore that leadership.
        let wal = RaftWal::start(
            dir.path(),
            WalFsync::Never,
            &ursula_shard::StaticShardMap::new(1, 1).expect("valid topology"),
        )
        .expect("start");
        let mut store = open(&wal);
        let gate = GroupRejoin::durable(1, placement.raft_group_id, &store)
            .await
            .expect("open the gate");
        assert!(gate.vote_gate_open());
        assert_eq!(store.read_vote().await.expect("vote"), Some(vote(1, 1)));
        drop((gate, store));
        drop(wal);

        // A crash of the next run (no clean shutdown) on the same host keeps
        // the open gate.
        let wal = RaftWal::start(
            dir.path(),
            WalFsync::Always,
            &ursula_shard::StaticShardMap::new(1, 1).expect("valid topology"),
        )
        .expect("start");
        let store = open(&wal);
        let gate = GroupRejoin::durable(1, placement.raft_group_id, &store)
            .await
            .expect("open the gate");
        assert_eq!(
            gate.vote_gate_open(),
            cfg!(target_os = "linux"),
            "without a boot id an unclean end reads as a host crash under never"
        );
        drop((gate, store));
        wal.shutdown().await.expect("clean shutdown");
    }

    #[test]
    fn the_heal_driver_restores_lost_voters_before_finishing_a_joint_config() {
        let mut joint = view(&[1, 2, 3], &[], &[2, 3]);
        joint.uniform = false;
        for stale in [false, true] {
            joint.stale_joint = stale;
            assert_eq!(
                plan_heal_step(&joint),
                Some(HealStep::RewindVoters {
                    targets: BTreeSet::from([2, 3]),
                })
            );
        }
        joint.reverted.clear();
        assert_eq!(plan_heal_step(&joint), Some(HealStep::FinishJoint));
    }

    #[test]
    fn the_heal_driver_flattens_only_a_joint_config_nothing_else_finishes() {
        let mut joint = view(&[1, 2, 3], &[], &[]);
        joint.uniform = false;
        // Just entered: OpenRaft's own second step is still on its way.
        assert_eq!(plan_heal_step(&joint), None);
        joint.stale_joint = true;
        assert_eq!(plan_heal_step(&joint), Some(HealStep::FinishJoint));
        joint.is_leader = false;
        assert_eq!(plan_heal_step(&joint), None);
    }
}

mod attach;
mod barrier;
mod bootstrap;
mod heal;
pub use attach::RecoveryConfig;
pub use attach::RecoveryGate;
pub use attach::RecoveryTransport;
pub use barrier::run_rejoin_vote_barrier;
#[cfg(test)]
use bootstrap::BootstrapDecision;
pub use bootstrap::GroupBootstrap;
pub use bootstrap::PeerGroupLog;
#[cfg(test)]
use bootstrap::bootstrap_decision;
pub use bootstrap::bootstrap_probe_vote;
pub use bootstrap::run_group_bootstrap;
#[cfg(test)]
use heal::HealStep;
#[cfg(test)]
use heal::HealView;
#[cfg(test)]
use heal::plan_heal_step;
pub use heal::run_rejoin_heal;
