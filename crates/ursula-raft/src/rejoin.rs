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
//! - **Rewind in place**: a follower that lost entries answers `Conflict`
//!   at or below the index its leader saw it match in this leadership. A
//!   gated group runs OpenRaft with log reversion allowed
//!   (`engine::config_for_recovery`), so the leader resets its progress for
//!   that follower on the first such conflict, a heartbeat's included, and
//!   replicates its log to it again. The follower stays a voter throughout,
//!   and the conflict still counts as its acknowledgement of the leader, so
//!   the leader keeps its quorum. That is safe because the leader holds
//!   every committed entry and the follower votes for no one until its gate
//!   opens. Every leader-side membership driver runs on one leader tick
//!   (`leader_tick`), which observes each reversion in the replication
//!   metrics, counts it per follower and logs it. The heal driver
//!   (`run_rejoin_heal`) brings the membership back to the configured
//!   voters when an operator or a recovery plan left one out.
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
//! plan's leader its vote only once the leader shows it was cut. Taking a
//! plan accepts the member's log; a member that crashed after that and
//! lost entries keeps its vote restricted through restarts, and its
//! satisfied plan closes its gate until a leader's barrier opens it.

use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::PoisonError;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use openraft::Instant as _;
use openraft::Raft;
use openraft::RaftMetrics;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use openraft::rt::WatchReceiver;
use openraft::rt::WatchSender;
use openraft::storage::RaftStateMachine;
use openraft::type_config::TypeConfigExt;
use openraft::type_config::alias::WatchReceiverOf;
use openraft::type_config::alias::WatchSenderOf;
use openraft::vote::RaftLeaderId;
use ursula_proto::admin::PlanStep;
use ursula_proto::admin::RecoveryPlanOutcome;
use ursula_proto::admin::RecoveryPlanReport;
use ursula_proto::admin::RecoveryPlanRequest;
use ursula_proto::admin::ReplicaLogView;
use ursula_shard::RaftGroupId;
use ursula_shard::RaftTarget;

use crate::log_store::CoreJournalError;
use crate::log_store::GroupLogState;
use crate::log_store::PlanCut;
use crate::log_store::RaftGroupFileLogStore;
use crate::log_store::RecoveryRecord;
use crate::target::WireTarget;
use crate::types::UrsulaAppendEntriesRequest;
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

/// Whether the replica whose metrics are `metrics` leads with a quorum that
/// acknowledged it within [`LIVE_LEADER_QUORUM_ACK`]. A leader that lost its
/// quorum stays in OpenRaft's leader state.
pub fn leads_live(metrics: &RaftMetrics<UrsulaRaftTypeConfig>) -> bool {
    metrics.state == openraft::ServerState::Leader
        && metrics
            .last_quorum_acked
            .as_ref()
            .is_some_and(|acked| acked.elapsed() < LIVE_LEADER_QUORUM_ACK)
}

/// The membership a replica keeps when a recovery plan cuts its log back to
/// its committed entry (#613): the newest membership entry at or below its
/// committed index. A restart rebuilds the membership from the log that is
/// left and the state machine. `log` is the newest such entry the log still
/// holds; `applied` is the state machine's membership, which covers every
/// purged entry. OpenRaft's own committed membership is only the
/// second-to-last membership entry in the log, which can be above this
/// replica's commit marker: the marker is never synced on its own.
pub(crate) fn kept_of(
    log: Option<openraft::alias::StoredMembershipOf<UrsulaRaftTypeConfig>>,
    applied: openraft::alias::StoredMembershipOf<UrsulaRaftTypeConfig>,
) -> openraft::alias::StoredMembershipOf<UrsulaRaftTypeConfig> {
    match log {
        Some(log) if log.log_id() > applied.log_id() => log,
        Some(_) | None => applied,
    }
}

/// The membership a replica keeps after a cut back to `committed`: through
/// its recovery gate's log store ([`GroupRejoin::kept_membership`]), or the
/// state machine's `applied` membership alone when it has no gate. The one
/// computation both the metrics and the simulation read.
pub(crate) fn kept_membership_of(
    rejoin: Option<&GroupRejoin>,
    committed: Option<u64>,
    applied: openraft::alias::StoredMembershipOf<UrsulaRaftTypeConfig>,
) -> openraft::alias::StoredMembershipOf<UrsulaRaftTypeConfig> {
    match rejoin {
        Some(rejoin) => rejoin.kept_membership(committed, applied),
        None => applied,
    }
}

/// The log id a cutting plan's leader keeps when the plan names the entry
/// at its point ([`RecoveryPlanRequest::point_entry`]).
fn point_log_id(plan: &RecoveryPlanRequest) -> Option<LogIdOf<UrsulaRaftTypeConfig>> {
    plan.point_entry.map(|entry| openraft::LogId {
        leader_id: openraft::vote::leader_id_adv::CommittedLeaderId::new(entry.term, entry.node_id),
        index: plan.point,
    })
}

/// What reaches the commit screen ([`GroupRejoin::committed_overwrite`]).
pub(crate) enum Incoming<'a> {
    Append(&'a openraft::raft::AppendEntriesRequest<UrsulaRaftTypeConfig>),
    Snapshot {
        last: Option<LogIdOf<UrsulaRaftTypeConfig>>,
        /// The history event the snapshot's state holds.
        history: Option<ursula_runtime::HistoryEvent>,
        /// The id of the history this replica's commit marker lies in
        /// ([`GroupRejoin::history_at_marker`]), 0 before the genesis;
        /// `None` when its log could not be read.
        local: Option<u64>,
    },
}

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
/// leader's committed vote and the barrier index the replica must apply,
/// the higher of the read index and the leader's last log index
/// (`registry::confirm_recovery_barrier`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatchUpTarget {
    leader: UrsulaVote,
    barrier_index: u64,
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
    /// A recovery plan elects `leader`: the replica votes only for it, and
    /// does not campaign, until the plan's history event applies here.
    /// `leader_term` is the plan term: no lower than the vote term of every
    /// replica the tool reached, the plan's members included, and of the
    /// leader when it took the plan. The vote goes to the leader
    /// only in an election above the plan term, one it started after it
    /// took the plan (N1, #581 review), and only while its log holds no
    /// entry above `point` at a term at or below the plan term: such an
    /// entry came from a leader elected before the plan (M1). For the same
    /// reason the replica takes replication only from a leader above the
    /// plan term. `stalled` once nothing applied here for the stall timeout
    /// since the plan was taken: the plan is wedged and the operator plans
    /// again.
    Planned {
        leader: u64,
        point: u64,
        leader_term: u64,
        stalled: bool,
    },
    /// A recovery plan elects this replica: it campaigns but votes for no
    /// other candidate, takes replication only from a leader above the plan
    /// term `leader_term`, and serves nothing until its history event
    /// applies.
    LeadsPlan {
        point: u64,
        leader_term: u64,
        stalled: bool,
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

    /// Whether the replica may campaign: an open gate, or the plan's leader.
    pub(crate) fn campaigns(&self) -> bool {
        matches!(self, Self::Open | Self::LeadsPlan { .. })
    }

    pub(crate) fn status(&self) -> RecoveryGateStatus {
        match self {
            Self::Open => RecoveryGateStatus::Open,
            Self::Closed { recovery, .. } => match recovery {
                GateRecovery::AwaitingBarrier => RecoveryGateStatus::AwaitingBarrier,
                GateRecovery::Stalled => RecoveryGateStatus::Stalled,
                GateRecovery::CatchingUp(_) => RecoveryGateStatus::CatchingUp,
            },
            Self::Planned { stalled: true, .. } | Self::LeadsPlan { stalled: true, .. } => {
                RecoveryGateStatus::Stalled
            }
            Self::Planned { .. } => RecoveryGateStatus::Planned,
            Self::LeadsPlan { .. } => RecoveryGateStatus::LeadsPlan,
            Self::Cutting => RecoveryGateStatus::Cutting,
        }
    }

    /// The gate of a replica that holds `record`, whose node is `node_id`.
    pub(crate) fn planned(record: &RecoveryRecord, node_id: u64) -> Self {
        if record.leader == node_id {
            Self::LeadsPlan {
                point: record.point,
                leader_term: record.leader_term,
                stalled: false,
            }
        } else {
            Self::Planned {
                leader: record.leader,
                point: record.point,
                leader_term: record.leader_term,
                stalled: false,
            }
        }
    }

    /// Whether this gate holds the plan `record` (m7, #581 review).
    pub(crate) fn holds(&self, record: &RecoveryRecord) -> bool {
        match self {
            Self::Planned {
                leader,
                point,
                leader_term,
                ..
            } => {
                *leader == record.leader
                    && *point == record.point
                    && *leader_term == record.leader_term
            }
            Self::LeadsPlan {
                point, leader_term, ..
            } => *point == record.point && *leader_term == record.leader_term,
            Self::Open | Self::Closed { .. } | Self::Cutting => false,
        }
    }

    /// Whether the replica takes replication from a leader at `term`: not
    /// while it restarts for a plan, and, while it holds a plan, only from
    /// a leader above the plan term (M1, #581 review). Every leader elected
    /// before the plan is at or below it, and every leader elected under it
    /// above.
    pub(crate) fn takes_replication_at(&self, term: u64) -> bool {
        match self {
            Self::Cutting => false,
            Self::Planned { leader_term, .. } | Self::LeadsPlan { leader_term, .. } => {
                term > *leader_term
                    || ursula_stream::sim_controls::off(
                        ursula_stream::sim_controls::Defence::PlanReplicationFence,
                    )
            }
            Self::Open | Self::Closed { .. } => true,
        }
    }

    /// Record an inbound AppendEntries that carried `leader_commit`.
    pub(crate) fn observe_append(&mut self, leader_commit: Option<u64>) {
        match self {
            Self::Closed { group, .. } if leader_commit.is_some_and(|index| index >= 1) => {
                *group = GroupEvidence::Initialized;
            }
            Self::Open
            | Self::Closed { .. }
            | Self::Planned { .. }
            | Self::LeadsPlan { .. }
            | Self::Cutting => {}
        }
    }

    /// Only this process's outbound recovery probe may establish a catch-up
    /// target. Inbound replication may have been delayed across the restart.
    /// `barrier_index` is the index the barrier names ([`CatchUpTarget`]).
    pub(crate) fn confirm_barrier(&mut self, leader: UrsulaVote, barrier_index: u64) {
        let Self::Closed { group, recovery } = self else {
            return;
        };
        if !leader.is_committed() {
            return;
        }
        if barrier_index >= 1 {
            *group = GroupEvidence::Initialized;
        }
        match recovery {
            GateRecovery::CatchingUp(target) if target.leader == leader => {
                target.barrier_index = target.barrier_index.max(barrier_index);
            }
            // A deposed (or incomparable) leader does not move the target.
            GateRecovery::CatchingUp(target)
                if leader.partial_cmp(&target.leader) != Some(std::cmp::Ordering::Greater) => {}
            GateRecovery::AwaitingBarrier | GateRecovery::Stalled | GateRecovery::CatchingUp(_) => {
                *recovery = GateRecovery::CatchingUp(CatchUpTarget {
                    leader,
                    barrier_index,
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
            Self::Open
            | Self::Closed { .. }
            | Self::Planned { .. }
            | Self::LeadsPlan { .. }
            | Self::Cutting => false,
        }
    }

    /// m6 (#581 review): the replica holds a plan under which nothing
    /// applied for the stall timeout since it took it, so the plan is
    /// wedged and the operator plans again. Its votes are unchanged.
    /// Returns whether the gate just stalled.
    pub(crate) fn stall_plan(&mut self) -> bool {
        match self {
            Self::Planned { stalled, .. } | Self::LeadsPlan { stalled, .. } if !*stalled => {
                *stalled = true;
                true
            }
            Self::Open
            | Self::Closed { .. }
            | Self::Planned { .. }
            | Self::LeadsPlan { .. }
            | Self::Cutting => false,
        }
    }

    /// Something applied under the plan: it is not wedged (m9, #581
    /// review). Only applied progress clears a plan's stall.
    pub(crate) fn unstall_plan(&mut self) -> bool {
        match self {
            Self::Planned { stalled, .. } | Self::LeadsPlan { stalled, .. } if *stalled => {
                *stalled = false;
                true
            }
            Self::Open
            | Self::Closed { .. }
            | Self::Planned { .. }
            | Self::LeadsPlan { .. }
            | Self::Cutting => false,
        }
    }

    /// Whether the replica applied the barrier it was given.
    pub(crate) fn caught_up(&self, local: LocalReplica) -> bool {
        match self {
            Self::Open => true,
            // A planned replica's gate changes when its plan is satisfied,
            // and a cutting replica restarts.
            Self::Planned { .. } | Self::LeadsPlan { .. } | Self::Cutting => false,
            Self::Closed { recovery, .. } => match recovery {
                GateRecovery::CatchingUp(target) => local
                    .last_applied
                    .is_some_and(|applied| applied >= target.barrier_index),
                GateRecovery::AwaitingBarrier | GateRecovery::Stalled => false,
            },
        }
    }

    pub(crate) fn open(&mut self) {
        *self = Self::Open;
    }

    /// Decide one vote request of `candidate` at term `candidate_term`,
    /// whose last log id is `candidate_last` (index and term).
    pub(crate) fn screen(
        &mut self,
        candidate: u64,
        candidate_term: u64,
        candidate_last: Option<(u64, u64)>,
    ) -> VoteScreen {
        let candidate_last_log_index = candidate_last.map(|(index, _term)| index);
        let group = match self {
            Self::Open => return VoteScreen::Pass,
            // B3 (#577), N1 and M1 (#581 review): a planned replica's vote
            // opens for one candidate, in a campaign above the plan term,
            // and only while the candidate's log holds no entry above the
            // point from a leader elected before the plan.
            Self::Planned {
                leader,
                point,
                leader_term,
                ..
            } if *leader == candidate => {
                use ursula_stream::sim_controls::Defence;
                let foreign_entry = candidate_last
                    .is_some_and(|(index, term)| index > *point && term <= *leader_term);
                let guard_off = ursula_stream::sim_controls::off(Defence::PlanLogGuard);
                if candidate_term > *leader_term && foreign_entry && !guard_off {
                    // Only the log guard refuses this vote.
                    ursula_stream::sim_controls::note_fired(Defence::PlanLogGuard);
                }
                let no_foreign_entry = !foreign_entry || guard_off;
                return if candidate_term > *leader_term && no_foreign_entry {
                    VoteScreen::Pass
                } else {
                    VoteScreen::Refuse
                };
            }
            Self::Planned { .. } | Self::LeadsPlan { .. } | Self::Cutting => {
                return VoteScreen::Refuse;
            }
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

/// Why a gate opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GateOpening {
    /// The replica applied a fresh leader barrier.
    CaughtUp,
    /// Every voter reported an empty group: this replica initializes it.
    FreshBootstrap,
    /// The history event of this replica's recovery plan applied here, and
    /// it lost nothing since it took the plan.
    PlanSatisfied,
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
    #[error("{group} owner stopped: {source}")]
    OwnerStopped {
        group: RaftTarget,
        #[source]
        source: openraft::error::Fatal<UrsulaRaftTypeConfig>,
    },
    #[error("{group} is not registered on this node")]
    NotRegistered { group: RaftTarget },
    #[error("{group} has stopped: its log store is closed")]
    StoreClosed { group: RaftTarget },
    #[error("{group} lost its vote history; accepting an unsynced log tail cannot restore it")]
    MissingVoteFloor { group: RaftTarget },
    #[error("record that {group}'s recovery gate opened: {source}")]
    Record {
        group: RaftTarget,
        #[source]
        source: CoreJournalError,
    },
    #[error(
        "{group} on this replica follows the live leader {leader}; a recovery plan is \
         only for a group without one"
    )]
    FollowsLeader { group: RaftTarget, leader: u64 },
    #[error(
        "this replica leads {group} with a live quorum; a recovery plan is only for a \
         group without a leader"
    )]
    LeadsLive { group: RaftTarget },
    #[error(
        "{group}'s recovery plan elects node {leader}, but this request's step {step:?} \
         is not this replica's: send `lead` to the leader, then `follow` with its plan term to \
         every other member"
    )]
    StepMismatch {
        group: RaftTarget,
        leader: u64,
        step: PlanStep,
    },
    #[error(
        "{group}'s recovery cut could not run: its log holds {held:?} where the plan \
         keeps {committed:?}; the plan was dropped and the replica waits for a new plan"
    )]
    CutRefused {
        group: RaftTarget,
        committed: LogIdOf<UrsulaRaftTypeConfig>,
        held: Option<LogIdOf<UrsulaRaftTypeConfig>>,
    },
    #[error(
        "{group}'s recovery plan elects this replica, but after its cut the membership it \
         keeps does not make it a voter: the plan was dropped and the replica waits for a new plan"
    )]
    CutLeftNoVote { group: RaftTarget },
    #[error(
        "{group}'s recovery plan elects this replica, but after its cut its restarted core \
         never showed the membership it keeps: the plan was dropped and the replica waits for a \
         new plan"
    )]
    CutMembershipUnknown { group: RaftTarget },
    #[error(
        "{group}'s recovery gate on this replica is {status:?}, not stalled: a leader's \
         barrier may still open it without losing anything; plan only a group whose gated \
         replicas report it stalled"
    )]
    NotStalled {
        group: RaftTarget,
        status: RecoveryGateStatus,
    },
    #[error(
        "{group} on this replica holds {actual:?}, not the expected {expected:?}; \
         observe the group again"
    )]
    ReplicaChanged {
        group: RaftTarget,
        expected: ReplicaLogView,
        actual: ReplicaLogView,
    },
    #[error(
        "{group} on this replica committed index {committed:?} and applied index \
         {applied:?}, above the plan's point {point}; the plan picked the wrong leader"
    )]
    RecoveryPointBehind {
        group: RaftTarget,
        point: u64,
        committed: Option<u64>,
        applied: Option<u64>,
    },
    #[error("{group} on this replica knows no committed entry, so a plan cannot cut its log")]
    NothingCommitted { group: RaftTarget },
    #[error(
        "{group} on this replica recorded {committed:?} committed but holds {held:?} at \
         that index; observe the group again"
    )]
    CommittedEntryMissing {
        group: RaftTarget,
        committed: LogIdOf<UrsulaRaftTypeConfig>,
        held: Option<LogIdOf<UrsulaRaftTypeConfig>>,
    },
    #[error(
        "{group}'s recovery plan elects node {leader}, which is no voter of the membership \
         it would campaign under"
    )]
    LeaderNotVoter { group: RaftTarget, leader: u64 },
    #[error(
        "{group}'s recovery plan elects this replica with point {point}, but its {basis} \
         is {expected}; observe the group again"
    )]
    PointMismatch {
        group: RaftTarget,
        point: u64,
        expected: u64,
        basis: PointBasis,
    },
    #[error("{group}'s replica on this node has not started")]
    NotStarted { group: RaftTarget },
    #[error("{group}'s recovery plan job on this node ended without an answer")]
    JobAborted { group: RaftTarget },
    #[error(
        "{group}'s replica on this node is taking another recovery plan; observe the group \
         again once it answered"
    )]
    PlanInFlight { group: RaftTarget },
    #[error(
        "{group} has one voter: a recovery plan never cuts its log, since no other voter \
         could have overwritten an entry it holds"
    )]
    SingleVoterCut { group: RaftTarget },
    #[error(
        "{group}'s recovery plan names plan term {leader_term}, below the vote term \
         {current_term} the tool read on this replica: send the plan term the leader answered"
    )]
    PlanTermBelowVote {
        group: RaftTarget,
        leader_term: u64,
        current_term: u64,
    },
    #[error("restart {group}'s replica to run its recovery cut: {source}")]
    Restart {
        group: RaftTarget,
        #[source]
        source: Box<ursula_runtime::RuntimeError>,
    },
    /// The meta replica did not restart to run its recovery cut.
    #[error("restart the meta replica to run its recovery cut")]
    MetaRestart {
        #[source]
        source: Box<crate::meta::replica::MetaReplicaError>,
    },
}

/// A recovery gate that did not open when its replica started
/// ([`GroupRejoin::durable`]): a write or read of the replica's log store
/// failed, and the group does not start. The engine factory converts it
/// once into the group's `RecoveryGateOpen` error.
#[derive(Debug, thiserror::Error)]
pub enum GateOpenError {
    #[error("record that this replica of {group}, which led it, starts as a follower")]
    StartAsFollower {
        group: RaftTarget,
        #[source]
        source: CoreJournalError,
    },
    #[error("{group}: drop the recovery plan whose cut cannot run")]
    DropPlan {
        group: RaftTarget,
        #[source]
        source: CoreJournalError,
    },
    #[error(
        "{group}: record the plan term as the vote of the plan's leader before its Raft \
         core starts"
    )]
    RaisePlanVote {
        group: RaftTarget,
        #[source]
        source: CoreJournalError,
    },
    #[error("{group}: cut the log for the recovery plan")]
    CutLog {
        group: RaftTarget,
        #[source]
        source: std::io::Error,
    },
    #[error("{group}: record the recovery cut as done")]
    CutRecord {
        group: RaftTarget,
        #[source]
        source: CoreJournalError,
    },
}

impl RecoveryGateError {
    /// The refusal this error answers with a `409 Conflict`, when it is one:
    /// the replica changed nothing (m3, #581 review).
    pub fn refusal(&self) -> Option<ursula_proto::admin::RecoveryPlanRefusal> {
        match self.answer() {
            PlanAnswer::Refused(refusal) => Some(refusal),
            PlanAnswer::NotRegistered | PlanAnswer::Failed | PlanAnswer::Unavailable => None,
        }
    }

    /// How the node answers this error, decided for every variant here so
    /// that a new one cannot fall into a default (n5, #581 review).
    pub fn answer(&self) -> PlanAnswer {
        use ursula_proto::admin::RecoveryPlanRefusal;
        PlanAnswer::Refused(match self {
            Self::MissingVoteFloor { .. } => RecoveryPlanRefusal::MissingVoteFloor,
            Self::LeadsLive { .. } => RecoveryPlanRefusal::LeadsLive,
            Self::FollowsLeader { leader, .. } => {
                RecoveryPlanRefusal::FollowsLeader { leader: *leader }
            }
            Self::StepMismatch { .. } => RecoveryPlanRefusal::StepMismatch,
            Self::LeaderNotVoter { .. } => RecoveryPlanRefusal::LeaderNotVoter,
            Self::NotStalled { status, .. } => RecoveryPlanRefusal::NotStalled { status: *status },
            Self::ReplicaChanged { .. } => RecoveryPlanRefusal::ReplicaChanged,
            Self::RecoveryPointBehind { .. } => RecoveryPlanRefusal::RecoveryPointBehind,
            Self::PointMismatch { .. } => RecoveryPlanRefusal::PointMismatch,
            Self::NothingCommitted { .. } => RecoveryPlanRefusal::NothingCommitted,
            Self::CommittedEntryMissing { .. } => RecoveryPlanRefusal::CommittedEntryMissing,
            Self::CutRefused { .. } => RecoveryPlanRefusal::CutRefused,
            Self::CutLeftNoVote { .. } => RecoveryPlanRefusal::CutLeftNoVote,
            Self::CutMembershipUnknown { .. } => RecoveryPlanRefusal::CutMembershipUnknown,
            Self::StoreClosed { .. } => RecoveryPlanRefusal::StoreClosed,
            Self::PlanInFlight { .. } => RecoveryPlanRefusal::PlanInFlight,
            Self::SingleVoterCut { .. } => RecoveryPlanRefusal::SingleVoterCut,
            Self::PlanTermBelowVote { .. } => RecoveryPlanRefusal::PlanTermBelowVote,
            Self::NotRegistered { .. } => return PlanAnswer::NotRegistered,
            // A restart that failed for a reason that passes is retryable,
            // as one refused while the group's previous engine still stops
            // (#709). The error's own status decides, not a list of cases.
            Self::Restart { source, .. }
                if source.status() == ursula_runtime::ErrorStatus::Temporary =>
            {
                return PlanAnswer::Unavailable;
            }
            Self::Record { .. } | Self::Restart { .. } | Self::MetaRestart { .. } => {
                return PlanAnswer::Failed;
            }
            Self::OwnerStopped { .. } | Self::NotStarted { .. } | Self::JobAborted { .. } => {
                return PlanAnswer::Unavailable;
            }
        })
    }
}

/// How a node answers a recovery plan it did not take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanAnswer {
    /// `409 Conflict`: the replica refused it and changed nothing.
    Refused(ursula_proto::admin::RecoveryPlanRefusal),
    /// `404 Not Found`: the group has no replica here.
    NotRegistered,
    /// `500 Internal Server Error`: a write or the restart failed.
    Failed,
    /// `503 Service Unavailable`: the replica is not running.
    Unavailable,
}

/// Restarts one group's replica on this node: stops its engine, so its Raft
/// core and log store close, and starts it again from its log store. A
/// recovery plan's cut runs in between (#581 section 4.5). The node's
/// runtime implements it; simulations restart their own replicas.
pub trait ReplicaRestart: Send {
    /// Restarts the replica and returns its new recovery gate, `None` when
    /// it has none.
    fn restart_replica(
        &mut self,
        raft_group_id: RaftGroupId,
    ) -> futures_util::future::BoxFuture<
        '_,
        Result<Option<Arc<GroupRejoin>>, ursula_runtime::RuntimeError>,
    >;
}

/// Which index of its log the plan's leader continues from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PointBasis {
    /// Without a cut: its last log index.
    LastLogIndex,
    /// With a cut: its committed index.
    CommittedIndex,
}

impl fmt::Display for PointBasis {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::LastLogIndex => "last log index",
            Self::CommittedIndex => "committed index",
        })
    }
}

/// The claim of a replica's one accept job, released when dropped.
struct PlanJob<'a>(&'a AtomicBool);

impl<'a> PlanJob<'a> {
    fn claim(in_flight: &'a AtomicBool) -> Option<Self> {
        in_flight
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
            .then_some(Self(in_flight))
    }
}

impl Drop for PlanJob<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

/// What a recovery plan's accept job needs from its group's Raft core.
pub(crate) trait PlanCore: Sync {
    /// Stops the core campaigning until the node's election policy decides
    /// again.
    fn disable_elections(&self);
    /// The core's vote term, committed entry and server state, read through
    /// the core.
    fn state(&self) -> futures_util::future::BoxFuture<'_, Result<CoreState, RecoveryGateError>>;
}

/// A Raft core's vote term, committed entry and server state.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CoreState {
    pub(crate) vote_term: u64,
    pub(crate) committed: Option<LogIdOf<UrsulaRaftTypeConfig>>,
    pub(crate) server_state: openraft::ServerState,
}

/// [`PlanCore`] for a group's Raft core, of any state machine, inside its
/// owner job.
pub(crate) struct RaftPlanCore<'a, SM: RaftStateMachine<UrsulaRaftTypeConfig>> {
    pub(crate) raft: &'a Raft<UrsulaRaftTypeConfig, SM>,
    pub(crate) group: RaftTarget,
}

impl<SM: RaftStateMachine<UrsulaRaftTypeConfig>> PlanCore for RaftPlanCore<'_, SM> {
    fn disable_elections(&self) {
        self.raft.runtime_config().elect(false);
    }

    fn state(&self) -> futures_util::future::BoxFuture<'_, Result<CoreState, RecoveryGateError>> {
        Box::pin(async move {
            self.raft
                .with_raft_state(|state| CoreState {
                    vote_term: state.vote_ref().leader_id().term(),
                    committed: state.local_committed().copied(),
                    server_state: state.server_state,
                })
                .await
                .map_err(|source| RecoveryGateError::OwnerStopped {
                    group: self.group,
                    source,
                })
        })
    }
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

/// One replica's recovery state for one group: the inbound vote gate.
pub struct GroupRejoin {
    node_id: u64,
    group: RaftTarget,
    metrics: OnceLock<MetricsReceiver>,
    gate: Mutex<VoteGate>,
    vote_floor: Mutex<Option<UrsulaVote>>,
    /// The group's log store, which keeps its log state and where the gate
    /// records that it opened. Weak, so a stopped group's store closes even
    /// while its gate is still registered.
    store: Weak<RaftGroupFileLogStore>,
    changes: WatchSenderOf<UrsulaRaftTypeConfig, ()>,
    /// The last snapshot the commit screen refused, by its last log id, and
    /// why (S2 of the fifth #613 review): a resend of it is refused before
    /// the install queues for the group's lock, the node's permit or a
    /// download. OpenRaft treats snapshots with one last log id as one
    /// state. A refusal for a snapshot only grows firmer as the marker rises.
    refused_snapshot: Mutex<
        Option<(
            Option<LogIdOf<UrsulaRaftTypeConfig>>,
            ursula_runtime::GroupInfraError,
        )>,
    >,
    /// The committed leader whose AppendEntries reached this replica last,
    /// and when.
    last_leader_append: Mutex<
        Option<(
            UrsulaVote,
            openraft::type_config::alias::InstantOf<UrsulaRaftTypeConfig>,
        )>,
    >,
    /// The cut of the plan this replica took could not run at its start:
    /// the entry it keeps, and what the log held there.
    cut_refused: Option<(
        LogIdOf<UrsulaRaftTypeConfig>,
        Option<LogIdOf<UrsulaRaftTypeConfig>>,
    )>,
    /// How long a plan may apply nothing before it reports stalled.
    stall_after: OnceLock<Duration>,
    /// Test-only: the accept job waits for a permit here once it changed
    /// the gate, so a test can drop its caller mid-job.
    #[cfg(any(test, feature = "fault-injection"))]
    plan_pause: Mutex<Option<Arc<crate::rt::sync::Semaphore>>>,
    /// An accept job runs on this replica (m4, #581 review): a second one is
    /// refused until it ends, so two never interleave.
    plan_in_flight: AtomicBool,
}

impl fmt::Debug for GroupRejoin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupRejoin")
            .field("node_id", &self.node_id)
            .field("group", &self.group)
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
        group: RaftTarget,
        store: &Arc<RaftGroupFileLogStore>,
    ) -> Result<Self, GateOpenError> {
        store
            .prevent_crashed_leader_resume(node_id)
            .await
            .map_err(|source| GateOpenError::StartAsFollower { group, source })?;
        // A recovery plan's cut runs before the Raft core starts, so the
        // core never sees the entries it removes.
        let mut cut_refused = None;
        match store.run_recovery_cut().await {
            Ok(false) => {}
            Ok(true) => {
                tracing::warn!(
                    node_id,
                    plane = group.plane().as_str_name(),
                    raft_group_id = group.wire_group_id(),
                    "recovery plan: this replica's log now ends at the last entry it knew was \
                     committed"
                );
                // m4 (#581 review): a leadership this replica held is for
                // the log it cut: it never resumes it.
                store
                    .start_as_follower(node_id)
                    .await
                    .map_err(|source| GateOpenError::StartAsFollower { group, source })?;
            }
            // m3 (#581 review): the log no longer holds the entry the cut
            // keeps. The plan is dropped and the gate stays closed until a
            // new plan; the group starts and reports it.
            Err(crate::log_store::RecoveryCutError::Diverged { committed, held }) => {
                tracing::error!(
                    node_id,
                    plane = group.plane().as_str_name(),
                    raft_group_id = group.wire_group_id(),
                    ?committed,
                    ?held,
                    "recovery plan: the log no longer holds the entry the plan's cut keeps; the \
                     plan is dropped and the replica waits for a new plan"
                );
                store
                    .drop_unrunnable_plan()
                    .await
                    .map_err(|source| GateOpenError::DropPlan { group, source })?;
                store
                    .start_as_follower(node_id)
                    .await
                    .map_err(|source| GateOpenError::StartAsFollower { group, source })?;
                cut_refused = Some((committed, held));
            }
            Err(crate::log_store::RecoveryCutError::Io(source)) => {
                return Err(GateOpenError::CutLog { group, source });
            }
            Err(crate::log_store::RecoveryCutError::Journal(source)) => {
                return Err(GateOpenError::CutRecord { group, source });
            }
        }
        let gate = match (store.recovery_record(), store.log_state()) {
            // M1 (#581 review): a member that may have lost entries keeps a
            // restricted vote: a plan's leader closes its gate, a member
            // stays planned, and its satisfied plan closes the gate too.
            (Some(record), GroupLogState::Recovering) => {
                tracing::warn!(
                    node_id,
                    plane = group.plane().as_str_name(),
                    raft_group_id = group.wire_group_id(),
                    leader = record.leader,
                    "recovery gate: this replica holds a recovery plan and may be missing Raft \
                     log entries it acknowledged; it keeps its vote restricted"
                );
                store
                    .start_as_follower(node_id)
                    .await
                    .map_err(|source| GateOpenError::StartAsFollower { group, source })?;
                if record.leader == node_id {
                    VoteGate::closed(GroupEvidence::Initialized)
                } else {
                    VoteGate::planned(&record, node_id)
                }
            }
            // The plan holds across restarts until it is satisfied. A
            // leadership this replica held is at or below the plan term:
            // it never resumes it (M2, #581 review).
            (Some(record), _) => {
                store
                    .start_as_follower(node_id)
                    .await
                    .map_err(|source| GateOpenError::StartAsFollower { group, source })?;
                // #613: the plan's leader starts with the plan term as its
                // uncommitted vote, if that is higher, so its first campaign
                // is above the plan term. Before the Raft core starts, like
                // the demotion above. OpenRaft then also refuses appends from
                // a leader whose vote is below `(plan term, this node)`. A
                // leader at the plan term with a higher node id passes it:
                // the replication fence is what refuses that one.
                if record.leader == node_id
                    && !ursula_stream::sim_controls::off(
                        ursula_stream::sim_controls::Defence::PlanTermVote,
                    )
                {
                    store
                        .persist_recovery_vote(VoteOf::<UrsulaRaftTypeConfig>::new(
                            record.leader_term,
                            node_id,
                        ))
                        .await
                        .map_err(|source| GateOpenError::RaisePlanVote { group, source })?;
                }
                VoteGate::planned(&record, node_id)
            }
            (None, _) if cut_refused.is_some() => VoteGate::closed(GroupEvidence::Initialized),
            (None, GroupLogState::Initialized) => VoteGate::Open,
            (None, GroupLogState::Recovering) => {
                tracing::warn!(
                    node_id,
                    plane = group.plane().as_str_name(),
                    raft_group_id = group.wire_group_id(),
                    "recovery gate: this replica may be missing Raft log entries it \
                     acknowledged; it stays out of elections until it has applied a fresh \
                     leader barrier"
                );
                store
                    .start_as_follower(node_id)
                    .await
                    .map_err(|source| GateOpenError::StartAsFollower { group, source })?;
                VoteGate::closed(GroupEvidence::Initialized)
            }
            // Unreachable while nothing retires a log: a retired log holds
            // no vote and no entries, and never takes a first election.
            // S2's `NotHeld` (#768) removes this arm.
            (None, GroupLogState::Retired) => {
                tracing::warn!(
                    node_id,
                    plane = group.plane().as_str_name(),
                    raft_group_id = group.wire_group_id(),
                    "recovery gate: this replica's log of the group is retired; it stays out of \
                     elections"
                );
                VoteGate::closed(GroupEvidence::Initialized)
            }
            (None, GroupLogState::Empty) => {
                tracing::debug!(
                    node_id,
                    plane = group.plane().as_str_name(),
                    raft_group_id = group.wire_group_id(),
                    "recovery gate: this replica holds nothing of the group yet; it joins only a \
                     new group's first election until it has applied a leader barrier"
                );
                store.hold_unknown_history();
                VoteGate::closed(GroupEvidence::Unknown)
            }
        };
        Ok(Self {
            node_id,
            group,
            metrics: OnceLock::new(),
            gate: Mutex::new(gate),
            // Any durable vote is a floor, the genesis `(0, 0)` included:
            // granting a vote or acknowledging a leader persists a higher
            // vote first, so a durable `(0, 0)` proves neither happened.
            vote_floor: Mutex::new(store.vote()),
            store: Arc::downgrade(store),
            changes: UrsulaRaftTypeConfig::watch_channel(()).0,
            last_leader_append: Mutex::new(None),
            refused_snapshot: Mutex::new(None),
            cut_refused,
            stall_after: OnceLock::new(),
            #[cfg(any(test, feature = "fault-injection"))]
            plan_pause: Mutex::new(None),
            plan_in_flight: AtomicBool::new(false),
        })
    }

    /// Test-only: every later accept job waits, once it changed the gate,
    /// until the returned semaphore has a permit.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn pause_plans_after_the_fence(&self) -> Arc<crate::rt::sync::Semaphore> {
        let pause = Arc::new(crate::rt::sync::Semaphore::new(0));
        *self
            .plan_pause
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(pause.clone());
        pause
    }

    /// Test-only: whether an accept job runs on this replica.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn plan_job_running(&self) -> bool {
        self.plan_in_flight.load(Ordering::SeqCst)
    }

    /// The node this replica runs on.
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    pub fn group(&self) -> RaftTarget {
        self.group
    }

    /// Test-only: the replica holds `record`'s gate, as after taking it.
    #[cfg(test)]
    pub(crate) fn hold_plan(&self, record: &RecoveryRecord) {
        *self.gate() = VoteGate::planned(record, self.node_id);
    }

    fn gate(&self) -> std::sync::MutexGuard<'_, VoteGate> {
        self.gate.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Attach the group's Raft metrics. Call once the Raft exists and before
    /// it is reachable from the network.
    pub(crate) fn bind<SM: RaftStateMachine<UrsulaRaftTypeConfig>>(
        &self,
        raft: &Raft<UrsulaRaftTypeConfig, SM>,
    ) -> Result<(), ursula_runtime::GroupEngineError> {
        let expected = raft.metrics().borrow_watched().id;
        if self.node_id != expected {
            return Err(ursula_runtime::GroupEngineError::Infra(
                ursula_runtime::GroupInfraError::RecoveryNodeMismatch {
                    group: self.group,
                    expected,
                    actual: self.node_id,
                },
            ));
        }
        self.metrics.set(raft.metrics()).map_err(|_already_bound| {
            ursula_runtime::GroupEngineError::Infra(
                ursula_runtime::GroupInfraError::RecoveryAlreadyBound { group: self.group },
            )
        })?;
        if !self.may_campaign() {
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
        let store = self
            .store
            .upgrade()
            .ok_or(RecoveryGateError::StoreClosed { group: self.group })?;
        let vote = store.persist_recovery_vote(vote).await.map_err(|source| {
            RecoveryGateError::Record {
                group: self.group,
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

    /// Records a fresh barrier from `leader` whose index is `barrier_index`
    /// ([`CatchUpTarget`]), when that leader's vote is at or above the floor.
    pub(crate) fn confirm_barrier(&self, leader: UrsulaVote, barrier_index: u64) {
        if self.replication_allowed(leader) {
            self.gate().confirm_barrier(leader, barrier_index);
        }
    }

    /// Campaigning is subject to both this gate and the node's shed policy.
    pub(crate) fn may_campaign(&self) -> bool {
        self.gate().campaigns() && !self.needs_vote_floor()
    }

    /// Opens the gate, recording it first.
    async fn open(&self, why: GateOpening) -> Result<(), RecoveryGateError> {
        let store = self
            .store
            .upgrade()
            .ok_or(RecoveryGateError::StoreClosed { group: self.group })?;
        if self.needs_vote_floor() && matches!(why, GateOpening::FreshBootstrap) {
            self.establish_vote_floor(self.metrics().map_or(UrsulaVote::new(0, 0), |m| m.vote))
                .await?;
        }
        store
            .record_recovered()
            .await
            .map_err(|source| RecoveryGateError::Record {
                group: self.group,
                source,
            })?;
        self.gate().open();
        self.changes.send_if_modified(|()| true);
        match why {
            GateOpening::CaughtUp => tracing::info!(
                node_id = self.node_id,
                plane = self.group.plane().as_str_name(),
                raft_group_id = self.group.wire_group_id(),
                "recovery gate opened: this replica applied a fresh leader barrier"
            ),
            GateOpening::FreshBootstrap => tracing::info!(
                node_id = self.node_id,
                plane = self.group.plane().as_str_name(),
                raft_group_id = self.group.wire_group_id(),
                "recovery gate opened: every voter reported an empty group"
            ),
            GateOpening::PlanSatisfied => tracing::info!(
                node_id = self.node_id,
                plane = self.group.plane().as_str_name(),
                raft_group_id = self.group.wire_group_id(),
                "recovery gate opened: this replica applied its recovery plan's history event"
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

    /// Why the replica takes no replication from a leader whose vote is
    /// `vote` ([`VoteGate::takes_replication_at`]), if it takes none.
    pub(crate) fn replication_refusal(
        &self,
        vote: &UrsulaVote,
    ) -> Option<ursula_runtime::GroupInfraError> {
        let group = self.group;
        let gate = self.gate();
        if gate.takes_replication_at(vote.leader_id().term()) {
            return None;
        }
        Some(match &*gate {
            VoteGate::Planned { leader_term, .. } | VoteGate::LeadsPlan { leader_term, .. } => {
                ursula_runtime::GroupInfraError::RecoveryPlanFenced {
                    group,
                    plan_term: *leader_term,
                }
            }
            VoteGate::Open | VoteGate::Closed { .. } | VoteGate::Cutting => {
                ursula_runtime::GroupInfraError::RecoveryCutting { group }
            }
        })
    }

    /// Drops the plan this replica leads because the cut left it no vote
    /// in the membership it keeps, or its restart never showed that
    /// membership (#613): a leader that cannot campaign would wedge every
    /// member restricted to it. Like a cut that cannot run, the plan goes in
    /// the same write that records the log recovering, and the gate stays
    /// closed until a new plan.
    pub(crate) async fn drop_plan_left_without_vote(
        &self,
        history: &crate::history::GroupHistory,
    ) -> Result<(), RecoveryGateError> {
        let group = self.group;
        let store = self
            .store
            .upgrade()
            .ok_or(RecoveryGateError::StoreClosed { group })?;
        store
            .drop_unrunnable_plan()
            .await
            .map_err(|source| RecoveryGateError::Record { group, source })?;
        drop(store);
        *self.gate() = VoteGate::closed(GroupEvidence::Initialized);
        history.set_plan(None);
        self.changes.send_if_modified(|()| true);
        tracing::error!(
            node_id = self.node_id,
            plane = group.plane().as_str_name(),
            raft_group_id = group.wire_group_id(),
            "recovery plan: after its cut this replica, the plan's leader, is no voter of the \
             membership it keeps; the plan is dropped and the replica waits for a new plan"
        );
        Ok(())
    }

    /// The cut of the plan this replica took could not run at its start
    /// (m3, #581 review): the entry the cut keeps, and what the log held.
    pub(crate) fn cut_refused(
        &self,
    ) -> Option<(
        LogIdOf<UrsulaRaftTypeConfig>,
        Option<LogIdOf<UrsulaRaftTypeConfig>>,
    )> {
        self.cut_refused
    }

    /// The live leader this replica follows: a committed leader whose
    /// AppendEntries reached it within [`LIVE_LEADER_QUORUM_ACK`] (m2, #581
    /// review: an append may reach the replica before its metrics show the
    /// leader).
    fn follows_live_leader(&self) -> Option<u64> {
        let last = *self
            .last_leader_append
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let (vote, at) = last?;
        let leader = *vote.leader_id().node_id();
        (leader != self.node_id && vote.is_committed() && at.elapsed() < LIVE_LEADER_QUORUM_ACK)
            .then_some(leader)
    }

    /// This replica's commit marker as the commit screen reads it (B1 of
    /// the fifth #613 review): the greatest of the store's marker, OpenRaft's
    /// committed index and its applied one. OpenRaft raises its committed
    /// index past the store's marker after a snapshot install without saving
    /// it, and under `fsync = never` the stored marker falls behind after a
    /// power loss. Read once per request, in the owner job. `None` when
    /// nothing is committed.
    ///
    /// OpenRaft's indexes are above the last entry the store holds only
    /// while the store has not yet recorded an install OpenRaft admitted.
    /// The screen cannot compare anything in that window, so it refuses with
    /// [`ursula_runtime::GroupInfraError::LogBehindCommit`] instead of
    /// lowering the marker; the sender retries once the store caught up.
    pub(crate) fn effective_marker(
        &self,
        metrics: &openraft::RaftMetrics<UrsulaRaftTypeConfig>,
    ) -> Result<Option<u64>, ursula_runtime::GroupInfraError> {
        let store = self
            .store
            .upgrade()
            .ok_or(ursula_runtime::GroupInfraError::OwnerStopped)?;
        let stored = store.committed_index();
        if ursula_stream::sim_controls::off(
            ursula_stream::sim_controls::Defence::CommitScreenPurged,
        ) {
            return Ok(stored);
        }
        let raft = [
            metrics.local_committed.map(|id| id.index),
            metrics.last_applied.map(|id| id.index),
        ]
        .into_iter()
        .flatten()
        .max();
        let held = store.last_held_index();
        let behind = raft.is_some_and(|index| held.is_none_or(|held| index > held));
        if behind {
            return Err(ursula_runtime::GroupInfraError::LogBehindCommit {
                group: self.group,
                committed: raft.unwrap_or(0),
                held,
            });
        }
        Ok(stored.max(raft))
    }

    /// The commit screen (#613): refuses an append, or a snapshot install,
    /// that would replace entries this replica knows are committed, so the
    /// replica stays stalled instead of diverging. Checked before OpenRaft
    /// sees the request, which assumes a committed entry is always held.
    /// `marker` is [`Self::effective_marker`]; with none, nothing is
    /// committed and nothing is refused. Without its store, the replica is
    /// stopping and refuses everything.
    ///
    /// - An append whose entries, or the entry it continues from, differ
    ///   from this replica's log at or below the marker, compared with the
    ///   last purged entry below it.
    /// - A snapshot whose last entry differs so.
    /// - A snapshot of another history than the one the marker lies in,
    ///   unless that history directly replaced this one and either kept
    ///   every entry up to the marker ([`ursula_runtime::HistoryEvent::kept`])
    ///   or was proposed by the leader of this replica's entry at the marker
    ///   ([`ursula_runtime::HistoryEvent::leader`]), whose log this replica
    ///   then holds up to there. A snapshot carries only its latest history
    ///   event, so a history two or more steps ahead, or on another branch,
    ///   cannot show that it kept this replica's committed entries: it is
    ///   refused as `UnrelatedHistory`, and the replica rejoins as a wiped
    ///   node. OpenRaft compares log ids term first, so a later leader's
    ///   snapshot always looks newer, even at a lower index.
    pub(crate) fn committed_overwrite(
        &self,
        incoming: Incoming<'_>,
        marker: Option<u64>,
    ) -> Option<ursula_runtime::GroupInfraError> {
        use ursula_stream::sim_controls::Defence;
        let (switch, ids, history) = match incoming {
            Incoming::Append(request) => (
                Defence::CommitScreen,
                request
                    .prev_log_id
                    .into_iter()
                    .chain(request.entries.iter().map(|entry| entry.log_id))
                    .collect::<Vec<_>>(),
                None,
            ),
            Incoming::Snapshot {
                last,
                history,
                local,
            } => (
                Defence::CommitScreenInstall,
                last.into_iter().collect(),
                Some((history, local)),
            ),
        };
        if ursula_stream::sim_controls::off(switch) {
            return None;
        }
        let Some(store) = self.store.upgrade() else {
            return Some(ursula_runtime::GroupInfraError::OwnerStopped);
        };
        let committed = marker?;
        let skip_purged = ursula_stream::sim_controls::off(Defence::CommitScreenPurged);
        let refusal = store
            .committed_conflict(committed, ids, skip_purged)
            .map(
                |index| ursula_runtime::GroupInfraError::CommittedOverwrite {
                    group: self.group,
                    committed,
                    index,
                },
            )
            .or_else(|| {
                let (incoming, local) = history?;
                let id = incoming.map_or(0, |event| event.id.get());
                if local == Some(id) {
                    return None;
                }
                let replaced = incoming.map_or(0, |event| event.previous);
                let chained = local == Some(replaced)
                    || (local.is_some()
                        && ursula_stream::sim_controls::off(Defence::CommitScreenChain));
                if !chained {
                    return Some(ursula_runtime::GroupInfraError::UnrelatedHistory {
                        group: self.group,
                        history: id,
                        replaced,
                        local,
                    });
                }
                let kept = incoming.map_or(0, |event| event.kept);
                // S1 of the fifth #613 review: the new history's leader may
                // have appended its blank entry, which this replica saw
                // committed, before the event.
                let same_leader = incoming.is_some_and(|event| {
                    event.leader.term > 0
                        && store.log_id_or_purged_at(committed).is_some_and(|held| {
                            (held.leader_id.term, held.leader_id.node_id)
                                == (event.leader.term, event.leader.node_id)
                        })
                });
                (kept < committed && !same_leader).then_some(
                    ursula_runtime::GroupInfraError::CommittedOverwrite {
                        group: self.group,
                        committed,
                        index: kept.saturating_add(1),
                    },
                )
            })?;
        ursula_stream::sim_controls::note_fired(switch);
        Some(refusal)
    }

    /// The refusal of the snapshot through `last_log_id`, when the commit
    /// screen refused it before.
    pub(crate) fn refused_snapshot(
        &self,
        last_log_id: Option<LogIdOf<UrsulaRaftTypeConfig>>,
    ) -> Option<ursula_runtime::GroupInfraError> {
        self.refused_snapshot
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .filter(|(refused, _)| *refused == last_log_id)
            .map(|(_, refusal)| refusal.clone())
    }

    /// Records that the commit screen refused the snapshot through
    /// `last_log_id`. A refusal for a log it could not read may pass later:
    /// not recorded.
    pub(crate) fn record_refused_snapshot(
        &self,
        last_log_id: Option<LogIdOf<UrsulaRaftTypeConfig>>,
        refusal: &ursula_runtime::GroupInfraError,
    ) {
        if matches!(
            refusal,
            ursula_runtime::GroupInfraError::UnrelatedHistory { local: None, .. }
                | ursula_runtime::GroupInfraError::OwnerStopped
        ) {
            return;
        }
        *self
            .refused_snapshot
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some((last_log_id, refusal.clone()));
    }

    /// The id of the history this replica's commit marker lies in (#613):
    /// that of the newest history event at or below `marker`, 0 before the
    /// genesis. `applied` is the newest event it applied, through
    /// `applied_index`; committed entries above that are read from its log.
    /// An event counts as apply would count it: only when it names the
    /// history before it. `None` when the log could not be read, or holds
    /// an import there, whose outcome only its apply knows.
    pub(crate) async fn history_at_marker(
        &self,
        applied: Option<ursula_runtime::HistoryEvent>,
        applied_index: Option<u64>,
        marker: Option<u64>,
    ) -> Option<u64> {
        let mut current = applied.map_or(0, |event| event.id.get());
        let Some(committed) = marker else {
            return Some(current);
        };
        let from = applied_index.map_or(0, |index| index.saturating_add(1));
        if from > committed {
            return Some(current);
        }
        let mut reader = self.store.upgrade()?;
        let entries =
            openraft::storage::RaftLogReader::try_get_log_entries(&mut reader, from..=committed)
                .await
                .ok()?;
        for entry in entries {
            match entry.payload {
                openraft::EntryPayload::Normal(crate::types::RaftCommand::Stream(
                    ursula_stream::StreamCommand::NewHistory {
                        history, previous, ..
                    },
                )) => {
                    if previous == current {
                        current = history.get();
                    }
                }
                openraft::EntryPayload::Normal(crate::types::RaftCommand::Stream(
                    ursula_stream::StreamCommand::ImportSnapshot { .. },
                )) => return None,
                openraft::EntryPayload::Normal(crate::types::RaftCommand::Meta(
                    crate::meta::MetaCommand::NewHistory {
                        history, previous, ..
                    },
                )) if previous == current => current = history.get(),
                _ => {}
            }
        }
        Some(current)
    }

    /// The membership this replica keeps when a recovery plan cuts its log
    /// back to `committed` ([`kept_of`]), given the state machine's
    /// `applied` membership.
    pub(crate) fn kept_membership(
        &self,
        committed: Option<u64>,
        applied: openraft::alias::StoredMembershipOf<UrsulaRaftTypeConfig>,
    ) -> openraft::alias::StoredMembershipOf<UrsulaRaftTypeConfig> {
        let log = committed.and_then(|index| {
            self.store
                .upgrade()
                .and_then(|store| store.membership_at_or_below(index))
        });
        kept_of(log, applied)
    }

    /// Takes a recovery plan (#581 section 4.5), as one owner job of its
    /// group on `core`. Refused, with nothing changed, when the replica leads
    /// or follows a live leader, the request's step is not this replica's
    /// role, it is the plan's leader and no voter of the membership it would
    /// campaign under (#613), the replica no longer holds
    /// the log the tool read, it committed or applied above the plan's
    /// point, or it is the plan's leader and its log does not continue from
    /// the point.
    ///
    /// Otherwise, before its first await, the replica closes its gate to
    /// the plan (the leader's gate campaigns but votes for no one else, a
    /// member's votes for the leader only) and its leader fence (`history`)
    /// learns the plan (N1, #581 review). With a cut it closes completely
    /// ([`VoteGate::Cutting`]). It then reads the committed entry and its
    /// vote through the core. The plan's leader votes for itself at the next
    /// term, the plan term, which members require on its vote requests. The
    /// plan and the accepted log are recorded in one durable write (N2).
    /// With a cut the group must then restart ([`PlanTaken::Restart`]) to
    /// run it before the answer.
    pub(crate) async fn accept_recovery_plan(
        &self,
        plan: &RecoveryPlanRequest,
        core: &dyn PlanCore,
        history: &crate::history::GroupHistory,
        applied: openraft::alias::StoredMembershipOf<UrsulaRaftTypeConfig>,
    ) -> Result<PlanTaken, RecoveryGateError> {
        let group = self.group;
        // m4 (#581 review): one accept job at a time. A second one would
        // read the first one's provisional gate as the state to restore.
        let _job = PlanJob::claim(&self.plan_in_flight)
            .ok_or(RecoveryGateError::PlanInFlight { group })?;
        if self.needs_vote_floor() {
            return Err(RecoveryGateError::MissingVoteFloor { group });
        }
        let store = self
            .store
            .upgrade()
            .ok_or(RecoveryGateError::StoreClosed { group })?;
        let metrics = self
            .metrics()
            .ok_or(RecoveryGateError::NotStarted { group })?;
        let leads = plan.leader == self.node_id;
        match (plan.step, leads) {
            // Item 6 of the #612 review: a correct plan term is at least the
            // highest vote term the tool read, this replica's included.
            (PlanStep::Follow { leader_term }, false)
                if leader_term < plan.expected.current_term =>
            {
                return Err(RecoveryGateError::PlanTermBelowVote {
                    group,
                    leader_term,
                    current_term: plan.expected.current_term,
                });
            }
            (PlanStep::Lead { .. }, true) | (PlanStep::Follow { .. }, false) => {}
            (step, _) => {
                return Err(RecoveryGateError::StepMismatch {
                    group,
                    leader: plan.leader,
                    step,
                });
            }
        }
        // #577 N7 and M2 (#581 review): a plan never fences a group that
        // has a live leader, whether this replica leads it with a recent
        // quorum or follows it.
        if leads_live(&metrics) {
            return Err(RecoveryGateError::LeadsLive { group });
        }
        if let Some(leader) = self.follows_live_leader() {
            return Err(RecoveryGateError::FollowsLeader { group, leader });
        }
        // #613: only the plan's leader checks membership, against the one
        // it campaigns under. A member's vote counts under the leader's
        // membership whatever its own says, so a member whose membership
        // is older or newer than the leader's is one the leader needs.
        // A cut that knows no committed entry is refused once the gate has
        // changed (`NothingCommitted`), so it keeps the effective one here.
        // A cut to a later committed entry another replica reported keeps
        // the leader's log up to the point, so its membership there.
        let keeps_to = if leads && plan.cut && plan.point_entry.is_some() {
            Some(plan.point)
        } else {
            metrics.local_committed.as_ref().map(|id| id.index())
        };
        let kept = self.kept_membership(keeps_to, applied);
        let effective_only = ursula_stream::sim_controls::off(
            ursula_stream::sim_controls::Defence::PlanKeptMembership,
        );
        let campaigns_under = if plan.cut && metrics.local_committed.is_some() && !effective_only {
            kept.membership()
        } else {
            metrics.membership_config.membership()
        };
        let member_checks = !leads
            && ursula_stream::sim_controls::off(
                ursula_stream::sim_controls::Defence::PlanMemberCheck,
            );
        let checked = if leads {
            Some(campaigns_under)
        } else if member_checks {
            Some(metrics.membership_config.membership())
        } else {
            None
        };
        if checked
            .is_some_and(|membership| !membership.voter_ids().any(|voter| voter == plan.leader))
        {
            return Err(RecoveryGateError::LeaderNotVoter {
                group,
                leader: plan.leader,
            });
        }
        // #581 section 4.5: a group with one voter never cuts, as the
        // planner decides it (m4 of the #613 review).
        if plan.cut
            && ursula_proto::admin::single_voter(
                metrics
                    .membership_config
                    .membership()
                    .get_joint_config()
                    .iter()
                    .chain(kept.membership().get_joint_config().iter()),
            )
        {
            return Err(RecoveryGateError::SingleVoterCut { group });
        }
        let status = self.status();
        if matches!(
            status,
            RecoveryGateStatus::AwaitingBarrier
                | RecoveryGateStatus::CatchingUp
                | RecoveryGateStatus::Cutting
        ) {
            return Err(RecoveryGateError::NotStalled { group, status });
        }
        // A replica that campaigns (an open one, or a plan's leader) climbs
        // its vote term with every campaign it starts during a stall, and no
        // plan reads it (N1 fix 5): only a gated replica's term must be the
        // one the tool read.
        let actual = ReplicaLog::of(&store);
        let expected = ReplicaLog::from(plan.expected);
        let campaigns = self.gate().campaigns();
        let changed = if campaigns {
            (actual.last_log_index, actual.last_log_term)
                != (expected.last_log_index, expected.last_log_term)
        } else {
            actual != expected
        };
        if changed {
            return Err(RecoveryGateError::ReplicaChanged {
                group,
                expected: plan.expected,
                actual: actual.into(),
            });
        }
        let committed = log_index(metrics.local_committed.as_ref());
        let applied = log_index(metrics.last_applied.as_ref());
        if committed.is_some_and(|index| index > plan.point)
            || applied.is_some_and(|index| index > plan.point)
        {
            return Err(RecoveryGateError::RecoveryPointBehind {
                group,
                point: plan.point,
                committed,
                applied,
            });
        }
        // The plan's leader continues from the point: its last entry, or
        // with a cut its committed one, or with a cut to a later entry
        // another replica reported committed (#613), exactly that entry.
        if leads
            && plan.cut
            && let Some(keep) = point_log_id(plan)
        {
            let held = store.log_id_at(plan.point);
            if held != Some(keep) || committed.is_some_and(|index| index > plan.point) {
                return Err(RecoveryGateError::CommittedEntryMissing {
                    group,
                    committed: keep,
                    held,
                });
            }
        } else if leads {
            let (expected, basis) = if plan.cut {
                (committed.unwrap_or(0), PointBasis::CommittedIndex)
            } else {
                (actual.last_log_index.unwrap_or(0), PointBasis::LastLogIndex)
            };
            if plan.point != expected {
                return Err(RecoveryGateError::PointMismatch {
                    group,
                    point: plan.point,
                    expected,
                    basis,
                });
            }
        }
        let cut_restart = plan.cut
            && !ursula_stream::sim_controls::off(ursula_stream::sim_controls::Defence::RecoveryCut);
        // N1 (#581 review): from here on, before any await, the replica is
        // closed to every leader but the plan's, and the plan's leader is
        // fenced: it serves and admits nothing until the plan's event. The
        // core stops campaigning until the job ends, so a campaign the
        // leader starts below the plan term cannot be under way when it
        // reads its state. The node's policy then decides (m6).
        let provisional = RecoveryRecord {
            leader: plan.leader,
            point: plan.point,
            // n3 (#581 review): the leader's vote term as its metrics show
            // it bounds the plan term from below already.
            leader_term: match plan.step {
                PlanStep::Lead { highest_term } => {
                    highest_term.max(metrics.vote.leader_id().term())
                }
                PlanStep::Follow { leader_term } => leader_term,
            },
            cut: PlanCut::NoCut,
        };
        let previous_plan = history.plan();
        let previous_gate = {
            let mut gate = self.gate();
            let next = if cut_restart {
                VoteGate::Cutting
            } else {
                VoteGate::planned(&provisional, self.node_id)
            };
            std::mem::replace(&mut *gate, next)
        };
        core.disable_elections();
        history.set_plan(Some(provisional));
        self.changes.send_if_modified(|()| true);
        #[cfg(any(test, feature = "fault-injection"))]
        {
            let pause = self
                .plan_pause
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
            if let Some(pause) = pause {
                drop(pause.acquire().await);
            }
        }
        match self.take_plan(plan, &store, core, leads, cut_restart).await {
            Ok((record, restart)) => {
                drop(store);
                if !restart {
                    *self.gate() = VoteGate::planned(&record, self.node_id);
                }
                history.set_plan(Some(record));
                self.changes.send_if_modified(|()| true);
                Ok(self.plan_taken(plan, record.leader_term, leads, restart))
            }
            Err(error) => {
                // Refused after the gate changed: nothing was recorded, so
                // the replica goes back to how it was. Its elections are
                // the node's policy's again once the job ends (m6).
                drop(store);
                *self.gate() = previous_gate;
                history.set_plan(previous_plan);
                self.changes.send_if_modified(|()| true);
                Err(error)
            }
        }
    }

    /// The part of [`Self::accept_recovery_plan`] after the gate changed:
    /// reads the committed entry, the vote term and the server state
    /// through the core, takes the plan term on the plan's leader, and
    /// records the plan. Returns the record and whether the group restarts
    /// before the answer: to run the cut, or to demote a replica that leads
    /// or campaigns (M2, #581 review).
    async fn take_plan(
        &self,
        plan: &RecoveryPlanRequest,
        store: &RaftGroupFileLogStore,
        core: &dyn PlanCore,
        leads: bool,
        cut_restart: bool,
    ) -> Result<(RecoveryRecord, bool), RecoveryGateError> {
        let group = self.group;
        // B1 (#581 review): the committed entry the cut keeps is read
        // through the core, after the gate closed, so nothing acknowledged
        // since can be cut.
        let state = core.state().await?;
        // m8 (#581 review): the committed index may have moved since the
        // checks above read it. The plan's leader continues from the point,
        // and no member keeps an entry above it.
        let committed = log_index(state.committed.as_ref());
        if committed.is_some_and(|index| index > plan.point) {
            return Err(RecoveryGateError::RecoveryPointBehind {
                group,
                point: plan.point,
                committed,
                applied: None,
            });
        }
        let raised = if leads && plan.cut {
            point_log_id(plan)
        } else {
            None
        };
        if leads && plan.cut && raised.is_none() && committed.unwrap_or(0) != plan.point {
            return Err(RecoveryGateError::PointMismatch {
                group,
                point: plan.point,
                expected: committed.unwrap_or(0),
                basis: PointBasis::CommittedIndex,
            });
        }
        let cut = if plan.cut {
            // The plan's leader keeps up to a later entry another replica
            // reported committed, when it holds exactly that entry (#613):
            // the cut then never lands below a reached replica's commit.
            let keep = match raised {
                Some(keep) => keep,
                None => state
                    .committed
                    .ok_or(RecoveryGateError::NothingCommitted { group })?,
            };
            let held = store.log_id_at(keep.index());
            if held != Some(keep) {
                return Err(RecoveryGateError::CommittedEntryMissing {
                    group,
                    committed: keep,
                    held,
                });
            }
            PlanCut::Pending { keep }
        } else {
            PlanCut::NoCut
        };
        let leader_term = match plan.step {
            // M1 (#581 review): the plan term is at least every vote term
            // the tool read, a plan member's included, and this replica's
            // own, which covers every campaign it started before the plan.
            // A leader at or below it is fenced and guarded. One above it
            // needed votes from replicas that hold no plan: open ones refuse
            // a log behind their own, closed and wiped ones vote for no one,
            // so it cannot get the plan's leader elected over its entries.
            // No vote is cast for the plan term (M2).
            PlanStep::Lead { highest_term } => highest_term.max(state.vote_term),
            PlanStep::Follow { leader_term } => leader_term,
        };
        // M2 (#581 review): a replica that leads, or the plan's leader while
        // it campaigns, restarts before it answers: its core drops the
        // leadership or the campaign, both at or below the plan term, and
        // starts again as a follower under the plan.
        // A negative control that defers the cut to the next start skips
        // every in-process restart, as before the #581 review.
        // #613 (n2 of the fourth #581 review): the plan's leader whose vote
        // term is below the plan term restarts too, and starts with the plan
        // term as its vote (`GroupRejoin::durable`), so its first campaign
        // is above it. Climbing one term per election timeout could outlast
        // the stall timeout when an open replica's campaigns raised the
        // plan term far above its own.
        let demote = (matches!(state.server_state, openraft::ServerState::Leader)
            || (leads
                && (matches!(state.server_state, openraft::ServerState::Candidate)
                    || state.vote_term < leader_term)))
            && !ursula_stream::sim_controls::off(ursula_stream::sim_controls::Defence::RecoveryCut);
        let restart = cut_restart || demote;
        if demote {
            *self.gate() = VoteGate::Cutting;
            self.changes.send_if_modified(|()| true);
        }
        let record = RecoveryRecord {
            leader: plan.leader,
            point: plan.point,
            leader_term,
            cut,
        };
        // N2 (#581 review): the plan and the accepted log in one write. A
        // later crash records the log recovering again, and then the
        // satisfied plan closes the gate until a leader's barrier (M1).
        store
            .take_recovery_plan(record)
            .await
            .map_err(|source| RecoveryGateError::Record { group, source })?;
        tracing::warn!(
            node_id = self.node_id,
            plane = group.plane().as_str_name(),
            raft_group_id = group.wire_group_id(),
            leader = plan.leader,
            point = plan.point,
            leader_term,
            cut = cut_restart,
            demote,
            "recovery plan taken: the plan's leader campaigns above the plan term and starts a \
             new history; every other member votes only for it. A replica that cuts its log, \
             leads or campaigns restarts its group first"
        );
        Ok((record, restart))
    }

    /// The answer of a taken plan.
    fn plan_taken(
        &self,
        plan: &RecoveryPlanRequest,
        leader_term: u64,
        leads: bool,
        restart: bool,
    ) -> PlanTaken {
        let report = RecoveryPlanReport {
            raft_group_id: self.group.wire_group_id(),
            node_id: self.node_id,
            outcome: if leads {
                RecoveryPlanOutcome::Leads
            } else {
                RecoveryPlanOutcome::Follows
            },
            leader: plan.leader,
            point: plan.point,
            leader_term,
        };
        if restart {
            PlanTaken::Restart(report)
        } else {
            PlanTaken::Holds(report)
        }
    }

    /// The history event of `record` applied here: the plan is satisfied.
    /// The record is removed if it is still this replica's plan (m1, #581
    /// review), and the gate it set changes: the leader's opens, and a
    /// member's opens unless a crash since the plan left its log recovering,
    /// then it closes until a leader's barrier opens it (M1). Runs outside
    /// OpenRaft's state-machine worker. Returns whether the record went.
    pub(crate) async fn retire_recovery_plan(
        &self,
        record: RecoveryRecord,
    ) -> Result<bool, RecoveryGateError> {
        let group = self.group;
        let store = self
            .store
            .upgrade()
            .ok_or(RecoveryGateError::StoreClosed { group })?;
        let removed = store
            .retire_recovery_plan(record)
            .await
            .map_err(|source| RecoveryGateError::Record { group, source })?;
        let recovering = store.log_state() == GroupLogState::Recovering;
        drop(store);
        if !removed || !self.gate().holds(&record) {
            return Ok(removed);
        }
        if recovering {
            *self.gate() = VoteGate::closed(GroupEvidence::Initialized);
            self.changes.send_if_modified(|()| true);
            tracing::info!(
                node_id = self.node_id,
                plane = group.plane().as_str_name(),
                raft_group_id = group.wire_group_id(),
                "recovery plan satisfied: this replica applied the plan's history event, but \
                 restarted after a crash since it took the plan; it votes again once it has \
                 applied a fresh leader barrier"
            );
        } else {
            self.open(GateOpening::PlanSatisfied).await?;
        }
        Ok(true)
    }

    /// Sets how long a plan may apply nothing before it reports stalled.
    pub(crate) fn set_stall_after(&self, stall_after: Duration) {
        if self.stall_after.set(stall_after).is_err() {
            tracing::debug!("the recovery gate's stall timeout was already set");
        }
    }

    /// How long a plan may apply nothing before it reports stalled.
    pub(crate) fn stall_after(&self) -> Duration {
        self.stall_after
            .get()
            .copied()
            .unwrap_or(RECOVERY_STALL_AFTER)
    }

    /// Reports a wedged plan (m6, #581 review), from the history driver.
    pub(crate) fn stall_plan(&self) {
        if self.gate().stall_plan() {
            self.changes.send_if_modified(|()| true);
            tracing::error!(
                node_id = self.node_id,
                plane = self.group.plane().as_str_name(),
                raft_group_id = self.group.wire_group_id(),
                "recovery plan: nothing applied under this replica's plan for the stall \
                 timeout; the plan's leader may have lost entries its members hold. Send a new \
                 plan to the group's members (ursulactl recover-group)"
            );
        }
    }

    /// Something applied under this replica's plan: it is no longer
    /// wedged (m9, #581 review), from the history driver.
    pub(crate) fn unstall_plan(&self) {
        if self.gate().unstall_plan() {
            self.changes.send_if_modified(|()| true);
        }
    }

    /// Returns whether the gate just stalled.
    fn stall(&self) -> bool {
        let stalled = self.gate().stall();
        if stalled && self.needs_vote_floor() {
            tracing::error!(
                node_id = self.node_id,
                plane = self.group.plane().as_str_name(),
                raft_group_id = self.group.wire_group_id(),
                "recovery stalled with lost vote history; a fresh quorum proof is required; accept-unsynced-loss cannot open this gate"
            );
        } else if stalled {
            tracing::error!(
                node_id = self.node_id,
                plane = self.group.plane().as_str_name(),
                raft_group_id = self.group.wire_group_id(),
                "recovery gate: no leader confirmed a barrier for this gated replica and it \
                 applied nothing; a majority of the group's voters may be gated, so the group \
                 has no leader and refuses writes. To accept the loss of writes acknowledged \
                 after the last fsync, send a recovery plan to every reachable member with \
                 POST /__ursula/raft/{}/recovery/accept-unsynced-loss",
                ursula_proto::admin::RaftTargetKey::from(self.group)
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
                plane = self.group.plane().as_str_name(),
                raft_group_id = self.group.wire_group_id(),
                candidate,
                "recovery gate: refusing a vote until a quorum proves this replica's lost vote"
            ),
            // Any replica, gated or not, refuses a candidate whose vote is
            // older than its own; the answer carries the newer vote.
            Some(floor) if request.vote < floor => tracing::debug!(
                node_id = self.node_id,
                plane = self.group.plane().as_str_name(),
                raft_group_id = self.group.wire_group_id(),
                candidate,
                candidate_vote = %request.vote,
                vote = %floor,
                "refusing a vote request older than this replica's vote"
            ),
            Some(_) => match self.gate().screen(
                candidate,
                request.vote.leader_id().term(),
                request
                    .last_log_id
                    .as_ref()
                    .map(|last| (last.index, last.leader_id.term)),
            ) {
                VoteScreen::Pass => return None,
                VoteScreen::Refuse => tracing::info!(
                    node_id = self.node_id,
                    plane = self.group.plane().as_str_name(),
                raft_group_id = self.group.wire_group_id(),
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
}

#[cfg(test)]
mod tests {

    /// #613: the kept membership is the newer of the log's newest entry at
    /// or below the committed index and the state machine's membership. With
    /// a snapshot only, the log holds none, and the applied one is kept.
    #[test]
    fn the_kept_membership_is_the_newer_of_the_log_and_the_applied_state() {
        use openraft::alias::StoredMembershipOf;
        let stored = |index: u64, voters: &[u64]| {
            StoredMembershipOf::<UrsulaRaftTypeConfig>::new(
                Some(openraft::LogId {
                    leader_id: openraft::vote::leader_id_adv::CommittedLeaderId::new(1, 1),
                    index,
                }),
                openraft::Membership::new_with_defaults(vec![voters.iter().copied().collect()], []),
            )
        };
        let voters = |kept: StoredMembershipOf<UrsulaRaftTypeConfig>| {
            kept.membership().voter_ids().collect::<Vec<_>>()
        };
        // A snapshot install, or a purge: the log holds no membership entry.
        assert_eq!(voters(super::kept_of(None, stored(4, &[1, 2, 3]))), vec![
            1, 2, 3
        ]);
        // The log's entry at or below the committed index is newer.
        assert_eq!(
            voters(super::kept_of(
                Some(stored(7, &[1, 2, 3, 4])),
                stored(4, &[1, 2, 3])
            )),
            vec![1, 2, 3, 4]
        );
        // The applied state is never older than what the log still holds.
        assert_eq!(
            voters(super::kept_of(
                Some(stored(3, &[1, 2])),
                stored(4, &[1, 2, 3])
            )),
            vec![1, 2, 3]
        );
    }
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
    use super::LogReversion;
    use super::PeerGroupLog;
    use super::RecoveryGateStatus;
    use super::ReversionWatch;
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
    fn an_unknown_history_gate_votes_in_a_new_groups_first_election() {
        let mut gate = VoteGate::closed(GroupEvidence::Unknown);
        // The initializer's candidate log is only the membership entry.
        assert_eq!(gate.screen(9, 1, Some((0, 1))), VoteScreen::Pass);
        assert_eq!(gate.screen(9, 1, None), VoteScreen::Pass);
        assert!(!gate.is_open(), "passing a first election opens nothing");
    }

    #[test]
    fn a_gated_replica_refuses_every_vote_once_the_group_holds_entries() {
        let mut gate = VoteGate::closed(GroupEvidence::Unknown);
        assert_eq!(gate.screen(9, 1, Some((11, 1))), VoteScreen::Refuse);
        // Having seen entries, it refuses an index-0 candidate too: that can
        // only be another emptied replica.
        assert_eq!(gate.screen(9, 1, Some((0, 1))), VoteScreen::Refuse);

        let mut gate = VoteGate::closed(GroupEvidence::Unknown);
        gate.observe_append(Some(1));
        assert_eq!(gate.screen(9, 1, None), VoteScreen::Refuse);

        // A replica that lost its unsynced tail knows from the start.
        let mut gate = VoteGate::closed(GroupEvidence::Initialized);
        assert_eq!(gate.screen(9, 1, Some((0, 1))), VoteScreen::Refuse);
        assert_eq!(gate.screen(9, 1, None), VoteScreen::Refuse);
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
        assert_eq!(gate.screen(9, 1, Some((12, 1))), VoteScreen::Refuse);
        gate.open();
        assert!(gate.is_open());
        assert_eq!(gate.status(), RecoveryGateStatus::Open);
        assert_eq!(gate.screen(9, 1, Some((12, 1))), VoteScreen::Pass);
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
            leader_term: 7,
            stalled: false,
        };
        let mut gate = planned.clone();
        assert_eq!(gate.status(), RecoveryGateStatus::Planned);
        assert!(!gate.campaigns(), "a planned replica does not campaign");
        assert_eq!(gate.screen(2, 8, Some((10, 1))), VoteScreen::Pass);
        assert_eq!(gate.screen(2, 8, Some((40, 8))), VoteScreen::Pass);
        assert_eq!(gate.screen(2, 8, None), VoteScreen::Pass);
        assert_eq!(gate.screen(3, 8, Some((40, 8))), VoteScreen::Refuse);
        assert_eq!(gate.screen(3, 8, None), VoteScreen::Refuse);
        // Barriers and replication change nothing: only the plan's history
        // event changes it.
        gate.observe_append(Some(41));
        gate.confirm_barrier(leader(5, 3), 41);
        assert!(!gate.caught_up(follower(Some(41))));
        assert_eq!(gate, planned);
        // m6 and m9 (#581 review): a plan that applied nothing reports the
        // group stalled. Replication alone does not clear it, applied
        // progress does. A closed gate's stall timer leaves it alone.
        assert!(!gate.stall());
        assert!(gate.stall_plan());
        assert_eq!(gate.status(), RecoveryGateStatus::Stalled);
        gate.observe_append(Some(42));
        assert_eq!(gate.status(), RecoveryGateStatus::Stalled);
        assert!(gate.unstall_plan());
        assert_eq!(gate, planned);
    }

    /// N1 (#581 review): a planned member elects the plan's leader only in
    /// a campaign it started after it took the plan: above the plan term.
    #[test]
    fn a_planned_member_votes_for_its_leader_only_above_the_plan_term() {
        let mut gate = VoteGate::Planned {
            leader: 2,
            point: 10,
            leader_term: 7,
            stalled: false,
        };
        // A campaign the leader started before it took the plan, at or
        // below the plan term: refused.
        assert_eq!(gate.screen(2, 6, Some((10, 1))), VoteScreen::Refuse);
        assert_eq!(gate.screen(2, 7, Some((10, 1))), VoteScreen::Refuse);
        // After it took the plan: granted.
        assert_eq!(gate.screen(2, 8, Some((10, 1))), VoteScreen::Pass);
        assert_eq!(gate.screen(3, 9, Some((10, 1))), VoteScreen::Refuse);
        // M1: an entry above the point at or below the plan term came from
        // a leader elected before the plan, here a deposed leader at term 7
        // that extended the plan's leader: refused. Entries the leader
        // appended under the plan carry a later term.
        assert_eq!(gate.screen(2, 9, Some((11, 7))), VoteScreen::Refuse);
        assert_eq!(gate.screen(2, 9, Some((11, 5))), VoteScreen::Refuse);
        assert_eq!(gate.screen(2, 9, Some((12, 8))), VoteScreen::Pass);
        // It takes replication from no leader at or below the plan term.
        assert!(!gate.takes_replication_at(7));
        assert!(gate.takes_replication_at(8));
    }

    /// N1 fix 3 (#581 review): the plan's leader campaigns and votes for
    /// no other candidate.
    #[test]
    fn the_plans_leader_campaigns_and_votes_for_no_one_else() {
        let mut gate = VoteGate::LeadsPlan {
            point: 10,
            leader_term: 7,
            stalled: false,
        };
        assert_eq!(gate.status(), RecoveryGateStatus::LeadsPlan);
        assert!(gate.campaigns());
        assert!(!gate.is_open());
        assert_eq!(gate.screen(3, 9, Some((40, 1))), VoteScreen::Refuse);
        assert_eq!(gate.screen(3, 9, None), VoteScreen::Refuse);
        assert!(!gate.stall());
        assert!(gate.stall_plan());
        assert_eq!(gate.status(), RecoveryGateStatus::Stalled);
        assert!(gate.campaigns(), "a stalled plan's leader still campaigns");
        // M1: a deposed leader at or below the plan term cannot extend it.
        assert!(!gate.takes_replication_at(7));
        assert!(gate.takes_replication_at(8));
        // m7: it holds its own plan only.
        let record = |point, leader_term| crate::log_store::RecoveryRecord {
            leader: 1,
            point,
            leader_term,
            cut: crate::log_store::PlanCut::NoCut,
        };
        assert!(gate.holds(&record(10, 7)));
        assert!(!gate.holds(&record(10, 9)));
        assert!(!gate.holds(&record(11, 7)));
    }

    #[test]
    fn a_cutting_gate_votes_for_no_one_and_never_campaigns() {
        let mut gate = VoteGate::Cutting;
        assert_eq!(gate.status(), RecoveryGateStatus::Cutting);
        assert!(!gate.is_open());
        assert_eq!(gate.screen(2, 1, Some((0, 1))), VoteScreen::Refuse);
        assert_eq!(gate.screen(2, 1, None), VoteScreen::Refuse);
        assert!(!gate.campaigns());
        assert!(!gate.stall());
        assert!(!gate.caught_up(follower(Some(100))));
    }

    #[test]
    fn bootstrap_initializes_only_when_every_voter_is_empty() {
        let empty = || Some(PeerGroupLog::Empty);
        let initialized = || Some(PeerGroupLog::Initialized);
        assert_eq!(
            bootstrap_decision(&[empty(), empty()]),
            BootstrapDecision::Initialize
        );
        assert_eq!(
            bootstrap_decision(&Vec::new()),
            BootstrapDecision::Initialize
        );
        // An empty voter must finish its durable floor before initialization.
        assert_eq!(
            bootstrap_decision(&[empty(), Some(PeerGroupLog::Unprepared)]),
            BootstrapDecision::Wait
        );
        // A silent voter might hold the group: wait for it.
        assert_eq!(
            bootstrap_decision(&[empty(), None]),
            BootstrapDecision::Wait
        );
        // A node that claims other genesis values is no answer: wait until
        // every node claims the same.
        let mismatch = Some(PeerGroupLog::GenesisMismatch {
            claim: ursula_control::samples::sample_genesis_claim(),
        });
        assert_eq!(
            bootstrap_decision(&[empty(), mismatch]),
            BootstrapDecision::Wait
        );
        // One voter with entries or a leader is enough to rejoin.
        assert_eq!(
            bootstrap_decision(&[None, initialized()]),
            BootstrapDecision::Rejoin
        );
        assert_eq!(
            bootstrap_decision(&[empty(), initialized()]),
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

    fn view(voters: &[u64], learners: &[u64]) -> HealView {
        HealView {
            is_leader: true,
            uniform: true,
            stale_joint: false,
            voters: voters.iter().copied().collect(),
            learners: learners.iter().copied().collect(),
            matched: BTreeMap::new(),
            committed: Some(10),
            configured: BTreeSet::from([1, 2, 3]),
        }
    }

    #[test]
    fn the_heal_driver_adds_back_a_missing_voter_and_promotes_it_once_caught_up() {
        assert_eq!(
            plan_heal_step(&view(&[1, 2], &[])),
            Some(HealStep::AddLearner { target: 3 })
        );
        let mut catching_up = view(&[1, 2], &[3]);
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
        // The configured voters, all present: nothing to do. A follower that
        // lost entries stays a voter, and OpenRaft rewinds it in place.
        assert_eq!(plan_heal_step(&view(&[1, 2, 3], &[])), None);
        let mut follower_view = view(&[1, 2], &[]);
        follower_view.is_leader = false;
        assert_eq!(plan_heal_step(&follower_view), None);
        // A voter outside the static config: not a restart repair, leave it
        // to the operator.
        assert_eq!(plan_heal_step(&view(&[1, 2, 4], &[])), None);
    }

    #[test]
    fn the_heal_driver_restores_two_missing_voters_in_a_group_of_five() {
        let five = |voters: &[u64], learners: &[u64]| HealView {
            configured: BTreeSet::from([1, 2, 3, 4, 5]),
            ..view(voters, learners)
        };
        assert_eq!(
            plan_heal_step(&five(&[1, 2, 3], &[])),
            Some(HealStep::AddLearner { target: 4 })
        );
        // 4 caught up while 5 is not back yet: promote 4 alone.
        let mut partly = five(&[1, 2, 3], &[4]);
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
        let mut last = five(&[1, 2, 3, 4], &[5]);
        last.matched.insert(5, Some(10));
        assert_eq!(
            plan_heal_step(&last),
            Some(HealStep::Promote {
                target: 5,
                voters: BTreeSet::from([1, 2, 3, 4, 5]),
            })
        );
    }

    /// The observer reports a follower whose matched index went down within
    /// one leadership, once per drop, and nothing across a leader change or
    /// a membership change.
    #[test]
    fn the_reversion_observer_reports_a_matched_index_that_went_down() {
        let matched = |entries: &[(u64, Option<u64>)]| {
            entries
                .iter()
                .map(|(follower, index)| (*follower, index.map(|index| log_id(3, 1, index))))
                .collect::<BTreeMap<_, _>>()
        };
        let vote = leader(3, 1);
        let mut watch = ReversionWatch::default();
        assert!(
            watch
                .observe(&vote, &matched(&[(2, Some(10)), (3, Some(10))]))
                .is_empty()
        );
        // Matching goes up: nothing.
        assert!(
            watch
                .observe(&vote, &matched(&[(2, Some(12)), (3, Some(12))]))
                .is_empty()
        );
        // OpenRaft rewound 3: its matched index is reset.
        assert_eq!(
            watch.observe(&vote, &matched(&[(2, Some(12)), (3, None)])),
            vec![LogReversion {
                follower: 3,
                matched: 12,
                now: None,
            }]
        );
        // It matches again from below, and catches up: one report only.
        assert!(
            watch
                .observe(&vote, &matched(&[(2, Some(12)), (3, Some(4))]))
                .is_empty()
        );
        assert!(
            watch
                .observe(&vote, &matched(&[(2, Some(12)), (3, Some(12))]))
                .is_empty()
        );
        // A drop seen without the reset in between is reported too.
        assert_eq!(
            watch.observe(&vote, &matched(&[(2, Some(7)), (3, Some(12))])),
            vec![LogReversion {
                follower: 2,
                matched: 12,
                now: Some(7),
            }]
        );
        // A new leader learns afresh what each follower holds.
        let next = leader(4, 1);
        assert!(
            watch
                .observe(&next, &matched(&[(2, Some(1)), (3, Some(1))]))
                .is_empty()
        );
        // A follower that left the membership and came back is no reversion.
        assert!(watch.observe(&next, &matched(&[(2, Some(1))])).is_empty());
        assert!(
            watch
                .observe(&next, &matched(&[(2, Some(1)), (3, None)]))
                .is_empty()
        );
        // A replica that stopped leading starts afresh when it leads again.
        watch.forget();
        assert!(watch.observe(&next, &matched(&[(2, None)])).is_empty());
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
        let gate = GroupRejoin::durable(
            1,
            ursula_shard::RaftTarget::Data(placement.raft_group_id),
            &store,
        )
        .await
        .expect("open the gate");
        assert_eq!(gate.status(), RecoveryGateStatus::AwaitingBarrier);
        assert!(!gate.may_campaign());
        assert!(!gate.holds_group_history());
        assert!(gate.stall());
        let plan =
            |leader, point, cut, last_log_index, last_log_term, current_term| RecoveryPlanRequest {
                point_entry: None,
                leader,
                point,
                cut,
                step: ursula_proto::admin::PlanStep::Lead {
                    highest_term: current_term,
                },
                expected: ReplicaLogView {
                    last_log_index,
                    last_log_term,
                    current_term,
                },
            };
        /// The core of a group that never started: the refusal comes first.
        struct NoCore;
        impl super::PlanCore for NoCore {
            fn disable_elections(&self) {
                unreachable!("refused before the core is asked")
            }
            fn state(
                &self,
            ) -> futures_util::future::BoxFuture<'_, Result<super::CoreState, RecoveryGateError>>
            {
                unreachable!("refused before the core is asked")
            }
        }
        let history = crate::history::GroupHistory::new(
            ursula_shard::RaftTarget::Data(placement.raft_group_id),
            ursula_runtime::HistoryPublisher::default().subscribe(),
        );
        let missing_floor = gate
            .accept_recovery_plan(
                &plan(1, 0, false, None, None, 0),
                &NoCore,
                &history,
                Default::default(),
            )
            .await
            .expect_err("accepting lost entries cannot replace a lost vote history");
        assert!(matches!(
            missing_floor,
            RecoveryGateError::MissingVoteFloor {
                group: ursula_shard::RaftTarget::Data(RaftGroupId(0))
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
            GroupRejoin::durable(
                1,
                ursula_shard::RaftTarget::Data(placement.raft_group_id),
                &store
            )
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
        let gate = GroupRejoin::durable(
            1,
            ursula_shard::RaftTarget::Data(placement.raft_group_id),
            &store,
        )
        .await
        .expect("open the gate");
        assert_eq!(gate.status(), RecoveryGateStatus::AwaitingBarrier);
        assert!(gate.holds_group_history(), "it never initializes again");
        assert_eq!(gate.gate().screen(9, 1, Some((0, 1))), VoteScreen::Refuse);
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
            leader_term: 3,
            cut: crate::log_store::PlanCut::NoCut,
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
        let gate = GroupRejoin::durable(
            1,
            ursula_shard::RaftTarget::Data(placement.raft_group_id),
            &store,
        )
        .await
        .expect("open the gate");
        assert_eq!(gate.status(), RecoveryGateStatus::Planned);
        assert_eq!(gate.gate().screen(2, 4, Some((1, 1))), VoteScreen::Pass);
        assert_eq!(gate.gate().screen(2, 3, Some((1, 1))), VoteScreen::Refuse);
        assert_eq!(gate.gate().screen(3, 4, Some((9, 1))), VoteScreen::Refuse);
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
        assert_eq!(gate.gate().screen(2, 4, Some((1, 1))), VoteScreen::Refuse);
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
        let gate = GroupRejoin::durable(
            1,
            ursula_shard::RaftTarget::Data(placement.raft_group_id),
            &store,
        )
        .await
        .expect("open the gate");
        assert_eq!(gate.status(), RecoveryGateStatus::AwaitingBarrier);
        assert!(!gate.may_campaign());
        store.record_recovery(None).await.expect("drop the plan");
        drop((gate, store));
        wal.shutdown().await.expect("clean shutdown");

        // N2 (#581 review): taking a plan records the plan and the accepted
        // log in one write, so a crash right after keeps both: the replica
        // comes back planned on an initialized log, never open without its
        // plan.
        let wal = RaftWal::start(
            dir.path(),
            WalFsync::Never,
            &ursula_shard::StaticShardMap::new(1, 1).expect("valid topology"),
        )
        .expect("start");
        let store = open(&wal);
        assert_eq!(store.log_state(), GroupLogState::Recovering);
        store
            .take_recovery_plan(record)
            .await
            .expect("take the plan");
        assert_eq!(store.log_state(), GroupLogState::Initialized);
        drop(store);
        drop(wal);
        let wal = RaftWal::start(
            dir.path(),
            WalFsync::Never,
            &ursula_shard::StaticShardMap::new(1, 1).expect("valid topology"),
        )
        .expect("start");
        let store = open(&wal);
        assert_eq!(store.recovery_record(), Some(record));
        let gate = GroupRejoin::durable(
            1,
            ursula_shard::RaftTarget::Data(placement.raft_group_id),
            &store,
        )
        .await
        .expect("open the gate");
        assert_eq!(gate.status(), RecoveryGateStatus::Planned);
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
        let gate = GroupRejoin::durable(
            1,
            ursula_shard::RaftTarget::Data(placement.raft_group_id),
            &store,
        )
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
        let gate = GroupRejoin::durable(
            1,
            ursula_shard::RaftTarget::Data(placement.raft_group_id),
            &store,
        )
        .await
        .expect("open the gate");
        assert_eq!(
            gate.vote_gate_open(),
            cfg!(target_os = "linux"),
            "without a boot id an unclean end reads as a host crash under never"
        );
        drop((gate, store));
        wal.shutdown().await.expect("clean shutdown");

        // m3 (#581 review): a cut whose kept entry the log no longer holds
        // cannot run. The replica still starts: the plan is dropped, the
        // gate stays closed until a new plan or a leader's barrier, and the
        // refusal is reported, typed.
        let wal = RaftWal::start(
            dir.path(),
            WalFsync::Never,
            &ursula_shard::StaticShardMap::new(1, 1).expect("valid topology"),
        )
        .expect("start");
        let store = open(&wal);
        let held = store.last_log_id();
        let keep = log_id(7, 2, 1);
        assert_ne!(held, Some(keep));
        store
            .record_recovery(Some(RecoveryRecord {
                leader: 1,
                point: 1,
                leader_term: 8,
                cut: crate::log_store::PlanCut::Pending { keep },
            }))
            .await
            .expect("record a plan whose cut cannot run");
        let gate = GroupRejoin::durable(
            1,
            ursula_shard::RaftTarget::Data(placement.raft_group_id),
            &store,
        )
        .await
        .expect("an unrunnable cut does not fail the start");
        assert_eq!(gate.cut_refused(), Some((keep, held)));
        assert_eq!(store.recovery_record(), None, "the plan was dropped");
        assert_eq!(gate.status(), RecoveryGateStatus::AwaitingBarrier);
        assert!(!gate.may_campaign());
        assert_eq!(store.last_log_id(), held, "nothing was cut");
        drop((gate, store));
        wal.shutdown().await.expect("clean shutdown");

        // m2 (#581 review): the dropped plan and the recovering log are one
        // durable write, so the next start keeps the gate closed too.
        let wal = RaftWal::start(
            dir.path(),
            WalFsync::Never,
            &ursula_shard::StaticShardMap::new(1, 1).expect("valid topology"),
        )
        .expect("start");
        let store = open(&wal);
        assert_eq!(store.recovery_record(), None);
        assert_eq!(store.log_state(), GroupLogState::Recovering);
        let gate = GroupRejoin::durable(
            1,
            ursula_shard::RaftTarget::Data(placement.raft_group_id),
            &store,
        )
        .await
        .expect("open the gate");
        assert_eq!(gate.cut_refused(), None);
        assert_eq!(gate.status(), RecoveryGateStatus::AwaitingBarrier);
        assert!(!gate.may_campaign());
        drop((gate, store));
        wal.shutdown().await.expect("clean shutdown");
    }

    #[test]
    fn the_heal_driver_flattens_only_a_joint_config_nothing_else_finishes() {
        let mut joint = view(&[1, 2, 3], &[]);
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
mod leader_tick;
mod reconcile;
pub(crate) use attach::GateProbe;
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
pub(crate) use heal::run_rejoin_heal;
pub(crate) use leader_tick::FollowerLogReversions;
#[cfg(test)]
use leader_tick::LogReversion;
#[cfg(test)]
use leader_tick::ReversionWatch;
pub use reconcile::MembershipTargetSource;
pub use reconcile::NoPeerGates;
pub use reconcile::PeerGates;
pub(crate) use reconcile::ReconcileShell;
pub use reconcile::TargetRead;
pub(crate) use reconcile::initial_status;
