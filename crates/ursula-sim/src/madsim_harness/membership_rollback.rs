//! Recovery plans across a membership change that the accepted loss rolls
//! back (#613), on journal-backed replicas under `raft.wal.fsync = never`.
//!
//! Scenario `recovery-plan-membership-rollback`: three voters and a learner,
//! node 4, which joins after the group's first writes. A membership entry is
//! synced before it is acknowledged, so no power loss drops it from a replica
//! that holds it. The variants:
//!
//! - **promotion or removal, cut off**: the leader `A` is cut off with node
//!   4 and appends the promotion of node 4, or the removal of another voter,
//!   and a write of its term that never commits. The other voters elect a
//!   leader, which acknowledges writes, and both lose power and every
//!   unsynced page. They keep the higher vote term, so the plan cuts every
//!   log back to its committed entry, and the change goes with the cut.
//! - **promotion or removal, blackout**: the other voters lose power first,
//!   before they could campaign, then `A` appends the change, which only
//!   node 4 receives, and loses power too. No vote term rises above the
//!   logs, so nothing is cut: the plan elects the longest log, and the
//!   change commits with it. For a promotion that is node 4, a voter only of
//!   the change, which the other voters' own membership does not name: they
//!   must still vote for it (R3, the bug #613 reported).
//! - **completed promotion**: the promotion commits while node 4 is cut off.
//!   Another voter campaigns alone, with no writes, so its vote term rises
//!   above every log term. Node 4 catches up both steps in one append and
//!   loses power at once, so its commit marker stays below them (M1 of the
//!   first #613 review). The old leader goes for good, and the plan cuts:
//!   node 4 reports the promotion, but the cut keeps it a learner.
//! - **learner ahead**: node 4, a learner, knows a later committed entry
//!   than any voter: it restarts cleanly, so its log and marker are durable,
//!   and the voters lose power and every unsynced page. A plan from a voter
//!   would overwrite node 4's committed entries (M-A of the second #613
//!   review); the tool refuses it, typed.
//! - **commit ahead**: a voter campaigns alone, the leader acknowledges
//!   writes and adds a second learner, node 5. That membership entry syncs
//!   every voter's log but not their commit markers, which stay below it,
//!   and node 4 restarts cleanly knowing it is committed. The voters lose
//!   power. The plan cuts every log back to that entry, not to the voters'
//!   markers, led by a voter that holds it (F3 of the third #613 review).
//! - **missed removal**: a voter stops, and its removal commits without
//!   it. The others lose power, and it comes back holding the membership it
//!   had: it stays outside the leader's membership, holding an unsatisfied
//!   plan, and the plan recovers without it.
//! - **two recoveries**: a voter learns writes are committed, on some
//!   seeds purges its log up to its marker, and is cut off: no plan
//!   reaches it. The others lose power and the writes, and a first plan
//!   keeps less than the voter's marker. The new leader writes
//!   past the marker and adds a second learner, whose membership entry
//!   syncs the writes, and the replicas lose power again: a second plan
//!   keeps more than the marker. The voter comes back and refuses the
//!   second history's appends, also below its purge point, and its
//!   snapshot, which replaced the first plan's history, not its own. It
//!   keeps its committed entries, then rejoins as a wiped node (A of the
//!   fourth #613 review, B1 of the fifth).
//!
//! The completed promotion, learner ahead, commit ahead and two recoveries
//! run with production-sized journal segments, so that only membership
//! batches sync and the commit markers stay where they were; the others
//! with the simulator's small ones.
//!
//! After each recovery, one follower misses a write through the leader,
//! which compacts its log, so the follower catches up from a snapshot
//! through the commit screen (F1 of the third #613 review).
//!
//! `ursulactl recover-group`'s planning loop (`next_step`, `send_plan`,
//! `plan_sent`) recovers each group over every replica. Invariants:
//!
//! - `membership_rollback_keeps_history` (G1): every replica holds a prefix
//!   of the acknowledged writes followed by the lost ones, never the cut-off
//!   leader's uncommitted write, never loses an entry it knew was committed,
//!   and once the plan is satisfied holds at least every write acknowledged
//!   before the change; after recovery, a write through the leader reaches
//!   every replica in its membership;
//! - `membership_rollback_never_elects_an_unplanned_leader`: until the plan
//!   is satisfied, every replica that leads holds the plan, and no two
//!   replicas lead in one term;
//! - `membership_rollback_one_voter_set`: once the plan is satisfied, every
//!   replica in the leader's membership holds the leader's membership;
//! - `membership_rollback_plan_recovers`: the planning loop ends with its
//!   first plan satisfied, or, for a learner ahead, refuses typed;
//! - `membership_rollback_fences_only_client_reads`: while a replica holds
//!   an unsatisfied plan it refuses client reads at once and still reports
//!   its usage.
//!
//! Negative controls ([`Controls`]), one per rule:
//!
//! - [`Controls::R3_MEMBER_CHECK`]: members refuse a leader outside their
//!   own membership. The blackout promotion's members refuse node 4 as
//!   `leader_not_voter`, and its first plan is never satisfied.
//! - [`Controls::R2_EFFECTIVE`]: the planner and the plan's leader judge a
//!   cut by the effective membership, and a cut that leaves the leader no
//!   vote keeps the plan. Node 4 leads a first cutting plan as a learner,
//!   the plan wedges, and only a second plan recovers.
//! - [`Controls::COMMIT_BLIND`]: the planner does not see a learner's
//!   committed or applied index, and the loop ignores
//!   `recovery_point_behind`. The plan's leader sends appends, then, once
//!   it compacts, a snapshot over the learner's committed entries; the
//!   learner refuses both, and the loop never recovers. With
//!   [`Controls::COMMIT_BLIND_UNSCREENED`] the learner takes the appends,
//!   and with [`Controls::COMMIT_BLIND_INSTALL_UNSCREENED`] the snapshot.
//! - [`Controls::CHAIN_UNSCREENED`]: the install screen admits a snapshot of
//!   any history that kept entries up to the marker. The voter two
//!   histories behind installs the second plan's snapshot over its
//!   committed entries.

use std::collections::BTreeSet;
use std::time::Duration;

use openraft::ChangeMembers;
use openraft::alias::LogIdOf;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use ursula_config::WalFsync;
use ursula_ctl::recovery::LoopState;
use ursula_ctl::recovery::PlanError;
use ursula_ctl::recovery::PlanMember;
use ursula_ctl::recovery::RecoverError;
use ursula_ctl::recovery::RecoverOutcome;
use ursula_ctl::recovery::Step;
use ursula_ctl::recovery::electorate;
use ursula_ctl::recovery::next_step;
use ursula_ctl::recovery::plan_sent;
use ursula_ctl::recovery::send_follows;
use ursula_ctl::recovery::send_plan;
use ursula_proto::admin::RecoveryPlanRefusal;
use ursula_raft::JournalTuning;
use ursula_raft::UrsulaRaftTypeConfig;
use ursula_runtime::FollowerRead;
use ursula_runtime::GroupEngine;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupInfraError;
use ursula_runtime::ReadStreamRequest;
use ursula_shard::RaftGroupId;

use super::SimEvent;
use super::SimTrace;
use super::SplitMix64;
use super::ThreeNodeRaftSimConfig;
use super::ThreeNodeRaftSimOutcome;
use super::accepted_loss::read_local;
use super::accepted_loss::try_append;
use super::journal_cluster::JOURNAL_GROUPS;
use super::journal_cluster::JournalCluster;
use super::journal_cluster::SimPlanTransport;
use super::journal_cluster::group_placement;
use super::journal_cluster::group_stream;
use super::journal_cluster::lead_every_group;
use super::journal_cluster::member_view;
use super::journal_cluster::metrics;
use super::recovery_wiring::RECOVERY_STALL_AFTER;
use super::recovery_wiring::REVERSION_INVARIANT;

pub(super) const HISTORY_INVARIANT: &str = "membership_rollback_keeps_history";
pub(super) const UNPLANNED_LEADER_INVARIANT: &str =
    "membership_rollback_never_elects_an_unplanned_leader";
pub(super) const VOTER_SET_INVARIANT: &str = "membership_rollback_one_voter_set";
pub(super) const RECOVERS_INVARIANT: &str = "membership_rollback_plan_recovers";
pub(super) const FENCE_INVARIANT: &str = "membership_rollback_fences_only_client_reads";

/// The voters.
const VOTERS: [u64; 3] = [1, 2, 3];
/// The learner that joins after the first writes.
const LEARNER: u64 = 4;
/// Every node.
const NODES: [u64; 5] = [1, 2, 3, 4, 5];
/// A second learner, which joins in the commit-ahead variant only.
const SECOND_LEARNER: u64 = 5;
/// The write the cut-off leader holds uncommitted.
const STALE: &[u8] = b"stale-uncommitted;";
/// How many plans the loop sends at most, as `ursulactl recover-group`.
const MAX_PLANS: usize = 3;
/// How long the loop may take for one group.
const RECOVER_WITHIN: Duration = Duration::from_secs(8);
/// How often the loop observes the replicas.
const POLL: Duration = Duration::from_millis(100);

/// What a negative control turns off. Production runs use the default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Controls {
    /// R2: the planner sees one voter set per replica, the union of its
    /// effective membership, also for a cut, and the server checks the plan's
    /// leader against its effective membership and keeps a plan whose cut
    /// leaves it no vote, as before #613.
    r2_effective: bool,
    /// R3: members refuse a leader outside their own membership, as before
    /// #613.
    r3_member_check: bool,
    /// M-A: the planner does not see a learner's committed index, and the
    /// loop ignores `recovery_point_behind`.
    commit_blind: bool,
    /// M-A: a replica takes an append that overwrites its committed entries.
    overwrite_unscreened: bool,
    /// F1 of the third #613 review: a replica installs a snapshot that
    /// replaces its committed entries; the append screen still runs.
    install_unscreened: bool,
    /// A of the fourth #613 review: the install screen admits a snapshot of
    /// any history that kept entries up to the marker, also one two
    /// histories ahead.
    chain_unscreened: bool,
    /// B1 of the fifth #613 review: the screen reads the store's marker
    /// alone and skips every index at or below the last purged entry.
    purge_unscreened: bool,
    /// Every node runs the production snapshot driver (#669), whose purges
    /// through covering snapshots and restored snapshots land at any time.
    /// Not a defence: the plans must hold with it.
    drivers: Option<super::journal_cluster::DriverPlan>,
}

#[cfg(test)]
impl Controls {
    const NONE: Self = Self {
        r2_effective: false,
        r3_member_check: false,
        commit_blind: false,
        overwrite_unscreened: false,
        install_unscreened: false,
        chain_unscreened: false,
        purge_unscreened: false,
        drivers: None,
    };
    /// Every defence on, and the snapshot driver on every node.
    pub(super) const WITH_DRIVERS: Self = Self {
        drivers: Some(super::deleted_data::DRIVERS),
        ..Self::NONE
    };
    pub(super) const R2_EFFECTIVE: Self = Self {
        r2_effective: true,
        ..Self::NONE
    };
    pub(super) const R3_MEMBER_CHECK: Self = Self {
        r3_member_check: true,
        ..Self::NONE
    };
    pub(super) const COMMIT_BLIND: Self = Self {
        commit_blind: true,
        ..Self::NONE
    };
    pub(super) const COMMIT_BLIND_UNSCREENED: Self = Self {
        commit_blind: true,
        overwrite_unscreened: true,
        ..Self::NONE
    };
    pub(super) const PURGE_UNSCREENED: Self = Self {
        purge_unscreened: true,
        ..Self::NONE
    };
    pub(super) const CHAIN_UNSCREENED: Self = Self {
        chain_unscreened: true,
        ..Self::NONE
    };
    pub(super) const COMMIT_BLIND_INSTALL_UNSCREENED: Self = Self {
        commit_blind: true,
        install_unscreened: true,
        ..Self::NONE
    };
}

/// The defences [`Controls`] turned off for this simulation, until dropped.
struct Switched;

impl Switched {
    fn apply(controls: Controls) -> Self {
        set_defences(controls);
        super::journal_cluster::set_snapshot_drivers(controls.drivers);
        ursula_stream::sim_controls::reset_fired();
        Self
    }
}

impl Drop for Switched {
    fn drop(&mut self) {
        set_defences(Controls::default());
        super::journal_cluster::set_snapshot_drivers(None);
    }
}

fn set_defences(controls: Controls) {
    use ursula_stream::sim_controls::Defence;
    use ursula_stream::sim_controls::set_off;
    set_off(Defence::PlanKeptMembership, controls.r2_effective);
    set_off(Defence::PlanMemberCheck, controls.r3_member_check);
    set_off(Defence::CommitScreen, controls.overwrite_unscreened);
    set_off(Defence::CommitScreenInstall, controls.install_unscreened);
    set_off(Defence::CommitScreenChain, controls.chain_unscreened);
    set_off(Defence::CommitScreenPurged, controls.purge_unscreened);
}

fn fail(trace: &mut SimTrace, invariant: &str, after_event: &str, message: String) -> ! {
    trace.push(SimEvent::InvariantFailed {
        invariant: invariant.to_owned(),
        after_event: after_event.to_owned(),
        message: message.clone(),
    });
    panic!("invariant `{invariant}` failed after `{after_event}`: {message}");
}

/// The membership change the leader appends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Change {
    /// Promote the learner to a voter.
    Promote,
    /// Promote the learner while it is cut off, so the promotion commits
    /// without it, then let it catch up both steps in one append: its commit
    /// marker, never synced on its own, stays below them (M1 of the first
    /// #613 review). The leader then goes for good, and a voter's solo
    /// campaign, with no writes, makes the plan cut.
    PromoteCompleted,
    /// Remove this voter.
    Remove(u64),
    /// No change: the learner knows a later committed entry than any voter
    /// (M-A of the second #613 review).
    LearnerAhead,
    /// A second learner joins: its membership entry syncs every voter's log
    /// but not their commit markers, which stay below it, and the first
    /// learner knows it is committed. A voter's solo campaign makes the plan
    /// cut, which keeps up to that entry (F3 of the third #613 review).
    CommitAhead,
    /// This voter misses its own committed removal: it stays outside the
    /// leader's membership after recovery.
    RemovedMissed(u64),
    /// This voter knows entries are committed that two recovery plans in a
    /// row, which never reach it, replace: it refuses the second plan's
    /// snapshot, and rejoins as a wiped node (A of the fourth #613 review).
    /// With `purged`, it purged its log up to its commit marker before it
    /// stopped, so the screen compares the leader's entries with its last
    /// purged one (B1 of the fifth #613 review).
    TwoRecoveries { behind: u64, purged: bool },
}

/// How the change is rolled back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Loss {
    /// The leader is cut off; a new term commits writes and loses them.
    CutOff,
    /// The other voters lose power before the change, and the leader right
    /// after it: no new term, no vote term above the logs.
    Blackout,
}

/// The plan of one run.
#[derive(Debug)]
struct Plan {
    leader: u64,
    change: Change,
    loss: Loss,
    /// Writes every group commits before the change.
    before: usize,
    /// Writes the new term acknowledges before the power loss.
    lost: usize,
}

impl Plan {
    fn from_seed(seed: u64) -> Self {
        let mut rng = SplitMix64::new(seed ^ 0x6d65_6d62_6572_7321);
        let leader = 1_u64.saturating_add(rng.next_bounded(3));
        let others = VOTERS
            .into_iter()
            .filter(|node| *node != leader)
            .collect::<Vec<_>>();
        let removed = others[usize::try_from(rng.next_bounded(2)).expect("small")];
        // Every nine consecutive seeds cover each variant, as the PR seeds
        // 505-513 do: 505 a blackout promotion (R3, no cut), 506 a learner
        // ahead (M-A), 507 a completed promotion (M1), 510 a commit ahead
        // (F3), 511 a missed removal, 512 two recoveries (A).
        let (change, loss) = match seed % 9 {
            1 => (Change::Promote, Loss::Blackout),
            2 => (Change::LearnerAhead, Loss::Blackout),
            3 => (Change::PromoteCompleted, Loss::CutOff),
            4 => (Change::Promote, Loss::CutOff),
            5 => (Change::Remove(removed), Loss::CutOff),
            6 => (Change::CommitAhead, Loss::CutOff),
            7 => (Change::RemovedMissed(removed), Loss::Blackout),
            8 => (
                Change::TwoRecoveries {
                    behind: removed,
                    purged: (seed / 9) % 2 == 0,
                },
                Loss::Blackout,
            ),
            _ => (Change::Remove(removed), Loss::Blackout),
        };
        Self {
            leader,
            change,
            loss,
            before: 2_usize.saturating_add(usize::try_from(rng.next_bounded(3)).expect("small")),
            lost: 1_usize.saturating_add(usize::try_from(rng.next_bounded(3)).expect("small")),
        }
    }
}

/// What the replicas may serve: the acknowledged writes, then the lost ones.
struct Served {
    before: Vec<u8>,
    lost: Vec<u8>,
}

impl Served {
    fn new(cluster: &JournalCluster, group: u32) -> Self {
        Self {
            before: cluster
                .acknowledged
                .get(&group)
                .cloned()
                .unwrap_or_default(),
            lost: Vec::new(),
        }
    }

    fn allowed(&self) -> Vec<u8> {
        let mut allowed = self.before.clone();
        allowed.extend_from_slice(&self.lost);
        allowed
    }
}

/// Fails G1 when a replica of `group` serves anything but a prefix of the
/// acknowledged writes followed by the lost ones, or, once `satisfied`,
/// less than every write acknowledged before the change.
async fn assert_history(
    cluster: &JournalCluster,
    group: u32,
    served: &Served,
    satisfied: bool,
    trace: &mut SimTrace,
    after: &str,
) {
    let allowed = served.allowed();
    for node_id in NODES {
        if !cluster.engines.contains_key(&(group, node_id)) {
            continue;
        }
        let bytes = read_local(cluster, group, node_id).await;
        if bytes.windows(STALE.len()).any(|window| window == STALE) {
            fail(
                trace,
                HISTORY_INVARIANT,
                after,
                format!(
                    "node {node_id} group {group} applied the cut-off leader's uncommitted write, \
                     which the lost history overwrote"
                ),
            );
        }
        if !allowed.starts_with(&bytes) {
            fail(
                trace,
                HISTORY_INVARIANT,
                after,
                format!(
                    "node {node_id} group {group} serves {:?}, which is no prefix of the \
                     acknowledged writes {:?}",
                    String::from_utf8_lossy(&bytes),
                    String::from_utf8_lossy(&allowed)
                ),
            );
        }
        if satisfied && bytes.len() < served.before.len() {
            fail(
                trace,
                HISTORY_INVARIANT,
                after,
                format!(
                    "node {node_id} group {group} lost a write acknowledged before the change: \
                     it serves {:?}",
                    String::from_utf8_lossy(&bytes)
                ),
            );
        }
    }
}

/// Fails when a replica of `group` won a leadership after the loss, at a
/// term above `loss_term`, without holding a plan, before a plan's history
/// event above `point` applied on it, or when two replicas lead in one term.
/// Before any plan was sent (`point` is `None`), no leadership above
/// `loss_term` may exist at all. A leader of a term at or below `loss_term`
/// was elected before the loss: it steps down once it sees the higher vote
/// term. Every replica in the leader state is checked.
fn assert_no_unplanned_leader(
    cluster: &JournalCluster,
    group: u32,
    loss_term: u64,
    point: Option<u64>,
    trace: &mut SimTrace,
    after: &str,
) {
    let mut terms = std::collections::BTreeMap::new();
    for leader in leaders(cluster, group) {
        let term = metrics(cluster, group, leader).current_term;
        if let Some(other) = terms.insert(term, leader) {
            fail(
                trace,
                UNPLANNED_LEADER_INVARIANT,
                after,
                format!("nodes {other} and {leader} both lead group {group} at term {term}"),
            );
        }
        if term <= loss_term {
            continue;
        }
        let history = cluster.engines[&(group, leader)].history();
        let satisfied = point.is_some_and(|point| {
            history
                .applied()
                .is_some_and(|event| event.event_index > point)
        });
        if history.plan().is_none() && !satisfied {
            fail(
                trace,
                UNPLANNED_LEADER_INVARIANT,
                after,
                format!(
                    "node {leader} leads group {group} at term {term} without a recovery plan \
                     (last plan point {point:?}): the loss stalled the group, so only a plan \
                     may elect a leader"
                ),
            );
        }
    }
}

/// Every running replica of `group` in the leader state.
fn leaders(cluster: &JournalCluster, group: u32) -> Vec<u64> {
    NODES
        .into_iter()
        .filter(|node_id| {
            cluster.engines.contains_key(&(group, *node_id))
                && metrics(cluster, group, *node_id).state == openraft::ServerState::Leader
        })
        .collect()
}

/// The leader of the highest term among `group`'s running replicas.
fn newest_leader(cluster: &JournalCluster, group: u32) -> Option<u64> {
    leaders(cluster, group)
        .into_iter()
        .max_by_key(|node_id| metrics(cluster, group, *node_id).current_term)
}

/// The replicas of `group` as the tool reads them: every running replica
/// but the `hidden` ones, which it does not reach, as blind to membership
/// as the control asks.
async fn observe(
    cluster: &JournalCluster,
    group: u32,
    controls: Controls,
    hidden: &[u64],
) -> Vec<PlanMember> {
    let mut members = Vec::new();
    for node_id in NODES {
        if cluster.engines.contains_key(&(group, node_id)) && !hidden.contains(&node_id) {
            members.push(blind(member_view(cluster, group, node_id).await, controls));
        }
    }
    members
}

/// `member` as a negative control has the planner see it: under R2, one
/// voter set, the union of the effective membership's configs, also for a
/// cut; under M-A, no committed or applied index for a learner.
fn blind(member: PlanMember, controls: Controls) -> PlanMember {
    let member = if controls.r2_effective {
        let union = member
            .voter_configs
            .iter()
            .flatten()
            .copied()
            .collect::<BTreeSet<_>>();
        PlanMember {
            voter_configs: vec![union.clone()],
            committed_voter_configs: vec![union],
            ..member
        }
    } else {
        member
    };
    let learner = !member
        .voter_configs
        .iter()
        .chain(member.committed_voter_configs.iter())
        .any(|voters| voters.contains(&member.node_id));
    if controls.commit_blind && learner {
        PlanMember {
            committed: None,
            committed_entry: None,
            last_applied: None,
            ..member
        }
    } else {
        member
    }
}

/// While `group`'s replicas hold an unsatisfied plan, each refuses a client
/// read at once and still reports its usage and state gauges, which
/// `/__ursula/usage` and `/__ursula/metrics` read (M-B of the second #613
/// review). Returns how many replicas it probed.
async fn assert_fences_only_client_reads(
    cluster: &mut JournalCluster,
    group: u32,
    point: u64,
    trace: &mut SimTrace,
) -> usize {
    let mut probed = 0_usize;
    for node_id in NODES {
        let Some(engine) = cluster.engines.get_mut(&(group, node_id)) else {
            continue;
        };
        let history = engine.history();
        let unsatisfied = history.plan().is_some()
            && !history
                .applied()
                .is_some_and(|event| event.event_index > point);
        // A leader keeps the leader fence, which waits a moment for the
        // event it proposes.
        let leads = openraft::rt::WatchReceiver::borrow_watched(&engine.raft_handle().metrics())
            .state
            == openraft::ServerState::Leader;
        if !unsatisfied || leads {
            continue;
        }
        let started = madsim::time::Instant::now();
        // A catch-up read and an immutable chunk's read alike.
        let mut refused = true;
        let mut read = Ok(());
        for follower in [FollowerRead::Prefix, FollowerRead::Immutable] {
            let answer = engine
                .route_read_stream(
                    ReadStreamRequest {
                        stream_id: group_stream(group),
                        offset: 0,
                        max_len: 1 << 10,
                        now_ms: 0,
                        leader_only: false,
                        read_index: None,
                        follower,
                    },
                    group_placement(group),
                )
                .await;
            let this = matches!(
                answer,
                Err(GroupEngineError::Infra(
                    GroupInfraError::RecoveryPending { .. }
                ))
            );
            if !this {
                refused = false;
                read = answer.map(|_route| ()).map_err(|error| (follower, error));
            }
        }
        // A replica whose plan was satisfied meanwhile serves the read.
        let still = engine.history().plan().is_some()
            && !engine
                .history()
                .applied()
                .is_some_and(|event| event.event_index > point);
        if still && (!refused || started.elapsed() >= Duration::from_secs(1)) {
            fail(
                trace,
                FENCE_INVARIANT,
                "recovery_plan_sent",
                format!(
                    "node {node_id} group {group} holds an unsatisfied plan, but a client read \
                     was not refused at once: {read:?}"
                ),
            );
        }
        if let Err(error) = engine.state_gauges(group_placement(group)).await {
            fail(
                trace,
                FENCE_INVARIANT,
                "recovery_plan_sent",
                format!(
                    "node {node_id} group {group} holds a plan and refuses its state gauges: \
                     {error}"
                ),
            );
        }
        if let Err(error) = engine.bucket_usage(group_placement(group)).await {
            fail(
                trace,
                FENCE_INVARIANT,
                "recovery_plan_sent",
                format!(
                    "node {node_id} group {group} holds a plan and refuses its usage report: \
                     {error}"
                ),
            );
        }
        probed = probed.saturating_add(1);
    }
    probed
}

/// How many appends a replica refused because they would overwrite its
/// committed entries, as a failure message reports it.
fn overwrite_refusals() -> String {
    use ursula_stream::sim_controls::Defence;
    use ursula_stream::sim_controls::fired;
    format!(
        "overwrite refusals: {}, install refusals: {}",
        fired(Defence::CommitScreen),
        fired(Defence::CommitScreenInstall)
    )
}

/// What one group's recovery did.
#[derive(Debug)]
struct Recovered {
    leader: u64,
    point: u64,
    cut: bool,
    /// The cut kept up to a later committed entry another replica reported
    /// than the leader's own marker.
    raised: bool,
    /// Running replicas outside the leader's membership.
    outside: usize,
    plans_sent: usize,
    /// Replicas probed for the client-read fence while planned.
    fence_probes: usize,
    first: Option<ursula_ctl::recovery::GroupPlan>,
    refusals: Vec<RecoveryPlanRefusal>,
}

/// `ursulactl recover-group`'s planning loop over `group`: observe every
/// replica, decide with [`next_step`], send with [`send_plan`] and record
/// with [`plan_sent`], checking meanwhile that no replica leads without the
/// plan.
async fn recover(
    cluster: &mut JournalCluster,
    group: u32,
    loss_term: u64,
    controls: Controls,
    hidden: &[u64],
    trace: &mut SimTrace,
) -> Result<Recovered, RecoverError> {
    let mut state = LoopState::default();
    let mut fence_probes = 0_usize;
    // What a failure reports: the first plan and every refusal, so that a
    // negative control can assert the mechanism it switched off.
    let mut first = None;
    let mut refusals = Vec::new();
    let deadline = madsim::time::Instant::now() + RECOVER_WITHIN;
    loop {
        let point = state.sent.map(|sent| sent.plan.point);
        assert_no_unplanned_leader(cluster, group, loss_term, point, trace, "recovery_planning");
        let members = observe(cluster, group, controls, hidden).await;
        match next_step(&mut state, &members, MAX_PLANS)? {
            Step::Done(RecoverOutcome::Satisfied {
                plan,
                plans_sent,
                outside,
                stopped: _,
            }) => {
                if !outside.is_empty() {
                    tracing::info!(?outside, group, "replicas outside the leader's membership");
                }
                return Ok(Recovered {
                    leader: plan.leader,
                    point: plan.point,
                    cut: plan.cut,
                    raised: plan.point_entry.is_some(),
                    outside: outside.len(),
                    plans_sent,
                    fence_probes,
                    first,
                    refusals,
                });
            }
            // The loop stops without its plan satisfied: some replica it
            // reached never applied the plan's history event.
            Step::Done(outcome) => fail(
                trace,
                RECOVERS_INVARIANT,
                "recovery_planning",
                format!(
                    "group {group}: the planning loop ended with {outcome:?}, not with its plan \
                     satisfied ({}, first plan {first:?}, refusals {refusals:?})",
                    overwrite_refusals()
                ),
            ),
            Step::Wait => {}
            Step::Plan(plan) => {
                state.plans_sent = state.plans_sent.saturating_add(1);
                first.get_or_insert(plan);
                cluster.plan_leaders.insert(plan.leader);
                let mut sent =
                    send_plan(&mut SimPlanTransport { cluster, group }, plan, &members).await;
                if let Ok(sent) = &mut sent {
                    for error in &sent.refused {
                        tracing::info!(%error, group, "a member refused the plan");
                        refusals.extend(error.refusal());
                    }
                    // Before the second #613 review the tool ignored a
                    // member's `recovery_point_behind`.
                    if controls.commit_blind {
                        sent.refused.retain(|error| {
                            error.refusal() != Some(RecoveryPlanRefusal::RecoveryPointBehind)
                        });
                    }
                }
                plan_sent(&mut state, plan, sent, &electorate(&plan, &members))?;
                fence_probes = fence_probes.saturating_add(
                    assert_fences_only_client_reads(cluster, group, plan.point, trace).await,
                );
                // F1 of the third #613 review: the plan's leader compacts, so
                // it sends the learner its committed state as a snapshot,
                // not as appends the screen refuses.
                if controls.commit_blind && state.plans_sent == 1 {
                    compact_planned_leader(cluster, group, plan.leader).await;
                }
            }
            Step::Resume { plan, leader_term } => {
                let sent = send_follows(
                    &mut SimPlanTransport { cluster, group },
                    plan,
                    leader_term,
                    &members,
                )
                .await;
                plan_sent(&mut state, plan, Ok(sent), &electorate(&plan, &members))?;
            }
        }
        if madsim::time::Instant::now() >= deadline {
            fail(
                trace,
                RECOVERS_INVARIANT,
                "recovery_planning",
                format!(
                    "group {group}: the planning loop timed out after {} plans ({}, first plan \
                     {first:?}, refusals {refusals:?})",
                    state.plans_sent,
                    overwrite_refusals()
                ),
            );
        }
        madsim::time::sleep(POLL).await;
    }
}

/// The membership `node_id` holds for `group`: its voter configs and its
/// learners.
fn membership_of(
    cluster: &JournalCluster,
    group: u32,
    node_id: u64,
) -> (Vec<BTreeSet<u64>>, BTreeSet<u64>) {
    let replica = metrics(cluster, group, node_id);
    let membership = replica.membership_config.membership();
    (
        membership.get_joint_config().clone(),
        membership.learner_ids().collect(),
    )
}

/// Waits until every replica in the leader's membership holds the leader's
/// membership, and fails the invariant if it does not settle.
async fn assert_one_voter_set(
    cluster: &JournalCluster,
    group: u32,
    trace: &mut SimTrace,
    context: &str,
) {
    let deadline = madsim::time::Instant::now() + Duration::from_secs(5);
    loop {
        let Some(leader) = newest_leader(cluster, group) else {
            assert!(
                madsim::time::Instant::now() < deadline,
                "{context}: group {group} lost its leader after the plan"
            );
            madsim::time::sleep(Duration::from_millis(25)).await;
            continue;
        };
        let (configs, learners) = membership_of(cluster, group, leader);
        let members = configs
            .iter()
            .flatten()
            .chain(learners.iter())
            .copied()
            .collect::<BTreeSet<_>>();
        let differing = members
            .iter()
            .copied()
            // A node that stays down holds nothing to compare.
            .filter(|node_id| {
                cluster.engines.contains_key(&(group, *node_id))
                    && membership_of(cluster, group, *node_id)
                        != (configs.clone(), learners.clone())
            })
            .collect::<Vec<_>>();
        if differing.is_empty() {
            return;
        }
        if madsim::time::Instant::now() >= deadline {
            fail(
                trace,
                VOTER_SET_INVARIANT,
                "recovery_plan_satisfied",
                format!(
                    "group {group}: nodes {differing:?} hold another membership than the \
                     leader {leader}'s {configs:?} with learners {learners:?}"
                ),
            );
        }
        madsim::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Writes through the group's leader after recovery and waits until every
/// running replica in its membership serves exactly what the leader serves,
/// the write included: one history, replicated from one leader (m2 of the
/// #613 review). One follower misses the write and the leader compacts past
/// it, so the follower catches up from a snapshot through the commit screen
/// (F1 of the third #613 review). Returns how many snapshots it was sent.
async fn assert_writes_after_recovery(
    cluster: &mut JournalCluster,
    group: u32,
    trace: &mut SimTrace,
    context: &str,
) -> usize {
    let payload = format!("after-recovery-{group};").into_bytes();
    let deadline = madsim::time::Instant::now() + Duration::from_secs(5);
    let Some(leader) = newest_leader(cluster, group) else {
        panic!("{context}: group {group} has no leader after the plan");
    };
    let (configs, learners) = membership_of(cluster, group, leader);
    let members = configs
        .iter()
        .flatten()
        .chain(learners.iter())
        .copied()
        .filter(|node_id| cluster.engines.contains_key(&(group, *node_id)))
        .collect::<BTreeSet<_>>();
    // A follower the leader commits without: every voter set keeps a
    // majority of running voters without it.
    let lagging = members.iter().copied().find(|node_id| {
        *node_id != leader
            && configs.iter().all(|voters| {
                let running = voters
                    .iter()
                    .filter(|voter| **voter != *node_id && members.contains(*voter))
                    .count();
                running.saturating_mul(2) > voters.len()
            })
    });
    if let Some(lagging) = lagging {
        cluster.policy.partition_bidirectional(leader, lagging);
    }
    let sent_before = lagging.map(|node_id| cluster.full_snapshots(group, node_id));
    loop {
        if try_append(cluster, group, leader, &payload).await {
            break;
        }
        assert!(
            madsim::time::Instant::now() < deadline,
            "{context}: group {group} takes no write after the plan"
        );
        madsim::time::sleep(Duration::from_millis(50)).await;
    }
    if let Some(lagging) = lagging {
        cluster.snapshot_and_purge_node(group, leader, 0).await;
        cluster.policy.heal_bidirectional(leader, lagging);
    }
    loop {
        let expected = read_local(cluster, group, leader).await;
        let mut differing = Vec::new();
        for node_id in &members {
            if read_local(cluster, group, *node_id).await != expected {
                differing.push(*node_id);
            }
        }
        if expected.ends_with(&payload) && differing.is_empty() {
            break;
        }
        if madsim::time::Instant::now() >= deadline {
            fail(
                trace,
                HISTORY_INVARIANT,
                "written_after_recovery",
                format!(
                    "group {group}: after a write through leader {leader}, nodes {differing:?} \
                     of its membership serve other bytes than its {:?} ({})",
                    String::from_utf8_lossy(&expected),
                    overwrite_refusals()
                ),
            );
        }
        madsim::time::sleep(Duration::from_millis(50)).await;
    }
    match (lagging, sent_before) {
        (Some(lagging), Some(before)) => {
            let sent = cluster
                .full_snapshots(group, lagging)
                .saturating_sub(before);
            assert!(
                sent > 0,
                "{context}: group {group}: node {lagging} caught up without a snapshot"
            );
            sent
        }
        _ => 0,
    }
}

/// The plan's leader, once it leads and applied the plan's event, snapshots
/// and purges its log, so the replicas behind it catch up from a snapshot.
async fn compact_planned_leader(cluster: &JournalCluster, group: u32, leader: u64) {
    let deadline = madsim::time::Instant::now() + Duration::from_secs(3);
    while madsim::time::Instant::now() < deadline {
        if cluster.engines.contains_key(&(group, leader))
            && metrics(cluster, group, leader).state == openraft::ServerState::Leader
            && cluster.engines[&(group, leader)]
                .history()
                .applied()
                .is_some()
        {
            cluster.snapshot_and_purge_node(group, leader, 0).await;
            return;
        }
        madsim::time::sleep(Duration::from_millis(25)).await;
    }
}

pub(super) async fn run_membership_rollback_inner(
    config: ThreeNodeRaftSimConfig,
    controls: Controls,
) -> ThreeNodeRaftSimOutcome {
    let _switched = Switched::apply(controls);
    let plan = Plan::from_seed(config.seed);
    let mut trace = SimTrace::default();
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });
    let context = format!("seed {} {plan:?}", config.seed);
    // Production-sized segments where commit markers matter: a simulated
    // journal otherwise rotates often, and each rotation syncs what came
    // before it, markers included. Then only membership batches sync, as in
    // production.
    let tuning = match plan.change {
        Change::PromoteCompleted
        | Change::LearnerAhead
        | Change::CommitAhead
        | Change::TwoRecoveries { .. } => JournalTuning {
            segment_bytes: 64 * 1024 * 1024,
            ..JournalTuning::new(WalFsync::Never)
        },
        Change::Promote | Change::Remove(_) | Change::RemovedMissed(_) => {
            JournalTuning::new(WalFsync::Never)
        }
    };
    let spares = if matches!(
        plan.change,
        Change::CommitAhead | Change::TwoRecoveries { .. }
    ) {
        2
    } else {
        1
    };
    let mut cluster = JournalCluster::start_with_spares(
        &format!("recovery-plan-membership-{}", config.seed),
        tuning,
        3,
        spares,
    )
    .await;
    for group in JOURNAL_GROUPS {
        cluster.append(group, plan.before).await;
    }
    let a = plan.leader;
    lead_every_group(&cluster, a, &context).await;
    trace.push(SimEvent::LeaderElected { leader_id: a });

    // The learner joins. Its membership entry is synced, and with it every
    // write before it.
    cluster.start_node(LEARNER).await;
    for group in JOURNAL_GROUPS {
        cluster.engines[&(group, a)]
            .raft_handle()
            .add_learner(
                LEARNER,
                openraft::BasicNode::new(format!("node-{LEARNER}")),
                true,
            )
            .await
            .unwrap_or_else(|error| panic!("{context}: group {group}: add the learner: {error}"));
    }
    cluster.wait_gates_open(Duration::from_secs(5)).await;
    let mut served = JOURNAL_GROUPS
        .iter()
        .map(|group| (*group, Served::new(&cluster, *group)))
        .collect::<std::collections::BTreeMap<_, _>>();

    let others = VOTERS
        .into_iter()
        .filter(|node| *node != a)
        .collect::<Vec<_>>();
    // Whether the promoted learner came back with its marker below its
    // promotion: it reports the promotion, but a cut keeps the learner.
    let mut marker_lagged = false;
    // The committed entries the learner holds ahead of every voter.
    let mut ahead = std::collections::BTreeMap::new();
    match plan.change {
        Change::PromoteCompleted => {
            marker_lagged =
                promote_completed(&mut cluster, &plan, &others, &mut served, &context).await;
        }
        Change::LearnerAhead => {
            ahead = learner_ahead(&mut cluster, &plan, &mut served, &context).await;
        }
        Change::CommitAhead => {
            ahead = commit_ahead(&mut cluster, &plan, &others, &mut served, &context).await;
        }
        Change::RemovedMissed(removed) => {
            removed_missed(&mut cluster, &plan, removed, &mut served, &context).await;
        }
        Change::TwoRecoveries { behind, purged } => {
            two_recoveries(
                &mut cluster,
                &plan,
                behind,
                purged,
                tuning,
                controls,
                &mut trace,
                &context,
            )
            .await;
            let unaccounted = cluster.reversions.unaccounted();
            if !unaccounted.is_empty() {
                fail(
                    &mut trace,
                    REVERSION_INVARIANT,
                    "scenario_end",
                    format!(
                        "seed {}: replicas with an open gate lost acknowledged entries: {unaccounted:?}",
                        config.seed
                    ),
                );
            }
            return ThreeNodeRaftSimOutcome {
                seed: config.seed,
                leader_id: a,
                target_node_id: Some(behind),
                appended_log_index: 0,
                trace,
            };
        }
        Change::Promote | Change::Remove(_) => {
            roll_back_a_minority_change(&mut cluster, &plan, &others, &mut served, &context).await;
        }
    }
    trace.push(SimEvent::FaultApplied {
        phase: "membership_change_appended".to_owned(),
    });
    cluster.policy.clear();
    trace.push(SimEvent::FaultApplied {
        phase: "quorum_lost_power".to_owned(),
    });

    // Every vote term at the loss: a leadership above them is won after it.
    let loss_terms = JOURNAL_GROUPS
        .iter()
        .map(|group| {
            let term = NODES
                .iter()
                .filter(|node_id| cluster.engines.contains_key(&(*group, **node_id)))
                .map(|node_id| metrics(&cluster, *group, *node_id).current_term)
                .max()
                .unwrap_or(0);
            (*group, term)
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    // The group stalls: no leader, until the operator plans.
    let stall_until = madsim::time::Instant::now() + RECOVERY_STALL_AFTER.saturating_mul(2);
    while madsim::time::Instant::now() < stall_until {
        for group in JOURNAL_GROUPS {
            assert_no_unplanned_leader(
                &cluster,
                group,
                loss_terms[&group],
                None,
                &mut trace,
                "quorum_lost",
            );
        }
        madsim::time::sleep(Duration::from_millis(50)).await;
    }
    for group in JOURNAL_GROUPS {
        assert_history(
            &cluster,
            group,
            &served[&group],
            false,
            &mut trace,
            "quorum_lost",
        )
        .await;
    }

    let mut cut = false;
    let mut raised = false;
    let mut outside = 0_usize;
    let mut installed = 0_usize;
    let mut plans = 0_usize;
    let mut fenced = 0_usize;
    let mut leader = a;
    for group in JOURNAL_GROUPS {
        let recovered = recover(
            &mut cluster,
            group,
            loss_terms[&group],
            controls,
            &[],
            &mut trace,
        )
        .await;
        // Whatever the loop did, the learner keeps what it knew committed.
        assert_learner_kept(&cluster, &ahead, &mut trace).await;
        let recovered = match recovered {
            Err(RecoverError::Plan(PlanError::BelowCommitted {
                node_id,
                committed,
                point,
            })) if plan.change == Change::LearnerAhead => {
                // The tool refuses, typed, and sends nothing.
                let held = NODES
                    .iter()
                    .filter(|node_id| {
                        cluster
                            .engines
                            .get(&(group, **node_id))
                            .is_some_and(|engine| engine.history().plan().is_some())
                    })
                    .copied()
                    .collect::<Vec<_>>();
                assert!(
                    held.is_empty(),
                    "{context}: group {group}: nodes {held:?} took a plan the tool refused"
                );
                trace.push(SimEvent::MembershipRollbackRefused {
                    node_id,
                    committed,
                    point,
                });
                continue;
            }
            Ok(recovered) if plan.change == Change::LearnerAhead => fail(
                &mut trace,
                RECOVERS_INVARIANT,
                "recovery_planning",
                format!(
                    "{context}: group {group}: the tool planned below the learner's committed \
                     entry: {recovered:?}"
                ),
            ),
            Ok(recovered) if recovered.plans_sent > 1 => fail(
                &mut trace,
                RECOVERS_INVARIANT,
                "recovery_planning",
                format!(
                    "{context}: group {group}: the first plan was not satisfied; it took {} \
                     plans, the first {:?}, refusals {:?}",
                    recovered.plans_sent, recovered.first, recovered.refusals
                ),
            ),
            Ok(recovered) => recovered,
            Err(error) => fail(
                &mut trace,
                RECOVERS_INVARIANT,
                "recovery_planning",
                format!(
                    "{context}: group {group}: the planning loop stopped: {error} ({})",
                    overwrite_refusals()
                ),
            ),
        };
        tracing::info!(?recovered, group, "{context}: the plan is satisfied");
        assert_history(
            &cluster,
            group,
            &served[&group],
            true,
            &mut trace,
            "recovery_plan_satisfied",
        )
        .await;
        assert_one_voter_set(&cluster, group, &mut trace, &context).await;
        installed = installed.saturating_add(
            assert_writes_after_recovery(&mut cluster, group, &mut trace, &context).await,
        );
        cut |= recovered.cut;
        raised |= recovered.raised;
        outside = outside.saturating_add(recovered.outside);
        plans = plans.saturating_add(recovered.plans_sent);
        fenced = fenced.saturating_add(recovered.fence_probes);
        leader = recovered.leader;
        assert!(recovered.point > 0, "{context}: a plan point");
    }
    if plan.change != Change::LearnerAhead {
        trace.push(SimEvent::MembershipRollbackRecovered {
            leader,
            cut,
            promoted: matches!(plan.change, Change::Promote | Change::PromoteCompleted),
            marker_lagged,
            raised,
            outside: u64::try_from(outside).unwrap_or(u64::MAX),
            installed: u64::try_from(installed).unwrap_or(u64::MAX),
            plans: u64::try_from(plans).unwrap_or(u64::MAX),
            fenced: u64::try_from(fenced).unwrap_or(u64::MAX),
        });
    }
    let unaccounted = cluster.reversions.unaccounted();
    if !unaccounted.is_empty() {
        fail(
            &mut trace,
            REVERSION_INVARIANT,
            "scenario_end",
            format!(
                "seed {}: replicas with an open gate lost acknowledged entries: {unaccounted:?}",
                config.seed
            ),
        );
    }
    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: leader,
        target_node_id: Some(a),
        appended_log_index: 0,
        trace,
    }
}

/// The leader appends a change that only the learner receives, then loses
/// it from a majority as `plan.loss` says.
async fn roll_back_a_minority_change(
    cluster: &mut JournalCluster,
    plan: &Plan,
    others: &[u64],
    served: &mut std::collections::BTreeMap<u32, Served>,
    context: &str,
) {
    let a = plan.leader;
    for voter in others {
        cluster.policy.partition_bidirectional(a, *voter);
        cluster.policy.partition_bidirectional(LEARNER, *voter);
    }
    if plan.loss == Loss::Blackout {
        // The other voters lose power first, before they could campaign:
        // no vote term rises above the logs, so the plan cuts nothing.
        for node in others {
            cluster.stop_node(*node).await;
        }
        for node in others {
            cluster.wals[node].power_loss_losing_unsynced().await;
        }
    }
    for group in JOURNAL_GROUPS {
        let change = match plan.change {
            Change::Promote
            | Change::PromoteCompleted
            | Change::LearnerAhead
            | Change::CommitAhead => ChangeMembers::AddVoterIds(BTreeSet::from([LEARNER])),
            Change::Remove(voter)
            | Change::RemovedMissed(voter)
            | Change::TwoRecoveries { behind: voter, .. } => {
                ChangeMembers::RemoveVoters(BTreeSet::from([voter]))
            }
        };
        let raft = cluster.engines[&(group, a)].raft_handle();
        // The change never commits: give up on its answer.
        if let Ok(answer) = madsim::time::timeout(
            Duration::from_millis(300),
            raft.change_membership(change, false),
        )
        .await
        {
            tracing::info!(?answer, group, "the change answered");
        }
        for node_id in [a, LEARNER] {
            let (configs, _) = membership_of(cluster, group, node_id);
            assert_eq!(
                configs.len(),
                2,
                "{context}: group {group}: node {node_id} holds the joint change"
            );
        }
    }

    match plan.loss {
        Loss::CutOff => {
            for group in JOURNAL_GROUPS {
                // Dropped by the cut whether or not it is acknowledged.
                if try_append(cluster, group, a, STALE).await {
                    tracing::info!(group, "the cut-off leader acknowledged its write");
                }
            }
            // The others elect a leader, which acknowledges writes.
            for group in JOURNAL_GROUPS {
                let deadline = madsim::time::Instant::now() + Duration::from_secs(10);
                let leader = loop {
                    if let Some(leader) = others.iter().copied().find(|node| {
                        metrics(cluster, group, *node).state == openraft::ServerState::Leader
                    }) {
                        break leader;
                    }
                    assert!(
                        madsim::time::Instant::now() < deadline,
                        "{context}: group {group}: the others elected no leader"
                    );
                    madsim::time::sleep(Duration::from_millis(25)).await;
                };
                for index in 0..plan.lost {
                    let payload = format!("lost-{group}-{index};").into_bytes();
                    assert!(
                        try_append(cluster, group, leader, &payload).await,
                        "{context}: group {group}: the new leader acknowledges a write"
                    );
                    if let Some(served) = served.get_mut(&group) {
                        served.lost.extend_from_slice(&payload);
                    }
                }
            }
            for node in others {
                cluster.stop_node(*node).await;
            }
            for node in others {
                cluster.wals[node].power_loss_losing_unsynced().await;
            }
            for node in others {
                cluster.start_node(*node).await;
            }
        }
        Loss::Blackout => {
            cluster.stop_node(a).await;
            cluster.wals[&a].power_loss_losing_unsynced().await;
            for node in VOTERS {
                cluster.start_node(node).await;
            }
        }
    }
}

/// The learner's promotion commits while it is cut off. Another voter, cut
/// off alone, campaigns meanwhile, so its vote term rises above every log
/// term, durably. The learner then catches up both steps in one append,
/// which syncs them, and loses power within milliseconds, before any later
/// write syncs the commit marker after them. The leader goes for good, and
/// the other voters lose power too. Returns whether the learner came back
/// with its marker below its promotion.
async fn promote_completed(
    cluster: &mut JournalCluster,
    plan: &Plan,
    others: &[u64],
    _served: &mut std::collections::BTreeMap<u32, Served>,
    context: &str,
) -> bool {
    let a = plan.leader;
    for node in VOTERS {
        cluster.policy.partition_bidirectional(LEARNER, node);
    }
    for group in JOURNAL_GROUPS {
        cluster.engines[&(group, a)]
            .raft_handle()
            .change_membership(ChangeMembers::AddVoterIds(BTreeSet::from([LEARNER])), false)
            .await
            .unwrap_or_else(|error| {
                panic!("{context}: group {group}: promote the learner: {error}")
            });
    }
    // A voter campaigns alone: its durable vote term is the floor.
    let campaigner = others[0];
    for node in VOTERS.into_iter().filter(|node| *node != campaigner) {
        cluster.policy.partition_bidirectional(campaigner, node);
    }
    madsim::time::sleep(Duration::from_millis(400)).await;
    cluster.policy.heal_bidirectional(LEARNER, a);
    let deadline = madsim::time::Instant::now() + Duration::from_secs(5);
    while JOURNAL_GROUPS.iter().any(|group| {
        membership_of(cluster, *group, LEARNER).0 != vec![BTreeSet::from([1, 2, 3, LEARNER])]
    }) {
        assert!(
            madsim::time::Instant::now() < deadline,
            "{context}: the learner never caught up its promotion"
        );
        madsim::time::sleep(Duration::from_millis(1)).await;
    }
    let survivors = others.iter().copied().chain([LEARNER]).collect::<Vec<_>>();
    cluster.stop_node(LEARNER).await;
    cluster.wals[&LEARNER].power_loss_losing_unsynced().await;
    // The leader goes for good.
    cluster.stop_node(a).await;
    for node in others {
        cluster.stop_node(*node).await;
    }
    for node in others {
        cluster.wals[node].power_loss_losing_unsynced().await;
    }
    for node in &survivors {
        cluster.start_node(*node).await;
    }
    // The restarted cores publish their logs before the check reads them.
    let deadline = madsim::time::Instant::now() + Duration::from_secs(5);
    while JOURNAL_GROUPS.iter().any(|group| {
        survivors.iter().any(|node| {
            metrics(cluster, *group, *node)
                .membership_config
                .log_id()
                .is_none()
        })
    }) {
        assert!(
            madsim::time::Instant::now() < deadline,
            "{context}: the restarted replicas never published their logs"
        );
        madsim::time::sleep(Duration::from_millis(5)).await;
    }
    JOURNAL_GROUPS.iter().any(|group| {
        let (effective, _) = membership_of(cluster, *group, LEARNER);
        effective.iter().any(|voters| voters.contains(&LEARNER))
            && !cluster.engines[&(*group, LEARNER)]
                .kept_voter_configs()
                .iter()
                .any(|voters| voters.contains(&LEARNER))
    })
}

/// The learner learns a later committed entry than any voter: writes it
/// acknowledges reach every replica, the learner restarts cleanly, so its
/// log and commit marker are durable, and the voters lose power and every
/// page written since their last membership batch. Returns, per group, the
/// committed entry the learner holds.
async fn learner_ahead(
    cluster: &mut JournalCluster,
    plan: &Plan,
    served: &mut std::collections::BTreeMap<u32, Served>,
    context: &str,
) -> std::collections::BTreeMap<u32, LogIdOf<UrsulaRaftTypeConfig>> {
    let a = plan.leader;
    for group in JOURNAL_GROUPS {
        for index in 0..plan.lost {
            let payload = format!("lost-{group}-{index};").into_bytes();
            assert!(
                try_append(cluster, group, a, &payload).await,
                "{context}: group {group}: the leader acknowledges a write"
            );
            if let Some(served) = served.get_mut(&group) {
                served.lost.extend_from_slice(&payload);
            }
        }
    }
    // The learner learns that every write is committed.
    let deadline = madsim::time::Instant::now() + Duration::from_secs(5);
    while JOURNAL_GROUPS.iter().any(|group| {
        let leader = metrics(cluster, *group, a);
        metrics(cluster, *group, LEARNER)
            .local_committed
            .map(|id| id.index)
            != leader.last_log_index
    }) {
        assert!(
            madsim::time::Instant::now() < deadline,
            "{context}: the learner never learned the commit"
        );
        madsim::time::sleep(Duration::from_millis(5)).await;
    }
    let ahead = JOURNAL_GROUPS
        .iter()
        .map(|group| {
            let committed = metrics(cluster, *group, LEARNER)
                .local_committed
                .expect("the learner knows a committed entry");
            (*group, committed)
        })
        .collect();
    let wal = cluster.wals[&LEARNER].clone();
    wal.clean_shutdown(cluster.stop_node(LEARNER)).await;
    cluster.start_node(LEARNER).await;
    for node in VOTERS {
        cluster.stop_node(node).await;
    }
    for node in VOTERS {
        cluster.wals[&node].power_loss_losing_unsynced().await;
    }
    for node in VOTERS {
        cluster.start_node(node).await;
    }
    ahead
}

/// F3 of the third #613 review. Another voter, cut off alone, campaigns
/// meanwhile, so its vote term rises above every log term. The leader
/// acknowledges writes, then adds a second learner: that membership entry
/// syncs every voter's log up to it, but no later batch syncs their commit
/// markers, which stay below it. The first learner learns it is committed
/// and restarts cleanly; every voter loses power and every unsynced page. A cut to a voter's own marker would overwrite the
/// learner's committed entry; the plan instead keeps up to it, from a voter
/// whose log holds it. Returns, per group, the committed entry the learner
/// holds.
async fn commit_ahead(
    cluster: &mut JournalCluster,
    plan: &Plan,
    others: &[u64],
    served: &mut std::collections::BTreeMap<u32, Served>,
    context: &str,
) -> std::collections::BTreeMap<u32, LogIdOf<UrsulaRaftTypeConfig>> {
    let a = plan.leader;
    let campaigner = others[0];
    for node in NODES.into_iter().filter(|node| *node != campaigner) {
        cluster.policy.partition_bidirectional(campaigner, node);
    }
    for group in JOURNAL_GROUPS {
        for index in 0..plan.lost {
            let payload = format!("lost-{group}-{index};").into_bytes();
            assert!(
                try_append(cluster, group, a, &payload).await,
                "{context}: group {group}: the leader acknowledges a write"
            );
            if let Some(served) = served.get_mut(&group) {
                served.lost.extend_from_slice(&payload);
            }
        }
    }
    cluster.start_node(SECOND_LEARNER).await;
    for group in JOURNAL_GROUPS {
        cluster.engines[&(group, a)]
            .raft_handle()
            .add_learner(
                SECOND_LEARNER,
                openraft::BasicNode::new(format!("node-{SECOND_LEARNER}")),
                true,
            )
            .await
            .unwrap_or_else(|error| {
                panic!("{context}: group {group}: add the second learner: {error}")
            });
    }
    madsim::time::sleep(Duration::from_millis(400)).await;
    // The learner learns that the membership entry is committed.
    let deadline = madsim::time::Instant::now() + Duration::from_secs(5);
    while JOURNAL_GROUPS.iter().any(|group| {
        let leader = metrics(cluster, *group, a);
        metrics(cluster, *group, LEARNER)
            .local_committed
            .map(|id| id.index)
            != leader.last_log_index
    }) {
        assert!(
            madsim::time::Instant::now() < deadline,
            "{context}: the learner never learned the commit"
        );
        madsim::time::sleep(Duration::from_millis(5)).await;
    }
    let ahead = JOURNAL_GROUPS
        .iter()
        .map(|group| {
            let committed = metrics(cluster, *group, LEARNER)
                .local_committed
                .expect("the learner knows a committed entry");
            (*group, committed)
        })
        .collect();
    let wal = cluster.wals[&LEARNER].clone();
    wal.clean_shutdown(cluster.stop_node(LEARNER)).await;
    cluster.start_node(LEARNER).await;
    let lost = VOTERS;
    for node in lost {
        cluster.stop_node(node).await;
    }
    for node in lost {
        cluster.wals[&node].power_loss_losing_unsynced().await;
    }
    for node in lost {
        cluster.start_node(node).await;
    }
    ahead
}

/// `removed`, a voter, stops; the leader removes it, which commits without
/// it, and acknowledges writes. The leader, the other voter and the learner
/// lose power and every unsynced page, and `removed` comes back holding the
/// membership it had: it stays outside the leader's membership, and the plan
/// recovers without it.
async fn removed_missed(
    cluster: &mut JournalCluster,
    plan: &Plan,
    removed: u64,
    served: &mut std::collections::BTreeMap<u32, Served>,
    context: &str,
) {
    let a = plan.leader;
    cluster.stop_node(removed).await;
    for group in JOURNAL_GROUPS {
        madsim::time::timeout(
            Duration::from_secs(5),
            cluster.engines[&(group, a)]
                .raft_handle()
                .change_membership(
                    ChangeMembers::RemoveVoters(BTreeSet::from([removed])),
                    false,
                ),
        )
        .await
        .unwrap_or_else(|_elapsed| panic!("{context}: group {group}: the removal never committed"))
        .unwrap_or_else(|error| panic!("{context}: group {group}: remove node {removed}: {error}"));
        for index in 0..plan.lost {
            let payload = format!("lost-{group}-{index};").into_bytes();
            assert!(
                try_append(cluster, group, a, &payload).await,
                "{context}: group {group}: the leader acknowledges a write"
            );
            if let Some(served) = served.get_mut(&group) {
                served.lost.extend_from_slice(&payload);
            }
        }
    }
    let lost = NODES
        .into_iter()
        .filter(|node| {
            *node != removed && cluster.engines.contains_key(&(JOURNAL_GROUPS[0], *node))
        })
        .collect::<Vec<_>>();
    for node in &lost {
        cluster.stop_node(*node).await;
    }
    for node in &lost {
        cluster.wals[node].power_loss_losing_unsynced().await;
    }
    for node in lost.iter().chain([&removed]) {
        cluster.start_node(*node).await;
    }
}

/// The highest vote term among `group`'s running replicas but `hidden`.
fn loss_term(cluster: &JournalCluster, group: u32, hidden: &[u64]) -> u64 {
    NODES
        .iter()
        .filter(|node_id| {
            cluster.engines.contains_key(&(group, **node_id)) && !hidden.contains(node_id)
        })
        .map(|node_id| metrics(cluster, group, *node_id).current_term)
        .max()
        .unwrap_or(0)
}

/// The leaders, the voter and the learners other than `keep` lose power
/// and every unsynced page, then start again.
async fn lose_power_but(cluster: &mut JournalCluster, keep: &[u64]) {
    let lost = NODES
        .into_iter()
        .filter(|node| {
            !keep.contains(node) && cluster.engines.contains_key(&(JOURNAL_GROUPS[0], *node))
        })
        .collect::<Vec<_>>();
    for node in &lost {
        cluster.stop_node(*node).await;
    }
    for node in &lost {
        cluster.wals[node].power_loss_losing_unsynced().await;
    }
    for node in &lost {
        cluster.start_node(*node).await;
    }
}

/// One recovery of every group without `hidden`, which must succeed with
/// its first plan.
async fn recover_all(
    cluster: &mut JournalCluster,
    controls: Controls,
    hidden: &[u64],
    trace: &mut SimTrace,
    context: &str,
) -> usize {
    let mut plans = 0_usize;
    for group in JOURNAL_GROUPS {
        let term = loss_term(cluster, group, hidden);
        match recover(cluster, group, term, controls, hidden, trace).await {
            Ok(recovered) => plans = plans.saturating_add(recovered.plans_sent),
            Err(error) => fail(
                trace,
                RECOVERS_INVARIANT,
                "recovery_planning",
                format!("{context}: group {group}: the planning loop stopped: {error}"),
            ),
        }
    }
    plans
}

/// A of the fourth #613 review. Voter `behind` learns that the leader's
/// writes are committed and, when `purged`, purges its log up to its marker
/// (B1 of the fifth). It is cut off, and the tool never reaches it. The
/// other replicas lose power and the writes,
/// and a first plan, which never reaches `behind`, keeps less than its
/// marker. The new leader writes past the marker and adds a second
/// learner, whose membership entry syncs the writes, and the replicas lose
/// power again: a second plan keeps more than the marker. `behind` is
/// reached again: it refuses the second history's appends and, once the leader
/// compacts, its snapshot, since that history replaced the first plan's,
/// not its own. It keeps its committed entries. Then it is wiped and
/// rejoins as a wiped node, and serves what the leader serves.
async fn two_recoveries(
    cluster: &mut JournalCluster,
    plan: &Plan,
    behind: u64,
    purged: bool,
    tuning: JournalTuning,
    controls: Controls,
    trace: &mut SimTrace,
    context: &str,
) {
    let a = plan.leader;
    for group in JOURNAL_GROUPS {
        for index in 0..plan.lost {
            let payload = format!("lost-{group}-{index};").into_bytes();
            assert!(
                try_append(cluster, group, a, &payload).await,
                "{context}: group {group}: the leader acknowledges a write"
            );
        }
    }
    let deadline = madsim::time::Instant::now() + Duration::from_secs(5);
    while JOURNAL_GROUPS.iter().any(|group| {
        metrics(cluster, *group, behind)
            .local_committed
            .map(|id| id.index)
            != metrics(cluster, *group, a).last_log_index
    }) {
        assert!(
            madsim::time::Instant::now() < deadline,
            "{context}: node {behind} never learned the commit"
        );
        madsim::time::sleep(Duration::from_millis(5)).await;
    }
    let held = JOURNAL_GROUPS
        .iter()
        .map(|group| {
            let committed = metrics(cluster, *group, behind)
                .local_committed
                .expect("a committed entry");
            (*group, committed)
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    if purged {
        // OpenRaft defers a purge while it still reads the entries; try a
        // few times.
        for group in JOURNAL_GROUPS {
            let mut attempts = 0_u32;
            while let Err(stage) = cluster
                .try_snapshot_and_purge_node(group, behind, 0, Duration::from_secs(1))
                .await
            {
                attempts = attempts.saturating_add(1);
                assert!(
                    attempts < 10,
                    "{context}: group {group}: node {behind} never purged its log: {stage}"
                );
            }
        }
    }
    let mut served = std::collections::BTreeMap::new();
    for group in JOURNAL_GROUPS {
        served.insert(group, read_local(cluster, group, behind).await);
    }
    // It is cut off, and no plan reaches it.
    for node in NODES.into_iter().filter(|node| *node != behind) {
        cluster.policy.partition_bidirectional(behind, node);
    }
    let hidden = [behind];
    trace.push(SimEvent::FaultApplied {
        phase: "quorum_lost_power".to_owned(),
    });
    lose_power_but(cluster, &hidden).await;
    let mut plans = recover_all(cluster, controls, &hidden, trace, context).await;
    let first = JOURNAL_GROUPS
        .iter()
        .map(|group| {
            let leader = newest_leader(cluster, *group).expect("a leader after the first plan");
            let event = cluster.engines[&(*group, leader)]
                .history()
                .applied()
                .expect("the first plan's event");
            (*group, event)
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    for (group, event) in &first {
        assert!(
            event.kept < held[group].index,
            "{context}: group {group}: the first plan kept {} of node {behind}'s committed {}",
            event.kept,
            held[group].index
        );
    }

    // The first history writes past the marker; a membership entry syncs it.
    for group in JOURNAL_GROUPS {
        let mut index = 0_u64;
        loop {
            let leader = newest_leader(cluster, group).expect("a leader");
            if metrics(cluster, group, leader).last_log_index.unwrap_or(0)
                > held[&group].index.saturating_add(2)
            {
                break;
            }
            let payload = format!("second-{group}-{index};").into_bytes();
            assert!(
                try_append(cluster, group, leader, &payload).await,
                "{context}: group {group}: the first plan's leader acknowledges a write"
            );
            index = index.saturating_add(1);
        }
    }
    cluster.start_node(SECOND_LEARNER).await;
    for group in JOURNAL_GROUPS {
        let leader = newest_leader(cluster, group).expect("a leader");
        cluster.engines[&(group, leader)]
            .raft_handle()
            .add_learner(
                SECOND_LEARNER,
                openraft::BasicNode::new(format!("node-{SECOND_LEARNER}")),
                true,
            )
            .await
            .unwrap_or_else(|error| {
                panic!("{context}: group {group}: add the second learner: {error}")
            });
    }
    lose_power_but(cluster, &[SECOND_LEARNER, behind]).await;
    plans = plans.saturating_add(recover_all(cluster, controls, &hidden, trace, context).await);
    for group in JOURNAL_GROUPS {
        let leader = newest_leader(cluster, group).expect("a leader after the second plan");
        let event = cluster.engines[&(group, leader)]
            .history()
            .applied()
            .expect("the second plan's event");
        assert_eq!(
            event.previous,
            first[&group].id.get(),
            "{context}: group {group}: the second history replaced the first"
        );
        assert!(
            event.kept >= held[&group].index,
            "{context}: group {group}: the second plan kept {}, not past node {behind}'s marker {}",
            event.kept,
            held[&group].index
        );
    }

    // `behind` returns on the history both plans replaced.
    use ursula_stream::sim_controls::Defence;
    use ursula_stream::sim_controls::fired;
    let refused_before = fired(Defence::CommitScreenInstall);
    let appends_before = fired(Defence::CommitScreen);
    let heal = |cluster: &mut JournalCluster| {
        for node in NODES.into_iter().filter(|node| *node != behind) {
            cluster.policy.heal_bidirectional(behind, node);
        }
    };
    let part = |cluster: &mut JournalCluster| {
        for node in NODES.into_iter().filter(|node| *node != behind) {
            cluster.policy.partition_bidirectional(behind, node);
        }
    };
    // The leaders still hold their logs: they probe down to `behind`'s
    // marker, at or below its purge point when it purged.
    heal(cluster);
    let deadline = madsim::time::Instant::now() + Duration::from_secs(5);
    while fired(Defence::CommitScreen) == appends_before {
        assert!(
            madsim::time::Instant::now() < deadline,
            "{context}: node {behind} was sent no append over its committed entries"
        );
        madsim::time::sleep(Duration::from_millis(25)).await;
    }
    // Every other replica compacts while `behind` is cut off, so no
    // replication to it holds their entries, and whichever leads, its
    // campaigns meanwhile having moved the leadership or not, sends it a
    // snapshot.
    part(cluster);
    madsim::time::sleep(Duration::from_millis(200)).await;
    let mut sent = std::collections::BTreeMap::new();
    for group in JOURNAL_GROUPS {
        sent.insert(group, cluster.full_snapshots(group, behind));
        for node in NODES
            .into_iter()
            .filter(|node| *node != behind && cluster.engines.contains_key(&(group, *node)))
        {
            let mut attempts = 0_u32;
            while let Err(stage) = cluster
                .try_snapshot_and_purge_node(group, node, 0, Duration::from_secs(1))
                .await
            {
                attempts = attempts.saturating_add(1);
                assert!(
                    attempts < 10,
                    "{context}: group {group}: node {node} never compacted: {stage}"
                );
            }
        }
    }
    heal(cluster);
    for group in JOURNAL_GROUPS {
        // Replication to `behind` resumes after the backoff its refusals
        // and the partition started.
        let deadline = madsim::time::Instant::now() + Duration::from_secs(15);
        while cluster.full_snapshots(group, behind) == sent[&group] {
            assert!(
                madsim::time::Instant::now() < deadline,
                "{context}: group {group}: the leader never sent node {behind} a snapshot"
            );
            madsim::time::sleep(Duration::from_millis(25)).await;
        }
    }
    madsim::time::sleep(Duration::from_millis(300)).await;
    assert_node_kept(cluster, behind, &held, &served, trace).await;
    let refused = fired(Defence::CommitScreenInstall).saturating_sub(refused_before);

    // The documented way back: the node rejoins wiped.
    cluster
        .wipe_node(behind, &format!("{context}-wiped-{behind}"), tuning)
        .await;
    cluster.start_node(behind).await;
    for group in JOURNAL_GROUPS {
        let deadline = madsim::time::Instant::now() + Duration::from_secs(20);
        loop {
            let leader = newest_leader(cluster, group);
            if let Some(leader) = leader
                && cluster.engines.contains_key(&(group, behind))
                && membership_of(cluster, group, leader)
                    .0
                    .iter()
                    .any(|voters| voters.contains(&behind))
                && read_local_or_empty(cluster, group, behind).await
                    == read_local(cluster, group, leader).await
            {
                break;
            }
            if madsim::time::Instant::now() >= deadline {
                fail(
                    trace,
                    HISTORY_INVARIANT,
                    "wiped_node_rejoined",
                    format!(
                        "{context}: group {group}: node {behind}, wiped, never served what the \
                         leader serves as a voter"
                    ),
                );
            }
            madsim::time::sleep(Duration::from_millis(50)).await;
        }
    }
    trace.push(SimEvent::MembershipRollbackRejoinedWiped {
        node_id: behind,
        refused_installs: refused,
        plans: u64::try_from(plans).unwrap_or(u64::MAX),
        purged,
    });
}

/// What `node_id` serves of `group`, or nothing while it cannot read it,
/// as a wiped replica before it caught up.
async fn read_local_or_empty(cluster: &JournalCluster, group: u32, node_id: u64) -> Vec<u8> {
    let Some(engine) = cluster.engines.get(&(group, node_id)) else {
        return Vec::new();
    };
    engine
        .sim_read_local_stream(
            ReadStreamRequest {
                stream_id: group_stream(group),
                offset: 0,
                max_len: 1 << 20,
                now_ms: 0,
                leader_only: false,
                read_index: None,
                follower: FollowerRead::Prefix,
            },
            group_placement(group),
        )
        .await
        .map(|read| read.payload.to_vec())
        .unwrap_or_default()
}

/// Fails G1 when `node_id` no longer holds the committed entry it held
/// before it went down, in its log or as its last purged entry, or serves
/// other bytes than it served then: a later history overwrote its
/// committed entries, or extended them with its own.
async fn assert_node_kept(
    cluster: &JournalCluster,
    node_id: u64,
    held: &std::collections::BTreeMap<u32, LogIdOf<UrsulaRaftTypeConfig>>,
    served: &std::collections::BTreeMap<u32, Vec<u8>>,
    trace: &mut SimTrace,
) {
    for (group, committed) in held {
        let mut store = cluster.wals[&node_id]
            .store(RaftGroupId(*group))
            .expect("the node runs");
        let purged = store
            .get_log_state()
            .await
            .expect("read the node's log state")
            .last_purged_log_id;
        let entry = store
            .try_get_log_entries(committed.index..=committed.index)
            .await
            .expect("read the node's log")
            .first()
            .map(|entry| entry.log_id)
            .or(purged.filter(|purged| purged.index == committed.index));
        let now = read_local(cluster, *group, node_id).await;
        if entry != Some(*committed) || served.get(group) != Some(&now) {
            fail(
                trace,
                HISTORY_INVARIANT,
                "returned_two_histories_behind",
                format!(
                    "node {node_id} group {group} knew {committed:?} was committed, but now holds \
                     {entry:?} there and serves {} bytes, not {}: a later history overwrote its \
                     committed entries ({})",
                    now.len(),
                    served.get(group).map_or(0, Vec::len),
                    overwrite_refusals()
                ),
            );
        }
    }
}

/// Fails G1 when the learner no longer holds the committed entry it held
/// before the loss, in its log or as its last purged entry: a plan
/// overwrote it. A snapshot driver (#669) may purge the log through a
/// snapshot that covers the entry.
async fn assert_learner_kept(
    cluster: &JournalCluster,
    ahead: &std::collections::BTreeMap<u32, LogIdOf<UrsulaRaftTypeConfig>>,
    trace: &mut SimTrace,
) {
    for (group, committed) in ahead {
        let mut store = cluster.wals[&LEARNER]
            .store(RaftGroupId(*group))
            .expect("the learner runs");
        let purged = store
            .get_log_state()
            .await
            .expect("read the learner's log state")
            .last_purged_log_id;
        let held = store
            .try_get_log_entries(committed.index..=committed.index)
            .await
            .expect("read the learner's log")
            .first()
            .map(|entry| entry.log_id)
            .or(purged.filter(|purged| purged.index == committed.index));
        if held != Some(*committed) {
            fail(
                trace,
                HISTORY_INVARIANT,
                "recovery_planning",
                format!(
                    "node {LEARNER} group {group} knew {committed:?} was committed, but now holds \
                     {held:?} there: a plan overwrote its committed entries ({}, snapshots \
                     sent {}, metrics {:?})",
                    overwrite_refusals(),
                    cluster.full_snapshots(*group, LEARNER),
                    {
                        let replica = metrics(cluster, *group, LEARNER);
                        (
                            replica.snapshot,
                            replica.purged,
                            replica.local_committed,
                            replica.last_log_index,
                        )
                    }
                ),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Controls;
    use super::HISTORY_INVARIANT;
    use super::RECOVERS_INVARIANT;
    use crate::madsim_harness::SimEvent;
    use crate::madsim_harness::SimScenario;
    use crate::madsim_harness::SimTrace;
    use crate::madsim_harness::ThreeNodeRaftSim;
    use crate::madsim_harness::ThreeNodeRaftSimConfig;
    use crate::madsim_harness::introspect::invariant_failed;
    use crate::madsim_harness::tests::sim_test_guard;

    const SCENARIO: SimScenario = SimScenario::RecoveryPlanMembershipRollback;

    fn config(seed: u64) -> ThreeNodeRaftSimConfig {
        ThreeNodeRaftSimConfig::new(seed, format!("ursula-sim-{seed}-{}", SCENARIO.slug()))
    }

    /// The shape a run ended in.
    #[derive(Debug, PartialEq, Eq)]
    enum Shape {
        /// The plan elected `leader`, cutting when `cut`; `lagged` when a
        /// completed promotion's voter came back with its marker below it;
        /// `raised` when the cut kept up to a learner's committed entry;
        /// `outside` when a replica stayed outside the leader's membership;
        /// `installed` when a follower caught up from a snapshot.
        Recovered {
            leader: u64,
            cut: bool,
            lagged: bool,
            fenced: bool,
            raised: bool,
            outside: bool,
            installed: bool,
        },
        /// The tool refused, typed, for a learner ahead.
        Refused { node_id: u64 },
        /// After two plans, the voter neither reached refused the second
        /// history's snapshot, and rejoined wiped.
        RejoinedWiped {
            node_id: u64,
            refused: bool,
            purged: bool,
        },
    }

    fn shape(seed: u64) -> Shape {
        let first = ThreeNodeRaftSim::run_membership_rollback_with_controls_report(
            config(seed),
            Controls::default(),
        );
        let second = ThreeNodeRaftSim::run_membership_rollback_with_controls_report(
            config(seed),
            Controls::default(),
        );
        assert_eq!(first, second, "seed {seed}");
        assert_eq!(first.scenario, SCENARIO);
        first
            .outcome
            .trace
            .events
            .iter()
            .find_map(|event| match event {
                SimEvent::MembershipRollbackRecovered {
                    leader,
                    cut,
                    marker_lagged,
                    fenced,
                    raised,
                    outside,
                    installed,
                    ..
                } => Some(Shape::Recovered {
                    leader: *leader,
                    cut: *cut,
                    lagged: *marker_lagged,
                    fenced: *fenced > 0,
                    raised: *raised,
                    outside: *outside > 0,
                    installed: *installed > 0,
                }),
                SimEvent::MembershipRollbackRefused { node_id, .. } => {
                    Some(Shape::Refused { node_id: *node_id })
                }
                SimEvent::MembershipRollbackRejoinedWiped {
                    node_id,
                    refused_installs,
                    purged,
                    ..
                } => Some(Shape::RejoinedWiped {
                    node_id: *node_id,
                    refused: *refused_installs > 0,
                    purged: *purged,
                }),
                _ => None,
            })
            .unwrap_or_else(|| panic!("seed {seed}: no outcome"))
    }

    /// Each PR seed whose variant a review raised runs in the shape it
    /// exists for, twice alike, and every recovery ends with a follower
    /// catching up from a snapshot through the commit screen:
    ///
    /// - 505, a blackout promotion: no cut, and node 4, a voter only of the
    ///   promotion, leads with the votes of members whose own membership
    ///   does not name it (R3);
    /// - 506, a learner ahead: the tool refuses, typed (M-A);
    /// - 507, a completed promotion whose voter comes back with its marker
    ///   below it: the plan cuts and a voter of the kept membership leads
    ///   (M1);
    /// - 510, a commit ahead: the plan cuts, keeping up to the learner's
    ///   committed entry, above every voter's marker (F3);
    /// - 511, a missed removal: the removed voter stays outside the
    ///   leader's membership, and the plan recovers without it;
    /// - 512, two recoveries: the voter neither plan reached refuses the
    ///   second history's snapshot, keeps its committed entries, and
    ///   rejoins wiped (A of the fourth review).
    #[test]
    fn each_variant_recovers_in_its_shape_and_replays() {
        let _guard = sim_test_guard();
        assert!(matches!(shape(505), Shape::Recovered {
            leader: 4,
            cut: false,
            fenced: true,
            raised: false,
            installed: true,
            ..
        }));
        assert_eq!(shape(506), Shape::Refused { node_id: 4 });
        let completed = shape(507);
        assert!(
            matches!(completed, Shape::Recovered {
                leader: 2 | 3,
                cut: true,
                lagged: true,
                ..
            }),
            "{completed:?}"
        );
        assert!(matches!(shape(510), Shape::Recovered {
            cut: true,
            raised: true,
            installed: true,
            ..
        }));
        assert!(matches!(shape(511), Shape::Recovered {
            outside: true,
            fenced: true,
            installed: true,
            ..
        }));
        assert!(matches!(shape(512), Shape::RejoinedWiped {
            refused: true,
            purged: true,
            ..
        }));
    }

    /// Runs `seed` under `controls`, asserts it fails `invariant`, and
    /// returns the failure's message.
    fn assert_fails(seed: u64, controls: Controls, invariant: &str) -> String {
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::panic::catch_unwind(|| {
            ThreeNodeRaftSim::run_membership_rollback_with_controls_report(config(seed), controls)
        });
        std::panic::set_hook(previous_hook);
        assert!(result.is_err(), "{controls:?} left seed {seed} passing");
        let trace = SimTrace::last_recorded();
        assert!(
            invariant_failed(&trace, invariant),
            "{controls:?} did not fail `{invariant}` on seed {seed}: {:?}",
            trace.events.last()
        );
        trace
            .events
            .iter()
            .find_map(|event| match event {
                SimEvent::InvariantFailed { message, .. } => Some(message.clone()),
                _ => None,
            })
            .unwrap_or_default()
    }

    /// R3, the bug #613 reported: members that refuse a leader outside their
    /// own membership refuse node 4, which the blackout promotion's plan
    /// elects, as `leader_not_voter`, and the loop never recovers.
    #[test]
    fn negative_control_members_checking_their_own_membership_never_recover() {
        let _guard = sim_test_guard();
        let message = assert_fails(505, Controls::R3_MEMBER_CHECK, RECOVERS_INVARIANT);
        assert!(message.contains("leader: 4"), "{message}");
        assert!(message.contains("LeaderNotVoter"), "{message}");
    }

    /// R2: judged by its effective membership, node 4 leads a cutting plan
    /// that leaves it a learner. The plan wedges, and its first plan is
    /// never satisfied.
    #[test]
    fn negative_control_a_cut_judged_by_the_effective_membership_wedges() {
        let _guard = sim_test_guard();
        let message = assert_fails(R2_SEED, Controls::R2_EFFECTIVE, RECOVERS_INVARIANT);
        assert!(
            message.contains("Some(GroupPlan { leader: 4, point: ")
                && message.contains("cut: true"),
            "{message}"
        );
    }

    /// M-A: blind to the learner's committed index and ignoring its
    /// `recovery_point_behind`, the tool plans below it. The plan's leader
    /// would overwrite the learner's committed entries, first by appends,
    /// then, once it compacts, by a snapshot; the learner refuses both,
    /// which the run shows fired, and the loop never recovers.
    #[test]
    fn negative_control_a_plan_below_a_learners_commit_never_recovers() {
        let _guard = sim_test_guard();
        let message = assert_fails(506, Controls::COMMIT_BLIND, RECOVERS_INVARIANT);
        assert!(
            message.contains("overwrite refusals: ") && !message.contains("overwrite refusals: 0,"),
            "the learner never refused an append over its committed entries: {message}"
        );
        assert!(
            message.contains("install refusals: ") && !message.contains("install refusals: 0,"),
            "the learner never refused a snapshot over its committed entries: {message}"
        );
    }

    /// F1 of the third #613 review: with the screen off for snapshot
    /// installs only, the learner installs the compacted leader's snapshot
    /// over its committed entries and loses them.
    #[test]
    fn negative_control_an_unscreened_install_overwrites_a_learners_commit() {
        let _guard = sim_test_guard();
        let message = assert_fails(
            506,
            Controls::COMMIT_BLIND_INSTALL_UNSCREENED,
            HISTORY_INVARIANT,
        );
        assert!(message.contains("a plan overwrote"), "{message}");
    }

    /// A of the fourth #613 review: a screen that admits any history which
    /// kept entries up to the marker installs the second plan's snapshot on
    /// the voter neither plan reached, over its committed entries.
    #[test]
    fn negative_control_a_snapshot_two_histories_ahead_overwrites_a_commit() {
        let _guard = sim_test_guard();
        let message = assert_fails(512, Controls::CHAIN_UNSCREENED, HISTORY_INVARIANT);
        assert!(message.contains("a later history overwrote"), "{message}");
    }

    /// B1 of the fifth #613 review: a screen that reads the store's marker
    /// alone and skips every purged index lets the second history's entries
    /// through to the voter that purged its log up to its marker. OpenRaft's
    /// debug assertion that it holds every committed entry catches it in
    /// this build; a release build would take them.
    #[test]
    fn negative_control_a_screen_blind_below_the_purge_point_takes_another_history() {
        let _guard = sim_test_guard();
        let message = panic_message(512, Controls::PURGE_UNSCREENED);
        assert!(
            message.contains("self.committed()") || message.contains(HISTORY_INVARIANT),
            "{message}"
        );
    }

    /// #669, reruns of the #647 recovery DSTs: with the production snapshot
    /// driver on every node and a short floor, purges through covering and
    /// restored snapshots land at any time, and every variant's plan still
    /// holds.
    #[test]
    fn scrub_driver_keeps_membership_rollback_plans() {
        let _guard = sim_test_guard();
        for seed in 505..=513 {
            let report = ThreeNodeRaftSim::run_membership_rollback_with_controls_report(
                config(seed),
                Controls::WITH_DRIVERS,
            );
            assert_eq!(report.scenario, SCENARIO, "seed {seed}");
        }
    }

    /// Runs `seed` under `controls`, which must panic, and returns the
    /// panic's message.
    fn panic_message(seed: u64, controls: Controls) -> String {
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::panic::catch_unwind(|| {
            ThreeNodeRaftSim::run_membership_rollback_with_controls_report(config(seed), controls)
        });
        std::panic::set_hook(previous_hook);
        let payload = result.expect_err("the run panicked");
        payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| {
                payload
                    .downcast_ref::<&str>()
                    .map(|text| (*text).to_owned())
            })
            .unwrap_or_default()
    }

    /// M-A without the screen: the learner is sent the plan's entries over
    /// its committed ones and takes them. OpenRaft's debug assertion that a
    /// committed entry is always held catches it in this build; a release
    /// build would overwrite the entries silently.
    #[test]
    fn negative_control_an_unscreened_learner_takes_an_overwrite_of_its_commit() {
        let _guard = sim_test_guard();
        let message = panic_message(506, Controls::COMMIT_BLIND_UNSCREENED);
        assert!(
            message.contains("self.committed()") || message.contains(HISTORY_INVARIANT),
            "{message}"
        );
    }

    /// A cut-off promotion seed in the R2 control's shape: node 4 wins the
    /// committed-index tie.
    const R2_SEED: u64 = 985;
}
