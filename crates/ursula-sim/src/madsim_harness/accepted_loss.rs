//! Recovery plans after an accepted loss (#581 section 4.5, B1 of its
//! review), on journal-backed replicas under `raft.wal.fsync = never`.
//!
//! Invariant `accepted_loss_never_applies_a_stale_entry` (scenario
//! `recovery-plan-cut-under-restarts`): a leader `A` is cut off and keeps
//! an uncommitted write of its term; the others elect a new leader, which
//! acknowledges writes, and both lose power and every unsynced page, so they
//! keep a higher vote term but none of the new term's entries. The group
//! stalls. The recovery tool plans with the reachable members: when `A`'s
//! last term is below the vote term the others hold, the plan cuts every log
//! back to what each member knew was committed and elects the member with
//! the greatest committed index, often `A`, the open survivor. Every member
//! restarts in a seeded order after taking the plan. No replica ever applies
//! `A`'s uncommitted write, which the lost history may have overwritten,
//! and every replica applies the plan's history event.
//!
//! Invariant `recovery_plan_never_elects_an_unplanned_leader` (scenario
//! `recovery-plan-survivor-campaigns`, N1 of the second #581 review): the
//! same quorum loss, but the tool sends the plan slowly, pausing at least
//! one election timeout before each replica, while `A`, the lagging open
//! survivor, campaigns all along. Until the plan is satisfied, no replica
//! leads without holding the plan.
//!
//! Negative controls switch off the defences one bug needs gone:
//!
//! - [`Controls::STALE_SUFFIX`] runs the cut as before the first review, at
//!   the member's next start, and lets members vote for a leader whose log
//!   holds an entry above the point at or below the plan term. `A` then
//!   wins with its stale entry and commits it. Either defence alone keeps
//!   the scenario passing ([`Controls::DEFERRED_CUT`],
//!   [`Controls::NO_LOG_GUARD`]).
//! - [`Controls::LEADER_LAST`] sends the plan as the tool did before the
//!   second review: to the members first, with the vote term read on each
//!   instead of the leader's plan term, and to the leader last, with the
//!   members' log guard off too. A member that took the plan elects `A` before
//!   `A` took it, and `A` leads without the plan.

use std::time::Duration;

use ursula_config::WalFsync;
use ursula_ctl::recovery::GroupPlan;
use ursula_ctl::recovery::PlanMember;
use ursula_ctl::recovery::PlanTransport;
use ursula_ctl::recovery::SendError;
use ursula_ctl::recovery::plan_group;
use ursula_ctl::recovery::send_plan;
use ursula_proto::admin::PlanStep;
use ursula_proto::admin::RecoveryPlanReport;
use ursula_proto::admin::RecoveryPlanRequest;
use ursula_runtime::AppendRequest;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::FollowerRead;
use ursula_runtime::GroupEngine;
use ursula_runtime::ReadStreamRequest;

use super::SimEvent;
use super::SimTrace;
use super::SplitMix64;
use super::ThreeNodeRaftSimConfig;
use super::ThreeNodeRaftSimOutcome;
use super::journal_cluster::JOURNAL_GROUPS;
use super::journal_cluster::JournalCluster;
use super::journal_cluster::NODES;
use super::journal_cluster::SimPlanTransport;
use super::journal_cluster::group_placement;
use super::journal_cluster::group_stream;
use super::journal_cluster::lead_every_group;
use super::journal_cluster::leader_by_state;
use super::journal_cluster::member_view;
use super::journal_cluster::metrics;
use super::journal_cluster::recover_group;
use super::journal_cluster::wait_leader;
use super::journal_cluster::wait_plan_satisfied;
use super::recovery_wiring::RECOVERY_STALL_AFTER;

pub(super) const CUT_INVARIANT: &str = "accepted_loss_never_applies_a_stale_entry";
pub(super) const UNPLANNED_LEADER_INVARIANT: &str =
    "recovery_plan_never_elects_an_unplanned_leader";

/// The write the cut-off leader holds uncommitted.
const STALE: &[u8] = b"stale-uncommitted;";

/// Server defences a negative control turns off. Production runs use the
/// default, with every defence on; only tests construct the others.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Controls {
    /// The cut waits for each member's next start, as before the #581
    /// review.
    deferred_cut: bool,
    /// Members vote for the plan's leader whatever its log holds above the
    /// point, as before the third #581 review.
    log_guard_off: bool,
    /// A replica holding a plan takes replication from any leader, as
    /// before the third #581 review.
    replication_fence_off: bool,
    /// The tool sends the plan to the members first, without a plan term,
    /// and to the leader last, as before the second #581 review.
    leader_last: bool,
}

#[cfg(test)]
impl Controls {
    const NONE: Self = Self {
        deferred_cut: false,
        log_guard_off: false,
        replication_fence_off: false,
        leader_last: false,
    };
    pub(super) const DEFERRED_CUT: Self = Self {
        deferred_cut: true,
        ..Self::NONE
    };
    pub(super) const NO_LOG_GUARD: Self = Self {
        log_guard_off: true,
        ..Self::NONE
    };
    /// Both defences against an uncut stale suffix: either alone keeps the
    /// leader's stale entry from winning (B1 of the #581 review).
    pub(super) const STALE_SUFFIX: Self = Self {
        deferred_cut: true,
        log_guard_off: true,
        ..Self::NONE
    };
    pub(super) const NO_REPLICATION_FENCE: Self = Self {
        replication_fence_off: true,
        ..Self::NONE
    };
    /// Both defences against a pre-plan leader's entry after the cut:
    /// either alone keeps it from being applied (M1 of the third review).
    pub(super) const FOREIGN_ENTRY: Self = Self {
        log_guard_off: true,
        replication_fence_off: true,
        ..Self::NONE
    };
    /// Both defences against an unplanned leader: the leader-first order
    /// with its plan term, and the members' log guard, which refuses the
    /// unplanned survivor's uncut entry above the point (N1 of the second
    /// review).
    pub(super) const LEADER_LAST: Self = Self {
        leader_last: true,
        log_guard_off: true,
        ..Self::NONE
    };
}

/// Which scenario of the family runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Shape {
    /// The plan is sent at once, and every member restarts after it.
    CutUnderRestarts,
    /// The plan is sent slowly while the open survivor campaigns.
    SurvivorCampaigns,
}

/// The defences [`Controls`] turned off for this simulation, until dropped.
pub(super) struct Switched;

impl Switched {
    pub(super) fn apply(controls: Controls) -> Self {
        set_defences(controls);
        ursula_stream::sim_controls::reset_fired();
        Self
    }
}

impl Drop for Switched {
    fn drop(&mut self) {
        set_defences(Controls::default());
    }
}

fn set_defences(controls: Controls) {
    use ursula_stream::sim_controls::Defence;
    use ursula_stream::sim_controls::set_off;
    // `leader_last` is the tool's order, not a server defence.
    set_off(Defence::RecoveryCut, controls.deferred_cut);
    set_off(Defence::PlanLogGuard, controls.log_guard_off);
    set_off(
        Defence::PlanReplicationFence,
        controls.replication_fence_off,
    );
}

fn fail(trace: &mut SimTrace, invariant: &str, after_event: &str, message: String) -> ! {
    trace.push(SimEvent::InvariantFailed {
        invariant: invariant.to_owned(),
        after_event: after_event.to_owned(),
        message: message.clone(),
    });
    panic!("invariant `{invariant}` failed after `{after_event}`: {message}");
}

/// The plan of one run.
#[derive(Debug)]
struct Plan {
    /// The leader that is cut off.
    cut_off: u64,
    /// Writes every group commits before the cut-off.
    before: usize,
    /// Writes the new leader acknowledges before the power loss.
    lost: usize,
    /// The order in which the members restart after taking the plan.
    restarts: Vec<u64>,
    /// How long the tool pauses before each replica it sends the plan to,
    /// in the survivor scenario: at least one election timeout.
    pause: Duration,
}

impl Plan {
    fn from_seed(seed: u64) -> Self {
        let mut rng = SplitMix64::new(seed ^ 0x6375_7470_6c61_6e21);
        let cut_off = 1_u64.saturating_add(rng.next_bounded(3));
        let mut others = NODES
            .into_iter()
            .filter(|node| *node != cut_off)
            .collect::<Vec<_>>();
        if rng.next_bounded(2) == 0 {
            others.reverse();
        }
        // The cut-off leader restarts too, before, between or after them.
        let at = usize::try_from(rng.next_bounded(3)).expect("small");
        let mut restarts = others;
        restarts.insert(at.min(restarts.len()), cut_off);
        Self {
            cut_off,
            before: 2_usize.saturating_add(usize::try_from(rng.next_bounded(3)).expect("small")),
            lost: 1_usize.saturating_add(usize::try_from(rng.next_bounded(3)).expect("small")),
            restarts,
            pause: Duration::from_millis(
                100_u64.saturating_add(rng.next_bounded(4).saturating_mul(100)),
            ),
        }
    }
}

/// Fails the invariant when a replica of `group` leads without holding the
/// plan at `point`, before it applied the plan's history event.
fn assert_no_unplanned_leader(
    cluster: &JournalCluster,
    group: u32,
    point: u64,
    trace: &mut SimTrace,
    after: &str,
) {
    let Some(leader) = leader_by_state(cluster, group) else {
        return;
    };
    let history = cluster.engines[&(group, leader)].history();
    let satisfied = history
        .applied()
        .is_some_and(|event| event.event_index > point);
    if history.plan().is_none() && !satisfied {
        fail(
            trace,
            UNPLANNED_LEADER_INVARIANT,
            after,
            format!(
                "node {leader} leads group {group} without the recovery plan at point {point}: \
                 it serves under the history the plan replaces"
            ),
        );
    }
}

/// Checks [`assert_no_unplanned_leader`] for `pause`.
async fn watch_leaders(
    cluster: &JournalCluster,
    group: u32,
    point: u64,
    pause: Duration,
    trace: &mut SimTrace,
    after: &str,
) {
    let until = madsim::time::Instant::now() + pause;
    loop {
        assert_no_unplanned_leader(cluster, group, point, trace, after);
        if madsim::time::Instant::now() >= until {
            return;
        }
        madsim::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The shipped sender of `ursulactl recover-group`, pausing `pause` before
/// each replica and checking meanwhile that no replica leads without the
/// plan.
struct SlowTransport<'a> {
    inner: SimPlanTransport<'a>,
    point: u64,
    pause: Duration,
    trace: &'a mut SimTrace,
    /// The plan's leader.
    leader: u64,
    /// Whether the plan's leader held the plan, or had applied its history
    /// event, whenever the plan went to another member: the leader-first
    /// order held.
    leader_first: bool,
}

impl SlowTransport<'_> {
    async fn accept(
        &mut self,
        node_id: u64,
        request: &RecoveryPlanRequest,
    ) -> Result<RecoveryPlanReport, SendError> {
        watch_leaders(
            self.inner.cluster,
            self.inner.group,
            self.point,
            self.pause,
            self.trace,
            "recovery_plan_sending",
        )
        .await;
        if node_id != self.leader {
            let history = self.inner.cluster.engines[&(self.inner.group, self.leader)].history();
            self.leader_first &= history.plan().is_some()
                || history
                    .applied()
                    .is_some_and(|event| event.event_index > self.point);
        }
        self.inner.accept(node_id, request).await
    }
}

/// Whether a slow send held the leader-first order (N1): the plan went to
/// its leader before any other member.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct SlowSend {
    leader_first: bool,
}

impl PlanTransport for SlowTransport<'_> {
    fn send(
        &mut self,
        node_id: u64,
        request: &RecoveryPlanRequest,
    ) -> impl std::future::Future<Output = Result<RecoveryPlanReport, SendError>> {
        self.accept(node_id, request)
    }
}

/// The recovery tool, slowly: reads the members, computes the plan with the
/// shipped planner and sends it with the shipped sender one replica at a
/// time, pausing `pause` before each and checking that no replica leads
/// without the plan. With `leader_last`, it sends as the tool did before the
/// second #581 review instead: the members first, without a plan term. A
/// member that a leader reached meanwhile refuses. Returns the plan.
async fn recover_group_slowly(
    cluster: &mut JournalCluster,
    group: u32,
    pause: Duration,
    leader_last: bool,
    trace: &mut SimTrace,
    context: &str,
) -> (GroupPlan, SlowSend) {
    let mut members = Vec::new();
    for node_id in NODES {
        members.push(member_view(cluster, group, node_id).await);
    }
    let plan = plan_group(&members)
        .unwrap_or_else(|error| panic!("{context}: group {group}: no plan: {error}"));
    cluster.plan_leaders.insert(plan.leader);
    if !leader_last {
        let mut transport = SlowTransport {
            inner: SimPlanTransport { cluster, group },
            point: plan.point,
            pause,
            trace,
            leader: plan.leader,
            leader_first: true,
        };
        let sent = send_plan(&mut transport, plan, &members)
            .await
            .unwrap_or_else(|error| panic!("{context}: group {group}: {plan:?}: {error}"));
        for error in &sent.refused {
            tracing::info!(%error, "{context}: group {group}: a member refused the plan");
        }
        return (plan, SlowSend {
            leader_first: transport.leader_first,
        });
    }
    let request = |member: &PlanMember, step: PlanStep| RecoveryPlanRequest {
        leader: plan.leader,
        point: plan.point,
        cut: plan.cut,
        step,
        expected: member.log,
    };
    let after = "recovery_plan_sending";
    for member in members
        .iter()
        .filter(|member| member.node_id != plan.leader)
    {
        watch_leaders(cluster, group, plan.point, pause, trace, after).await;
        // No plan term exists before the leader took the plan: the term a
        // hand-built body names is the one read on the member, which the
        // server's lower bound lets through.
        let follow = request(member, PlanStep::Follow {
            leader_term: member.log.current_term,
        });
        if let Err(error) = cluster
            .accept_recovery_plan(group, member.node_id, &follow)
            .await
        {
            tracing::info!(%error, "{context}: group {group}: a member refused the plan");
        }
    }
    watch_leaders(cluster, group, plan.point, pause, trace, after).await;
    let lead = members
        .iter()
        .find(|member| member.node_id == plan.leader)
        .expect("the plan's leader is reachable");
    let lead = request(lead, PlanStep::Lead {
        highest_term: plan.highest_term,
    });
    if let Err(error) = cluster
        .accept_recovery_plan(group, plan.leader, &lead)
        .await
    {
        tracing::info!(%error, "{context}: group {group}: the plan's leader refused");
    }
    (plan, SlowSend {
        leader_first: false,
    })
}

/// Waits until every replica of `group` applied the plan's history event,
/// checking meanwhile that no replica leads without the plan.
async fn wait_satisfied_watching(
    cluster: &JournalCluster,
    group: u32,
    point: u64,
    trace: &mut SimTrace,
    context: &str,
) {
    let deadline = madsim::time::Instant::now() + Duration::from_secs(15);
    loop {
        assert_no_unplanned_leader(cluster, group, point, trace, "recovery_plan_sent");
        let satisfied = NODES.iter().all(|node_id| {
            cluster
                .engines
                .get(&(group, *node_id))
                .is_some_and(|engine| {
                    engine
                        .history()
                        .applied()
                        .is_some_and(|event| event.event_index > point)
                        && engine.history().plan().is_none()
                })
        });
        if satisfied {
            return;
        }
        assert!(
            madsim::time::Instant::now() < deadline,
            "{context}: group {group}: the recovery plan's history event never applied everywhere"
        );
        madsim::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Appends `payload` on `node_id`'s replica of `group` and gives up after a
/// moment: whether it was acknowledged.
pub(super) async fn try_append(
    cluster: &mut JournalCluster,
    group: u32,
    node_id: u64,
    payload: &[u8],
) -> bool {
    let Some(engine) = cluster.engines.get_mut(&(group, node_id)) else {
        return false;
    };
    let append = engine.append(
        AppendRequest::from_bytes(group_stream(group), payload.to_vec()),
        group_placement(group),
        ColdWriteAdmission::default(),
    );
    matches!(
        madsim::time::timeout(Duration::from_secs(1), append).await,
        Ok(Ok(_))
    )
}

/// What `node_id`'s replica of `group` serves locally.
pub(super) async fn read_local(cluster: &JournalCluster, group: u32, node_id: u64) -> Vec<u8> {
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

/// Fails the invariant when any replica of any group serves the cut-off
/// leader's uncommitted write.
async fn assert_no_stale_entry(cluster: &JournalCluster, trace: &mut SimTrace, after: &str) {
    for group in JOURNAL_GROUPS {
        for node_id in NODES {
            let served = read_local(cluster, group, node_id).await;
            if served.windows(STALE.len()).any(|window| window == STALE) {
                fail(
                    trace,
                    CUT_INVARIANT,
                    after,
                    format!(
                        "node {node_id} group {group} applied the cut-off leader's uncommitted \
                         write, which the lost history may have overwritten"
                    ),
                );
            }
        }
    }
}

pub(super) async fn run_recovery_plan_inner(
    config: ThreeNodeRaftSimConfig,
    shape: Shape,
    controls: Controls,
) -> ThreeNodeRaftSimOutcome {
    let _switched = Switched::apply(controls);
    let plan = Plan::from_seed(config.seed);
    let mut trace = SimTrace::default();
    trace.push(SimEvent::ClusterBuilt { seed: config.seed });
    let context = format!("seed {}", config.seed);
    let mut cluster = JournalCluster::start_with_fsync(
        &format!("recovery-plan-cut-{}", config.seed),
        WalFsync::Never,
    )
    .await;
    for group in JOURNAL_GROUPS {
        cluster.append(group, plan.before).await;
    }
    cluster.verify_reads().await;
    let a = plan.cut_off;
    lead_every_group(&cluster, a, &context).await;
    trace.push(SimEvent::LeaderElected { leader_id: a });

    // A is cut off and appends a write of its term that never commits.
    for node in NODES.into_iter().filter(|node| *node != a) {
        cluster.policy.partition_bidirectional(a, node);
    }
    trace.push(SimEvent::FaultApplied {
        phase: "leader_cut_off".to_owned(),
    });
    let mut stale_writes = 0_u64;
    for group in JOURNAL_GROUPS {
        if !try_append(&mut cluster, group, a, STALE).await {
            stale_writes = stale_writes.saturating_add(1);
        }
    }

    // The others elect a new leader, which acknowledges writes.
    let others = NODES
        .into_iter()
        .filter(|node| *node != a)
        .collect::<Vec<_>>();
    for group in JOURNAL_GROUPS {
        let deadline = madsim::time::Instant::now() + Duration::from_secs(10);
        let leader =
            loop {
                if let Some(leader) = others.iter().copied().find(|node| {
                    metrics(&cluster, group, *node).state == openraft::ServerState::Leader
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
                try_append(&mut cluster, group, leader, &payload).await,
                "{context}: group {group}: the new leader acknowledges a write"
            );
        }
    }

    // Both lose power and every page written since the last fsync: they keep
    // their votes, not the new term's entries.
    for node in &others {
        cluster.stop_node(*node).await;
    }
    for node in &others {
        cluster.wals[node].power_loss_losing_unsynced().await;
    }
    for node in &others {
        cluster.start_node(*node).await;
    }
    for node in NODES.into_iter().filter(|node| *node != a) {
        cluster.policy.heal_bidirectional(a, node);
    }
    trace.push(SimEvent::FaultApplied {
        phase: "quorum_lost_power".to_owned(),
    });

    // The group stalls: no leader, until the operator plans.
    madsim::time::sleep(RECOVERY_STALL_AFTER.saturating_mul(2)).await;
    assert_no_stale_entry(&cluster, &mut trace, "quorum_lost").await;
    for group in JOURNAL_GROUPS {
        if leader_by_state(&cluster, group).is_some() {
            // A kept or regained a quorum: the loss healed without a plan.
            assert_no_stale_entry(&cluster, &mut trace, "healed_without_a_plan").await;
        }
    }

    let mut points = Vec::new();
    let mut leader = a;
    let mut cut = false;
    let mut uncut_leader = false;
    // The survivor scenario's evidence of which defence held (N1).
    let mut slow_sends = Vec::new();
    for group in JOURNAL_GROUPS {
        if leader_by_state(&cluster, group).is_some() {
            continue;
        }
        let taken = match shape {
            Shape::CutUnderRestarts => recover_group(&mut cluster, group, &NODES, &context).await,
            Shape::SurvivorCampaigns => {
                let (taken, sent) = recover_group_slowly(
                    &mut cluster,
                    group,
                    plan.pause,
                    controls.leader_last,
                    &mut trace,
                    &context,
                )
                .await;
                slow_sends.push(sent);
                taken
            }
        };
        let (plan_leader, point, plan_cut) = (taken.leader, taken.point, taken.cut);
        // Whether the plan's leader still holds an entry above the point at
        // or below the plan term: the stale suffix, uncut.
        if let Some(record) = cluster.engines[&(group, plan_leader)].history().plan() {
            let held = member_view(&cluster, group, plan_leader).await.log;
            uncut_leader |= held.last_log_index.is_some_and(|index| index > point)
                && held
                    .last_log_term
                    .is_some_and(|term| term <= record.leader_term);
        }
        if shape == Shape::SurvivorCampaigns {
            wait_satisfied_watching(&cluster, group, point, &mut trace, &context).await;
        }
        leader = plan_leader;
        cut |= plan_cut;
        points.push((group, point));
    }
    trace.push(SimEvent::FaultApplied {
        phase: "recovery_planned".to_owned(),
    });

    // The members restart in any order.
    let mut restarts = 0_u64;
    let restart_order = match shape {
        Shape::CutUnderRestarts => plan.restarts.clone(),
        Shape::SurvivorCampaigns => Vec::new(),
    };
    for node in &restart_order {
        let wal = cluster.wals[node].clone();
        wal.clean_shutdown(cluster.stop_node(*node)).await;
        cluster.start_node(*node).await;
        restarts = restarts.saturating_add(1);
        madsim::time::sleep(Duration::from_millis(200)).await;
        assert_no_stale_entry(&cluster, &mut trace, "member_restarted").await;
    }
    for (group, point) in &points {
        wait_leader(&cluster, *group, &context).await;
        wait_plan_satisfied(&cluster, *group, &NODES, *point, &context).await;
    }
    madsim::time::sleep(Duration::from_millis(500)).await;
    assert_no_stale_entry(&cluster, &mut trace, "plan_satisfied").await;
    trace.push(SimEvent::RecoveryPlanVerified {
        leader,
        cut,
        stale_writes,
        restarts,
        uncut_leader,
        leader_first: !slow_sends.is_empty() && slow_sends.iter().all(|sent| sent.leader_first),
        guard_refusals: ursula_stream::sim_controls::fired(
            ursula_stream::sim_controls::Defence::PlanLogGuard,
        ),
    });
    ThreeNodeRaftSimOutcome {
        seed: config.seed,
        leader_id: leader,
        target_node_id: Some(a),
        appended_log_index: 0,
        trace,
    }
}

#[cfg(test)]
mod tests {
    use super::CUT_INVARIANT;
    use super::Controls;
    use super::UNPLANNED_LEADER_INVARIANT;
    use crate::madsim_harness::SimEvent;
    use crate::madsim_harness::SimScenario;
    use crate::madsim_harness::SimTrace;
    use crate::madsim_harness::ThreeNodeRaftSim;
    use crate::madsim_harness::ThreeNodeRaftSimConfig;
    use crate::madsim_harness::introspect::invariant_failed;
    use crate::madsim_harness::tests::sim_test_guard;

    fn config(seed: u64, scenario: SimScenario) -> ThreeNodeRaftSimConfig {
        ThreeNodeRaftSimConfig::new(seed, format!("ursula-sim-{seed}-{}", scenario.slug()))
    }

    fn assert_fails(scenario: SimScenario, seed: u64, controls: Controls, invariant: &str) {
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::panic::catch_unwind(|| {
            ThreeNodeRaftSim::run_recovery_plan_with_controls_report(
                config(seed, scenario),
                scenario,
                controls,
            )
        });
        std::panic::set_hook(previous_hook);
        assert!(result.is_err(), "{controls:?} left seed {seed} passing");
        assert!(
            invariant_failed(&SimTrace::last_recorded(), invariant),
            "{controls:?} did not fail `{invariant}` on seed {seed}"
        );
    }

    fn assert_passes_and_replays(scenario: SimScenario, seeds: std::ops::RangeInclusive<u64>) {
        for seed in seeds {
            let first = ThreeNodeRaftSim::run_recovery_plan_with_controls_report(
                config(seed, scenario),
                scenario,
                Controls::default(),
            );
            let second = ThreeNodeRaftSim::run_recovery_plan_with_controls_report(
                config(seed, scenario),
                scenario,
                Controls::default(),
            );
            assert_eq!(first, second, "seed {seed}");
            assert_eq!(first.scenario, scenario);
        }
    }

    #[test]
    fn a_recovery_plan_with_a_cut_never_applies_a_stale_entry_and_replays() {
        let _guard = sim_test_guard();
        assert_passes_and_replays(SimScenario::RecoveryPlanCutUnderRestarts, 385..=389);
    }

    /// N1 of the second #581 review: sent slowly, leader first, a plan never
    /// lets the campaigning survivor lead without it.
    #[test]
    fn a_slow_plan_never_elects_an_unplanned_leader_and_replays() {
        let _guard = sim_test_guard();
        assert_passes_and_replays(SimScenario::RecoveryPlanSurvivorCampaigns, 390..=394);
    }

    /// B1 of the #581 review, without both of its defences: the cut runs
    /// at the next start, and members vote for the uncut leader. It wins
    /// and commits its stale entry.
    #[test]
    fn negative_control_an_uncut_leader_without_the_log_guard_applies_the_stale_entry() {
        let _guard = sim_test_guard();
        assert_fails(
            SimScenario::RecoveryPlanCutUnderRestarts,
            386,
            Controls::STALE_SUFFIX,
            CUT_INVARIANT,
        );
    }

    /// Either defence of B1 alone keeps the scenario passing, and each run
    /// shows that the remaining one fired. Without the log guard, the cut
    /// in the accept job leaves the plan's leader no stale suffix. With the
    /// cut deferred, the leader keeps its stale suffix after the plan, and
    /// the members' log guard keeps it from winning until its own start
    /// runs the cut.
    #[test]
    fn either_defence_against_a_stale_suffix_alone_suffices() {
        let _guard = sim_test_guard();
        let scenario = SimScenario::RecoveryPlanCutUnderRestarts;
        let verified = |controls: Controls, seed: u64| {
            let report = ThreeNodeRaftSim::run_recovery_plan_with_controls_report(
                config(seed, scenario),
                scenario,
                controls,
            );
            report
                .outcome
                .trace
                .events
                .iter()
                .find_map(|event| match event {
                    crate::madsim_harness::SimEvent::RecoveryPlanVerified {
                        cut,
                        uncut_leader,
                        ..
                    } => Some((*cut, *uncut_leader)),
                    _ => None,
                })
                .unwrap_or_else(|| panic!("seed {seed} {controls:?}: no plan verified"))
        };
        let (mut cut_ran, mut guard_fired) = (false, false);
        for seed in 385..=389 {
            let (cut, uncut_leader) = verified(Controls::NO_LOG_GUARD, seed);
            assert!(!uncut_leader, "seed {seed}: the cut left a stale suffix");
            cut_ran |= cut;
            let (_, uncut_leader) = verified(Controls::DEFERRED_CUT, seed);
            guard_fired |= uncut_leader;
        }
        assert!(cut_ran, "no seed cut: the cut alone was never shown");
        assert!(
            guard_fired,
            "no seed left the plan's leader uncut: the log guard alone was never shown"
        );
    }

    /// Either defence of N1 alone keeps the survivor scenario passing, and
    /// the runs show that the remaining one fired. Without the log guard,
    /// every run sends the plan to its leader before every other member, so
    /// no member holds the plan while its leader does not. With the plan
    /// sent leader last, some run counts a vote that a member refused the
    /// unplanned leader for its entry above the point at or below the plan
    /// term, a refusal only the log guard makes.
    ///
    /// The guard alone keeps out the unplanned leader, but under the
    /// leader-last order it does not always let the plan finish (#643). On
    /// seeds 393 and 403 a member that took the plan elects the plan's
    /// leader below the plan term before the leader took it. The leader
    /// keeps leading there while the survivor campaigns at a higher term,
    /// and the plan's history event never applies. Those seeds check safety
    /// only: no leader without the plan is ever elected (G1), and the run
    /// ends, if at all, in the wait for the plan. The tool and the runbook
    /// always send the plan to its leader first, so the stall needs a
    /// hand-built `follow` sent before the `lead`. Before #638, seed 393
    /// passed only after other simulations on the same thread.
    #[test]
    fn either_defence_against_an_unplanned_leader_alone_suffices() {
        let _guard = sim_test_guard();
        let scenario = SimScenario::RecoveryPlanSurvivorCampaigns;
        let leader_last_only = Controls {
            log_guard_off: false,
            ..Controls::LEADER_LAST
        };
        let verified = |controls: Controls, seed: u64| {
            let report = ThreeNodeRaftSim::run_recovery_plan_with_controls_report(
                config(seed, scenario),
                scenario,
                controls,
            );
            report
                .outcome
                .trace
                .events
                .iter()
                .find_map(|event| match event {
                    SimEvent::RecoveryPlanVerified {
                        leader_first,
                        guard_refusals,
                        ..
                    } => Some((*leader_first, *guard_refusals)),
                    _ => None,
                })
                .unwrap_or_else(|| panic!("seed {seed} {controls:?}: no plan verified"))
        };
        let mut guard_fired = false;
        for seed in 394..=398 {
            let (leader_first, _) = verified(Controls::NO_LOG_GUARD, seed);
            assert!(
                leader_first,
                "seed {seed}: a member took the plan before its leader"
            );
            let (_, guard_refusals) = verified(leader_last_only, seed);
            guard_fired |= guard_refusals > 0;
        }
        assert!(
            guard_fired,
            "no seed counted a vote the log guard refused: the guard alone was never shown"
        );
        for seed in [393, 403] {
            let (leader_first, _) = verified(Controls::NO_LOG_GUARD, seed);
            assert!(
                leader_first,
                "seed {seed}: a member took the plan before its leader"
            );
            let previous_hook = std::panic::take_hook();
            std::panic::set_hook(Box::new(|_| {}));
            let result = std::panic::catch_unwind(|| {
                ThreeNodeRaftSim::run_recovery_plan_with_controls_report(
                    config(seed, scenario),
                    scenario,
                    leader_last_only,
                )
            });
            std::panic::set_hook(previous_hook);
            let failed = SimTrace::last_recorded()
                .events
                .into_iter()
                .find(|event| matches!(event, SimEvent::InvariantFailed { .. }));
            assert!(failed.is_none(), "seed {seed}: {failed:?}");
            if let Err(payload) = result {
                let panic = crate::madsim_harness::introspect::panic_payload_to_string(payload);
                assert!(
                    panic.contains("the recovery plan's history event never applied everywhere"),
                    "seed {seed}: {panic}"
                );
            }
        }
    }

    #[test]
    fn negative_control_a_plan_sent_leader_last_elects_an_unplanned_leader() {
        let _guard = sim_test_guard();
        assert_fails(
            SimScenario::RecoveryPlanSurvivorCampaigns,
            394,
            Controls::LEADER_LAST,
            UNPLANNED_LEADER_INVARIANT,
        );
    }
}
