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
//! [`Controls::DEFERRED_CUT`] runs the cut as before the review: at the
//! member's next start, with members voting for the uncut leader. `A` then
//! wins with its stale entry and commits it.

use std::time::Duration;

use ursula_config::WalFsync;
use ursula_runtime::AppendRequest;
use ursula_runtime::ColdWriteAdmission;
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
use super::journal_cluster::group_placement;
use super::journal_cluster::group_stream;
use super::journal_cluster::lead_every_group;
use super::journal_cluster::leader_by_state;
use super::journal_cluster::metrics;
use super::journal_cluster::recover_group;
use super::journal_cluster::wait_leader;
use super::journal_cluster::wait_plan_satisfied;
use super::recovery_wiring::RECOVERY_STALL_AFTER;

pub(super) const CUT_INVARIANT: &str = "accepted_loss_never_applies_a_stale_entry";

/// The write the cut-off leader holds uncommitted.
const STALE: &[u8] = b"stale-uncommitted;";

/// Server defences a negative control turns off. Production runs use the
/// default, with every defence on; only tests construct the others.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Controls {
    /// The cut waits for each member's next start, and members vote for the
    /// plan's leader with its log uncut, as before the #581 review.
    deferred_cut: bool,
}

#[cfg(test)]
impl Controls {
    pub(super) const DEFERRED_CUT: Self = Self { deferred_cut: true };
}

/// The defences [`Controls`] turned off for this simulation, until dropped.
struct Switched;

impl Switched {
    fn apply(controls: Controls) -> Self {
        set_defences(controls);
        Self
    }
}

impl Drop for Switched {
    fn drop(&mut self) {
        set_defences(Controls::default());
    }
}

fn set_defences(controls: Controls) {
    ursula_stream::sim_controls::set_off(
        ursula_stream::sim_controls::Defence::RecoveryCut,
        controls.deferred_cut,
    );
}

fn fail(trace: &mut SimTrace, after_event: &str, message: String) -> ! {
    trace.push(SimEvent::InvariantFailed {
        invariant: CUT_INVARIANT.to_owned(),
        after_event: after_event.to_owned(),
        message: message.clone(),
    });
    panic!("invariant `{CUT_INVARIANT}` failed after `{after_event}`: {message}");
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
        }
    }
}

/// Appends `payload` on `node_id`'s replica of `group` and gives up after a
/// moment: whether it was acknowledged.
async fn try_append(
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
async fn read_local(cluster: &JournalCluster, group: u32, node_id: u64) -> Vec<u8> {
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

pub(super) async fn run_recovery_plan_cut_inner(
    config: ThreeNodeRaftSimConfig,
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
    for group in JOURNAL_GROUPS {
        if leader_by_state(&cluster, group).is_some() {
            continue;
        }
        let request = recover_group(&mut cluster, group, &NODES, &context).await;
        leader = request.leader;
        cut |= request.cut;
        points.push((group, request.point));
    }
    trace.push(SimEvent::FaultApplied {
        phase: "recovery_planned".to_owned(),
    });

    // The members restart in any order.
    let mut restarts = 0_u64;
    for node in &plan.restarts {
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
    use crate::madsim_harness::SimScenario;
    use crate::madsim_harness::SimTrace;
    use crate::madsim_harness::ThreeNodeRaftSim;
    use crate::madsim_harness::ThreeNodeRaftSimConfig;
    use crate::madsim_harness::introspect::invariant_failed;
    use crate::madsim_harness::tests::sim_test_guard;

    fn config(seed: u64) -> ThreeNodeRaftSimConfig {
        let scenario = SimScenario::RecoveryPlanCutUnderRestarts;
        ThreeNodeRaftSimConfig::new(seed, format!("ursula-sim-{seed}-{}", scenario.slug()))
    }

    #[test]
    fn a_recovery_plan_with_a_cut_never_applies_a_stale_entry_and_replays() {
        let _guard = sim_test_guard();
        let scenario = SimScenario::RecoveryPlanCutUnderRestarts;
        for seed in 385..=389 {
            let first = ThreeNodeRaftSim::run_recovery_plan_with_controls_report(
                config(seed),
                Controls::default(),
            );
            let second = ThreeNodeRaftSim::run_recovery_plan_with_controls_report(
                config(seed),
                Controls::default(),
            );
            assert_eq!(first, second, "seed {seed}");
            assert_eq!(first.scenario, scenario);
        }
    }

    /// B1 of the #581 review, against the cut as it was: the uncut leader
    /// wins and commits its stale entry.
    #[test]
    fn a_deferred_cut_applies_the_stale_entry() {
        let _guard = sim_test_guard();
        SimTrace::clear_recorded();
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::panic::catch_unwind(|| {
            ThreeNodeRaftSim::run_recovery_plan_with_controls_report(
                config(386),
                Controls::DEFERRED_CUT,
            )
        });
        std::panic::set_hook(previous_hook);
        assert!(
            result.is_err(),
            "the deferred cut left the scenario passing"
        );
        assert!(
            invariant_failed(&SimTrace::last_recorded(), CUT_INVARIANT),
            "the deferred cut did not fail `{CUT_INVARIANT}`"
        );
    }
}
