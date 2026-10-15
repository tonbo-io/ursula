//! M1 of the third #581 review: a leader elected before a recovery plan
//! never gets an entry applied after the plan, with five voters.
//!
//! P (node 4) is cut off and does not campaign meanwhile, as a drained node
//! does, so its vote term stays below the later leaders'. X (node 5) is
//! elected, then cut off too, and appends a write of its term that never
//! commits. B, C and D (nodes 1 to 3) elect a leader at a later term, commit
//! writes, then lose power and every unsynced page. P rejoins them, X stays
//! cut off. The tool plans over P, B, C and D: the plan cuts, and its leader
//! is P. Right after P took the plan, before its first campaign, X reaches P
//! with its leadership at a term below the plan term. Then the other members
//! take the plan.
//!
//! Two defences keep X's write from being applied: P takes no replication
//! from a leader at or below the plan term, and the members refuse P while
//! its log holds an entry above the point at or below the plan term. Either
//! suffices. With both off, X extends P's log, the members elect P, and its
//! history event lands above X's write, which they apply under the old
//! history.

use std::time::Duration;

use ursula_config::WalFsync;
use ursula_ctl::recovery::PlanTransport;
use ursula_ctl::recovery::SendError;
use ursula_ctl::recovery::plan_group;
use ursula_ctl::recovery::send_plan;
use ursula_proto::admin::RecoveryPlanReport;
use ursula_proto::admin::RecoveryPlanRequest;
use ursula_raft::JournalTuning;

use crate::madsim_harness::accepted_loss::Controls;
use crate::madsim_harness::accepted_loss::Switched;
use crate::madsim_harness::accepted_loss::read_local;
use crate::madsim_harness::accepted_loss::try_append;
use crate::madsim_harness::journal_cluster::JOURNAL_GROUPS;
use crate::madsim_harness::journal_cluster::JOURNAL_POWER_LOSS_SEEDS;
use crate::madsim_harness::journal_cluster::JournalCluster;
use crate::madsim_harness::journal_cluster::SimPlanTransport;
use crate::madsim_harness::journal_cluster::lead_every_group;
use crate::madsim_harness::journal_cluster::member_view;
use crate::madsim_harness::journal_cluster::metrics;
use crate::madsim_harness::recovery_wiring::RECOVERY_STALL_AFTER;
use crate::madsim_harness::run_with_madsim;
use crate::madsim_harness::tests::sim_test_guard;

/// The plan's leader, cut off without campaigning.
const P: u64 = 4;
/// The leader elected before the plan that reaches P after it.
const X: u64 = 5;
/// The members that lose power.
const REST: [u64; 3] = [1, 2, 3];
/// The replicas the tool reaches.
const REACHED: [u64; 4] = [1, 2, 3, 4];
/// X's write, which never commits.
const FOREIGN: &[u8] = b"foreign-uncommitted;";

/// What one run saw.
#[derive(Debug, Default)]
struct Outcome {
    /// Groups whose plan elected P, so X reached the plan's leader.
    shaped: usize,
    /// Of those, groups where X extended P's log above the point at or
    /// below the plan term: the replication fence did not stop it.
    extended: usize,
    /// Of those, groups whose plan was satisfied: the log guard did not
    /// stop P.
    shaped_satisfied: usize,
    /// Groups whose plan was satisfied on every reached replica.
    satisfied: usize,
    /// A reached replica applied X's write.
    foreign_applied: bool,
}

/// [`SimPlanTransport`] that lets X reach the plan's leader right after it
/// took the plan, before any other member is sent the plan.
struct XReachesTheLeader<'a> {
    inner: SimPlanTransport<'a>,
    leader: u64,
    /// X extended P's log above the point at or below the plan term.
    extended: bool,
}

impl XReachesTheLeader<'_> {
    async fn accept(
        &mut self,
        node_id: u64,
        request: &RecoveryPlanRequest,
    ) -> Result<RecoveryPlanReport, SendError> {
        let answer = self.inner.accept(node_id, request).await;
        if node_id == self.leader && self.leader == P {
            // P follows X while they are healed, and X's replication to P
            // resumes after its backoff from the long partition.
            self.inner.cluster.policy.heal_bidirectional(X, P);
            madsim::time::sleep(Duration::from_secs(3)).await;
            self.inner.cluster.policy.partition_bidirectional(X, P);
            if let Ok(report) = &answer {
                let held = member_view(self.inner.cluster, self.inner.group, P)
                    .await
                    .log;
                self.extended = held
                    .last_log_index
                    .is_some_and(|index| index > request.point)
                    && held
                        .last_log_term
                        .is_some_and(|term| term <= report.leader_term);
            }
        }
        answer
    }
}

impl PlanTransport for XReachesTheLeader<'_> {
    fn send(
        &mut self,
        node_id: u64,
        request: &RecoveryPlanRequest,
    ) -> impl std::future::Future<Output = Result<RecoveryPlanReport, SendError>> {
        self.accept(node_id, request)
    }
}

async fn foreign_applied(cluster: &JournalCluster, group: u32) -> bool {
    for node_id in REACHED {
        let served = read_local(cluster, group, node_id).await;
        if served
            .windows(FOREIGN.len())
            .any(|window| window == FOREIGN)
        {
            return true;
        }
    }
    false
}

fn run(seed: u64, controls: Controls) -> Outcome {
    run_with_madsim(seed, async move {
        let _switched = Switched::apply(controls);
        let context = format!("seed {seed} {controls:?}");
        let mut cluster = JournalCluster::start_voters(
            &format!("foreign-entry-{seed}"),
            JournalTuning::new(WalFsync::Never),
            5,
        )
        .await;
        for group in JOURNAL_GROUPS {
            cluster.append(group, 3).await;
        }
        cluster.verify_reads().await;
        lead_every_group(&cluster, 1, &context).await;
        madsim::time::sleep(Duration::from_millis(200)).await;

        // P is cut off and does not campaign.
        for group in JOURNAL_GROUPS {
            cluster.engines[&(group, P)]
                .raft_handle()
                .runtime_config()
                .elect(false);
        }
        for node_id in [1, 2, 3, X] {
            cluster.policy.partition_bidirectional(P, node_id);
        }
        // X leads, is cut off and appends a write that never commits.
        lead_every_group(&cluster, X, &context).await;
        for node_id in REST {
            cluster.policy.partition_bidirectional(X, node_id);
        }
        for group in JOURNAL_GROUPS {
            assert!(
                !try_append(&mut cluster, group, X, FOREIGN).await,
                "{context}: X's write committed without a quorum"
            );
        }
        // B, C and D elect a leader at a later term and commit writes.
        for group in JOURNAL_GROUPS {
            let deadline = madsim::time::Instant::now() + Duration::from_secs(10);
            let leader = loop {
                if let Some(leader) = REST.into_iter().find(|node_id| {
                    metrics(&cluster, group, *node_id).state == openraft::ServerState::Leader
                }) {
                    break leader;
                }
                assert!(
                    madsim::time::Instant::now() < deadline,
                    "{context}: group {group}: B, C and D elected no leader"
                );
                madsim::time::sleep(Duration::from_millis(25)).await;
            };
            for index in 0..2 {
                let payload = format!("lost-{group}-{index};").into_bytes();
                assert!(
                    try_append(&mut cluster, group, leader, &payload).await,
                    "{context}: group {group}: B, C and D commit a write"
                );
            }
        }
        // They lose power and every unsynced page.
        for node_id in REST {
            cluster.stop_node(node_id).await;
        }
        for node_id in REST {
            cluster.wals[&node_id].power_loss_losing_unsynced().await;
        }
        for node_id in REST {
            cluster.start_node(node_id).await;
        }
        // P rejoins them; X stays cut off.
        for node_id in REST {
            cluster.policy.heal_bidirectional(P, node_id);
        }
        madsim::time::sleep(RECOVERY_STALL_AFTER.saturating_mul(2)).await;

        let mut outcome = Outcome::default();
        for group in JOURNAL_GROUPS {
            let mut members = Vec::new();
            for node_id in REACHED {
                members.push(member_view(&cluster, group, node_id).await);
            }
            let plan = plan_group(&members)
                .unwrap_or_else(|error| panic!("{context}: group {group}: no plan: {error}"));
            cluster.plan_leaders.insert(plan.leader);
            let mut transport = XReachesTheLeader {
                inner: SimPlanTransport {
                    cluster: &mut cluster,
                    group,
                },
                leader: plan.leader,
                extended: false,
            };
            let sent = send_plan(&mut transport, plan, &members)
                .await
                .unwrap_or_else(|error| panic!("{context}: group {group}: {plan:?}: {error}"));
            let extended = transport.extended;
            for error in &sent.refused {
                tracing::info!(%error, "{context}: group {group}: a member refused the plan");
            }
            let shaped = plan.leader == P;
            if shaped {
                outcome.shaped = outcome.shaped.saturating_add(1);
                outcome.extended = outcome.extended.saturating_add(usize::from(extended));
            }
            let deadline = madsim::time::Instant::now() + Duration::from_secs(10);
            loop {
                if foreign_applied(&cluster, group).await {
                    outcome.foreign_applied = true;
                    break;
                }
                let satisfied = REACHED.iter().all(|node_id| {
                    let history = cluster.engines[&(group, *node_id)].history();
                    history
                        .applied()
                        .is_some_and(|event| event.event_index > plan.point)
                        && history.plan().is_none()
                });
                if satisfied {
                    outcome.satisfied = outcome.satisfied.saturating_add(1);
                    outcome.shaped_satisfied =
                        outcome.shaped_satisfied.saturating_add(usize::from(shaped));
                    // The event landed: nothing below it changes any more.
                    madsim::time::sleep(Duration::from_millis(300)).await;
                    outcome.foreign_applied |= foreign_applied(&cluster, group).await;
                    break;
                }
                if madsim::time::Instant::now() >= deadline {
                    break;
                }
                madsim::time::sleep(Duration::from_millis(25)).await;
            }
        }
        outcome
    })
}

/// With every defence on, X's write is never applied, and the plan elects
/// P and is satisfied.
#[test]
fn a_leader_before_the_plan_never_extends_the_plans_leader() {
    let _guard = sim_test_guard();
    let mut shaped = 0_usize;
    for seed in JOURNAL_POWER_LOSS_SEEDS {
        let outcome = run(seed, Controls::default());
        assert!(!outcome.foreign_applied, "seed {seed}: {outcome:?}");
        assert_eq!(
            outcome.satisfied,
            JOURNAL_GROUPS.len(),
            "seed {seed}: {outcome:?}"
        );
        shaped = shaped.saturating_add(outcome.shaped);
    }
    assert!(shaped > 0, "no seed elected P: the scenario never ran");
}

/// Either defence alone keeps X's write from being applied, and each run
/// shows that the remaining one fired: without the log guard, the fence
/// keeps X's entry out of P's log and the plan is satisfied; without the
/// fence, X extends P's log and the guard keeps P from being elected.
#[test]
fn either_defence_against_a_foreign_entry_alone_suffices() {
    let _guard = sim_test_guard();
    let (mut fenced, mut guarded) = (0_usize, 0_usize);
    for seed in JOURNAL_POWER_LOSS_SEEDS {
        let outcome = run(seed, Controls::NO_LOG_GUARD);
        assert!(
            !outcome.foreign_applied,
            "seed {seed} fence only: {outcome:?}"
        );
        assert_eq!(outcome.extended, 0, "seed {seed} fence only: {outcome:?}");
        assert_eq!(
            outcome.shaped_satisfied, outcome.shaped,
            "seed {seed} fence only: {outcome:?}"
        );
        fenced = fenced.saturating_add(outcome.shaped);

        let outcome = run(seed, Controls::NO_REPLICATION_FENCE);
        assert!(
            !outcome.foreign_applied,
            "seed {seed} guard only: {outcome:?}"
        );
        assert_eq!(
            outcome.extended, outcome.shaped,
            "seed {seed} guard only: {outcome:?}"
        );
        assert_eq!(
            outcome.shaped_satisfied, 0,
            "seed {seed} guard only: {outcome:?}"
        );
        guarded = guarded.saturating_add(outcome.shaped);
    }
    assert!(
        fenced > 0 && guarded > 0,
        "no seed elected P: {fenced} {guarded}"
    );
}

/// M1 of the third #581 review, with both defences off: X extends P's log
/// after the plan, the members elect P and apply X's write.
#[test]
fn negative_control_a_leader_before_the_plan_extends_its_leader_and_its_write_applies() {
    let _guard = sim_test_guard();
    let applied = JOURNAL_POWER_LOSS_SEEDS
        .into_iter()
        .filter(|seed| run(*seed, Controls::FOREIGN_ENTRY).foreign_applied)
        .count();
    assert!(
        applied > 0,
        "no seed applied X's write with both defences off"
    );
}
