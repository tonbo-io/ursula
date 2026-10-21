//! Unit tests of the hosting rules, the seed role, the leader candidates and
//! the teardown steps.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use ursula_proto::placement::MembershipAnswer;
use ursula_proto::placement::NodeLifecycle;
use ursula_proto::placement::TeardownStage;
use ursula_proto::placement::ViewStage;
use ursula_shard::RaftGroupId;

use super::Admission;
use super::CandidatesOf;
use super::Counts;
use super::HostingInput;
use super::HostingPlan;
use super::HostingStep;
use super::LocalLog;
use super::LocalReplica;
use super::SeedRoleOf;
use super::Standing;
use super::Teardown;
use super::TeardownEnd;
use super::TeardownEvent;
use super::TeardownOutcome;
use super::TeardownStep;
use super::ViewInput;
use super::leader_candidates;
use super::plan_hosting;
use super::seed_role;
use crate::placement::ConfirmedStamp;
use crate::placement::ConfirmedView;
use crate::placement::LifecycleChange;
use crate::placement::PlacementView;
use crate::placement::reconcile::MembershipConfig;
use crate::placement::samples::Driver;
use crate::placement::samples::ids;
use crate::placement::samples::meta;
use crate::placement::samples::node;
use crate::placement::samples::params;
use crate::placement::samples::voters;

const G0: RaftGroupId = RaftGroupId(0);
const G1: RaftGroupId = RaftGroupId(1);
const G2: RaftGroupId = RaftGroupId(2);
const G3: RaftGroupId = RaftGroupId(3);

/// The view of the driver's state, applied up to its last entry.
fn view_of(driver: &Driver) -> PlacementView {
    PlacementView {
        stage: ViewStage::Applied {
            applied: meta(driver.term, driver.index),
            history: meta(1, 0),
        },
        state: Arc::new(driver.state.clone()),
    }
}

fn confirmed(driver: &Driver) -> ConfirmedView {
    let view = view_of(driver);
    let ViewStage::Applied { applied, history } = view.stage else {
        panic!("an applied view");
    };
    ConfirmedView::new(Arc::new(view), ConfirmedStamp { history, applied }).unwrap()
}

fn replica(log: LocalLog) -> LocalReplica {
    LocalReplica {
        log,
        counts: Counts::Voter,
        teardown: None,
    }
}

fn tearing_down(log: LocalLog, teardown: Teardown) -> LocalReplica {
    LocalReplica {
        teardown: Some(teardown),
        ..replica(log)
    }
}

/// A teardown past its first step.
const FENCING: Teardown = Teardown::Running(TeardownStep::Fence);

fn plan(
    me: u64,
    standing: Standing,
    view: ViewInput<'_>,
    local: &BTreeMap<RaftGroupId, LocalReplica>,
) -> HostingPlan {
    plan_hosting(&HostingInput {
        me: node(me),
        standing,
        view,
        local,
    })
}

/// Four nodes at RF 3: node 4 votes in groups 1 to 3. Group 1 then moved
/// to `{1, 2, 3}`, retiring node 4's replica.
fn moved_off_node_4() -> Driver {
    let mut driver = Driver::live(&[1, 2, 3, 4], 4, 3);
    assert_eq!(driver.placement(G1).target_voters(), &voters(&[2, 3, 4]));
    driver.request_move(G1, &[1, 2, 3]).unwrap();
    driver.report(G1, &[1, 2, 3]).unwrap();
    driver
}

#[test]
fn a_confirmed_view_opens_creates_tears_down_and_reports_by_role() {
    let driver = moved_off_node_4();
    let created_at = driver.cluster().retirement(G1, node(4)).unwrap().created_at;
    let view = confirmed(&driver);
    let mut local = BTreeMap::from([
        (G0, replica(LocalLog::Retired)),
        (G1, replica(LocalLog::Holding)),
        (G3, replica(LocalLog::Holding)),
    ]);
    // No role in group 1: torn down, open until fenced. Group 2 is placed
    // and absent: created.
    assert_eq!(
        plan(4, Standing::Member, ViewInput::Confirmed(&view), &local),
        HostingPlan {
            admitted: BTreeMap::from([
                (G1, Admission::Open),
                (G2, Admission::Create),
                (G3, Admission::Open),
            ]),
            steps: vec![HostingStep::TearDown { group: G1 }],
            unplaced: BTreeSet::from([G1]),
        }
    );
    // Fenced: no longer admitted, and nothing new starts.
    local.insert(G1, tearing_down(LocalLog::Holding, FENCING));
    let fenced = plan(4, Standing::Member, ViewInput::Confirmed(&view), &local);
    assert!(!fenced.admitted.contains_key(&G1));
    assert!(fenced.steps.is_empty());
    // Retired: the record is reported.
    local.insert(G1, replica(LocalLog::Retired));
    assert_eq!(
        plan(4, Standing::Member, ViewInput::Confirmed(&view), &local),
        HostingPlan {
            admitted: BTreeMap::from([(G2, Admission::Create), (G3, Admission::Open)]),
            steps: vec![HostingStep::ReportTornDown {
                group: G1,
                created_at,
            }],
            unplaced: BTreeSet::new(),
        }
    );
    // Absent after a wipe: reported too.
    local.remove(&G1);
    let wiped = plan(4, Standing::Member, ViewInput::Confirmed(&view), &local);
    assert_eq!(wiped.steps, vec![HostingStep::ReportTornDown {
        group: G1,
        created_at
    }]);
}

#[test]
fn an_unconfirmed_view_opens_only_what_the_journal_holds_and_asks_for_a_confirmation() {
    let driver = moved_off_node_4();
    let view = view_of(&driver);
    let local = BTreeMap::from([
        (G1, replica(LocalLog::Holding)),
        (G2, replica(LocalLog::Retired)),
        (G3, replica(LocalLog::Unknown)),
    ]);
    assert_eq!(
        plan(4, Standing::Member, ViewInput::Unconfirmed(&view), &local),
        HostingPlan {
            admitted: BTreeMap::from([(G1, Admission::Open), (G3, Admission::Open)]),
            steps: vec![HostingStep::ConfirmView],
            unplaced: BTreeSet::from([G1]),
        }
    );
    // Nothing a confirmation would change: no confirmation.
    let steady = Driver::live(&[1, 2, 3, 4], 4, 3);
    let local = BTreeMap::from([
        (G1, replica(LocalLog::Holding)),
        (G2, replica(LocalLog::Empty)),
        (G3, replica(LocalLog::Holding)),
    ]);
    let view = view_of(&steady);
    let quiet = plan(4, Standing::Member, ViewInput::Unconfirmed(&view), &local);
    assert!(quiet.steps.is_empty(), "{quiet:?}");
    // A view that has not bootstrapped opens what the journal holds.
    let empty = PlacementView::empty();
    let forming = plan(4, Standing::Member, ViewInput::Unconfirmed(&empty), &local);
    assert_eq!(forming.admitted.len(), 3);
    assert!(forming.steps.is_empty() && forming.unplaced.is_empty());
}

#[test]
fn a_role_ends_a_teardown_still_asking_and_never_one_past_it() {
    let mut driver = Driver::live(&[1, 2, 3, 4], 4, 3);
    driver.request_move(G1, &[1, 2, 3]).unwrap();
    // Node 4 is outgoing in group 1, with a teardown left asking.
    let view = confirmed(&driver);
    let local = BTreeMap::from([
        (G1, tearing_down(LocalLog::Holding, Teardown::Asking)),
        (G2, tearing_down(LocalLog::Holding, FENCING)),
    ]);
    let planned = plan(4, Standing::Member, ViewInput::Confirmed(&view), &local);
    assert_eq!(planned.steps, vec![HostingStep::CancelTeardown {
        group: G1
    }]);
    assert_eq!(planned.admitted.get(&G1), Some(&Admission::Open));
    // A teardown past its first step finishes: no admission, no create.
    assert_eq!(planned.admitted.get(&G2), None);
    // An unconfirmed view cancels nothing, and asks for a confirmation.
    let view = view_of(&driver);
    let unconfirmed = plan(4, Standing::Member, ViewInput::Unconfirmed(&view), &local);
    assert_eq!(unconfirmed.steps, vec![HostingStep::ConfirmView]);
}

#[test]
fn an_unknown_log_is_admitted_and_never_torn_down() {
    let driver = Driver::live(&[1, 2, 3, 4], 4, 3);
    let view = confirmed(&driver);
    // Node 4 has no role in group 0.
    let local = BTreeMap::from([(G0, replica(LocalLog::Unknown))]);
    let planned = plan(4, Standing::Member, ViewInput::Confirmed(&view), &local);
    assert_eq!(planned.admitted.get(&G0), Some(&Admission::Open));
    assert!(!planned.steps.contains(&HostingStep::TearDown { group: G0 }));
    assert_eq!(planned.unplaced, BTreeSet::from([G0]));
}

#[test]
fn a_removed_node_keeps_open_only_what_counts_as_a_voter() {
    let driver = Driver::live(&[1, 2, 3, 4], 4, 3);
    let view = confirmed(&driver);
    let local = BTreeMap::from([
        (G0, tearing_down(LocalLog::Holding, FENCING)),
        (G1, replica(LocalLog::Holding)),
        (G2, LocalReplica {
            counts: Counts::NotVoter,
            ..replica(LocalLog::Holding)
        }),
        (G3, tearing_down(LocalLog::Unknown, Teardown::Asking)),
    ]);
    assert_eq!(
        plan(4, Standing::Removed, ViewInput::Confirmed(&view), &local),
        HostingPlan {
            admitted: BTreeMap::from([(G1, Admission::Open), (G3, Admission::Open)]),
            steps: vec![HostingStep::CancelTeardown { group: G3 }],
            unplaced: BTreeSet::from([G1, G3]),
        }
    );
}

#[test]
fn a_view_showing_the_node_removed_plans_as_removed() {
    let mut driver = Driver::live(&[1, 2, 3, 4], 4, 3);
    driver.lifecycle(4, LifecycleChange::Decommission).unwrap();
    for (group, to) in [(G1, [1, 2, 3]), (G2, [1, 2, 3]), (G3, [1, 2, 3])] {
        driver.request_move(group, &to).unwrap();
        driver.report(group, &to).unwrap();
    }
    assert_eq!(
        driver.cluster().node(node(4)).unwrap().lifecycle,
        NodeLifecycle::Removed
    );
    let view = confirmed(&driver);
    let local = BTreeMap::from([(G1, replica(LocalLog::Holding))]);
    // Not torn down: removal destroys nothing.
    assert_eq!(
        plan(4, Standing::Member, ViewInput::Confirmed(&view), &local),
        HostingPlan {
            admitted: BTreeMap::from([(G1, Admission::Open)]),
            steps: Vec::new(),
            unplaced: BTreeSet::from([G1]),
        }
    );
}

#[test]
fn only_a_seeded_groups_recorded_initializer_initializes() {
    let mut driver = Driver::bootstrapped(&[1, 2, 3, 4], params(&[1, 2, 3, 4], 4, 3, 4));
    let view = confirmed(&driver);
    assert_eq!(seed_role(&view, node(2), G1), Some(SeedRoleOf::Initializer));
    assert_eq!(seed_role(&view, node(3), G1), Some(SeedRoleOf::Voter));
    assert_eq!(seed_role(&view, node(1), G1), None);
    assert_eq!(seed_role(&view, node(2), RaftGroupId(9)), None);
    driver.report(G1, &[2, 3, 4]).unwrap();
    assert_eq!(seed_role(&confirmed(&driver), node(2), G1), None);
    let forming = Driver::new();
    assert_eq!(seed_role(&confirmed(&forming), node(1), G0), None);
}

#[test]
fn leader_candidates_walk_the_placement_then_the_membership_then_the_peers() {
    let mut driver = Driver::live(&[1, 2, 3, 4], 4, 3);
    driver.request_move(G1, &[1, 2, 3]).unwrap();
    let view = view_of(&driver);
    let peers = ids(&[1, 2, 3, 4, 5, 6]);
    let outsider = leader_candidates(node(5), CandidatesOf::Group(G1), &view, None, &peers);
    assert_eq!(outsider.tiers(), [
        ids(&[2, 3]),
        ids(&[4]),
        ids(&[1]),
        ids(&[6])
    ]);
    assert_eq!(outsider.first_tier(), Some(&ids(&[2, 3])));
    assert_eq!(
        outsider.iter().map(|id| id.get()).collect::<Vec<_>>(),
        vec![2, 3, 4, 1, 6]
    );
    let membership = MembershipConfig::Uniform {
        voters: voters(&[2, 3, 4]),
        learners: ids(&[1, 7]),
    };
    let member = leader_candidates(
        node(2),
        CandidatesOf::Group(G1),
        &view,
        Some(&membership),
        &peers,
    );
    assert_eq!(member.tiers(), [
        ids(&[3]),
        ids(&[4]),
        ids(&[1]),
        ids(&[7]),
        ids(&[5, 6])
    ]);
    let stable = leader_candidates(node(5), CandidatesOf::Group(G0), &view, None, &peers);
    assert_eq!(stable.first_tier(), Some(&ids(&[1, 2, 3])));
    let blind = leader_candidates(
        node(5),
        CandidatesOf::Group(G0),
        &PlacementView::empty(),
        None,
        &peers,
    );
    assert_eq!(blind.tiers(), [ids(&[1, 2, 3, 4, 6])]);
}

/// The meta form asks meta's voters first, from the best source this node
/// has: its local meta membership, else the bootstrapped view's meta
/// voters, else its own claim. A form that skipped a source, or put the
/// peers before the voters, orders the walk differently.
#[test]
fn meta_leader_candidates_ask_the_meta_voters_from_the_best_source_first() {
    let claimed = voters(&[4, 5, 6]);
    let peers = ids(&[1, 2, 3, 4, 5, 6, 7]);
    let meta = CandidatesOf::Meta { claimed: &claimed };
    let forming = leader_candidates(node(7), meta, &PlacementView::empty(), None, &peers);
    assert_eq!(forming.tiers(), [ids(&[4, 5, 6]), ids(&[1, 2, 3])]);
    // Bootstrapped with meta voters {1,2,3}: the view outranks the claim.
    let view = view_of(&Driver::live(&[1, 2, 3, 4], 4, 3));
    let bootstrapped = leader_candidates(node(7), meta, &view, None, &peers);
    assert_eq!(bootstrapped.tiers(), [ids(&[1, 2, 3]), ids(&[4, 5, 6])]);
    // The local membership outranks both, its learners come next.
    let membership = MembershipConfig::Uniform {
        voters: voters(&[2, 5]),
        learners: ids(&[7, 3]),
    };
    let member = leader_candidates(node(7), meta, &view, Some(&membership), &peers);
    assert_eq!(member.tiers(), [ids(&[2, 5]), ids(&[3]), ids(&[1, 4, 6])]);
}

#[test]
fn a_teardown_retires_only_after_two_not_member_answers() {
    let not_member = TeardownEvent::Answered(Some(MembershipAnswer::NotMember));
    let mut teardown = Teardown::start();
    assert_eq!(teardown.stage(), TeardownStage::Ask);
    // Anything but a live leader's `NotMember` leaves it asking.
    for event in [
        TeardownEvent::Answered(None),
        TeardownEvent::Answered(Some(MembershipAnswer::Learner)),
        TeardownEvent::Answered(Some(MembershipAnswer::Voter)),
        TeardownEvent::StepDone,
    ] {
        assert_eq!(teardown.advance(event), TeardownOutcome::Continue(teardown));
    }
    assert_eq!(
        teardown.advance(TeardownEvent::Cancel),
        TeardownOutcome::Ended(TeardownEnd::Cancelled)
    );
    let TeardownOutcome::Continue(fenced) = teardown.advance(not_member) else {
        panic!("the first answer fences");
    };
    teardown = fenced;
    assert_eq!(teardown, Teardown::Running(TeardownStep::Fence));
    assert_eq!(teardown.stage(), TeardownStage::Fence);
    for expected in [
        TeardownStep::Unregister,
        TeardownStep::Stop,
        TeardownStep::AwaitIdle,
        TeardownStep::AskAgain,
    ] {
        // Past step 0 a cancel or an answer changes nothing.
        assert_eq!(
            teardown.advance(TeardownEvent::Cancel),
            TeardownOutcome::Continue(teardown)
        );
        assert_eq!(
            teardown.advance(not_member),
            TeardownOutcome::Continue(teardown)
        );
        let TeardownOutcome::Continue(next) = teardown.advance(TeardownEvent::StepDone) else {
            panic!("a step ended the teardown");
        };
        assert_eq!(next, Teardown::Running(expected));
        teardown = next;
    }
    // The second ask readmits on anything but `NotMember`.
    assert_eq!(
        teardown.advance(TeardownEvent::Answered(None)),
        TeardownOutcome::Ended(TeardownEnd::Readmitted)
    );
    assert_eq!(
        teardown.advance(TeardownEvent::StepDone),
        TeardownOutcome::Continue(teardown)
    );
    let TeardownOutcome::Continue(retiring) = teardown.advance(not_member) else {
        panic!("the second answer retires");
    };
    assert_eq!(retiring, Teardown::Running(TeardownStep::Retire));
    let TeardownOutcome::Continue(forgetting) = retiring.advance(TeardownEvent::StepDone) else {
        panic!("retiring ended the teardown");
    };
    assert_eq!(forgetting, Teardown::Running(TeardownStep::ForgetMetrics));
    assert_eq!(
        forgetting.advance(TeardownEvent::StepDone),
        TeardownOutcome::Ended(TeardownEnd::Retired)
    );
}
