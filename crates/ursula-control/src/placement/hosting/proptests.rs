//! Hosting property tests H1 to H9 of the design (issue #768), over states
//! the kernel reaches from random commands and history events, any node,
//! any local journal, standing and teardown, and views that are empty,
//! forming or bootstrapped, confirmed or not. The teardown property runs
//! random events through [`Teardown`].

use std::collections::BTreeMap;
use std::sync::Arc;

use proptest::prelude::ProptestConfig;
use proptest::test_runner::RngSeed;
use ursula_proto::placement::MembershipAnswer;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::NodeLifecycle;
use ursula_proto::placement::ReplicaRole;
use ursula_proto::placement::ViewStage;
use ursula_shard::RaftGroupId;

use super::Admission;
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
use super::plan_hosting;
use super::seed_role;
use crate::placement::ConfirmedStamp;
use crate::placement::ConfirmedView;
use crate::placement::ControlPlaneState;
use crate::placement::GroupPlacement;
use crate::placement::PlacementView;
use crate::placement::proptests::reachable;
use crate::placement::samples::meta;
use crate::placement::samples::node;

fn ensure(condition: bool, violation: impl FnOnce() -> String) -> Result<(), String> {
    if condition { Ok(()) } else { Err(violation()) }
}

fn local_replica(log: u8, voter: bool, teardown: u8) -> LocalReplica {
    LocalReplica {
        log: match log {
            0 => LocalLog::Unknown,
            1 => LocalLog::Empty,
            2 => LocalLog::Holding,
            _ => LocalLog::Retired,
        },
        counts: if voter {
            Counts::Voter
        } else {
            Counts::NotVoter
        },
        teardown: match teardown {
            0 => Some(Teardown::Asking),
            1 => Some(Teardown::Running(TeardownStep::Fence)),
            2 => Some(Teardown::Running(TeardownStep::AskAgain)),
            _ => None,
        },
    }
}

/// Every property of the plan for `input`, whose view `view` is, with
/// `confirmed` its confirmation when it has one.
fn check_plan(
    input: &HostingInput<'_>,
    view: &PlacementView,
    confirmed: Option<&ConfirmedView>,
    plan: &HostingPlan,
) -> Result<(), String> {
    let me = input.me;
    let cluster = view.cluster();
    let removed = input.standing == Standing::Removed
        || cluster
            .and_then(|cluster| cluster.node(me))
            .is_some_and(|record| record.lifecycle == NodeLifecycle::Removed);
    let is_confirmed = matches!(input.view, ViewInput::Confirmed(_));
    let role = |group: RaftGroupId| cluster.and_then(|cluster| cluster.role(group, me));
    let local = |group: RaftGroupId| input.local.get(&group);
    let held = |group: RaftGroupId| local(group).is_some_and(|replica| replica.log.is_held());
    let teardown = |group: RaftGroupId| local(group).and_then(|replica| replica.teardown);
    let admissible =
        |group: RaftGroupId| held(group) && !matches!(teardown(group), Some(Teardown::Running(_)));

    for (group, admission) in &plan.admitted {
        let group = *group;
        match admission {
            // H6: `Open` only for a held log, never past a teardown's step 0.
            Admission::Open => ensure(admissible(group), || {
                format!("H6: group {} opened, {:?}", group.0, local(group))
            })?,
            // H1, H7: `Create` only from a confirmed bootstrapped view, for a
            // `Voter` or `Incoming` role, an absent or retired log, no
            // teardown, and not on a removed node.
            Admission::Create => ensure(
                is_confirmed
                    && !removed
                    && matches!(
                        role(group),
                        Some(ReplicaRole::Voter | ReplicaRole::Incoming)
                    )
                    && !held(group)
                    && teardown(group).is_none(),
                || format!("H1: group {} created, {:?}", group.0, local(group)),
            )?,
        }
    }
    if removed {
        // H3: exactly the held replicas that count as voters, and nothing
        // started.
        for (group, replica) in input.local {
            let wanted = admissible(*group) && replica.counts == Counts::Voter;
            ensure(plan.admitted.contains_key(group) == wanted, || {
                format!("H3: removed node's admission of group {}", group.0)
            })?;
        }
        ensure(
            plan.steps
                .iter()
                .all(|step| matches!(step, HostingStep::CancelTeardown { .. })),
            || format!("H3: a removed node planned {:?}", plan.steps),
        )?;
        let kept: Vec<RaftGroupId> = plan.admitted.keys().copied().collect();
        let unplaced: Vec<RaftGroupId> = plan.unplaced.iter().copied().collect();
        return ensure(kept == unplaced, || {
            "a removed node's unplaced replicas".to_owned()
        });
    }
    // H4: every held replica is admitted `Open` until its teardown passes
    // step 0, an unknown one always.
    for group in input.local.keys() {
        if admissible(*group) {
            ensure(plan.admitted.get(group) == Some(&Admission::Open), || {
                format!("H4: held group {} not admitted", group.0)
            })?;
        }
    }
    for (position, step) in plan.steps.iter().enumerate() {
        match step {
            HostingStep::ConfirmView => {
                ensure(position == 0 && !is_confirmed, || {
                    "H5: a confirmation not first, or from a confirmed view".to_owned()
                })?;
            }
            // H2: from a confirmed bootstrapped view, for an empty or holding
            // log without a role and without a teardown.
            HostingStep::TearDown { group } => ensure(
                is_confirmed
                    && role(*group).is_none()
                    && matches!(
                        local(*group).map(|replica| replica.log),
                        Some(LocalLog::Empty | LocalLog::Holding)
                    )
                    && teardown(*group).is_none(),
                || format!("H2: teardown of group {}", group.0),
            )?,
            HostingStep::CancelTeardown { group } => ensure(
                is_confirmed
                    && teardown(*group) == Some(Teardown::Asking)
                    && (role(*group).is_some()
                        || local(*group).map(|replica| replica.log) == Some(LocalLog::Unknown)),
                || format!("cancel of group {}", group.0),
            )?,
            // H8: a report only for a record, without a teardown, of a log
            // that is retired or absent.
            HostingStep::ReportTornDown { group, created_at } => ensure(
                is_confirmed
                    && role(*group).is_none()
                    && cluster
                        .and_then(|cluster| cluster.retirement(*group, me))
                        .is_some_and(|record| record.created_at == *created_at)
                    && teardown(*group).is_none()
                    && !held(*group),
                || format!("H8: report of group {}", group.0),
            )?,
        }
    }
    let Some(cluster) = cluster else {
        ensure(plan.unplaced.is_empty(), || {
            "unplaced without a cluster".to_owned()
        })?;
        return ensure(
            plan.steps.is_empty()
                && plan
                    .admitted
                    .values()
                    .all(|admission| *admission == Admission::Open),
            || format!("H5: an unbootstrapped view planned {plan:?}"),
        );
    };
    for group in input.local.keys() {
        ensure(
            plan.unplaced.contains(group) == (held(*group) && role(*group).is_none()),
            || format!("unplaced group {}", group.0),
        )?;
    }
    if is_confirmed {
        // The rules are complete: what they call for is planned.
        for (group, placement) in cluster.groups().iter() {
            let creates = matches!(
                placement.role(me),
                Some(ReplicaRole::Voter | ReplicaRole::Incoming)
            ) && !held(group)
                && teardown(group).is_none();
            ensure(
                !creates || plan.admitted.get(&group) == Some(&Admission::Create),
                || format!("H1: group {} not created", group.0),
            )?;
        }
        for (group, replica) in input.local {
            let tears = role(*group).is_none()
                && matches!(replica.log, LocalLog::Empty | LocalLog::Holding)
                && replica.teardown.is_none();
            ensure(
                !tears
                    || plan
                        .steps
                        .contains(&HostingStep::TearDown { group: *group }),
                || format!("H2: group {} not torn down", group.0),
            )?;
            let cancels = replica.teardown == Some(Teardown::Asking) && role(*group).is_some();
            ensure(
                !cancels
                    || plan
                        .steps
                        .contains(&HostingStep::CancelTeardown { group: *group }),
                || format!("H2: a role did not end group {}'s teardown", group.0),
            )?;
        }
        for (group, records) in cluster.retirements() {
            if let Some(record) = records.get(&me)
                && teardown(*group).is_none()
                && !held(*group)
            {
                let report = HostingStep::ReportTornDown {
                    group: *group,
                    created_at: record.created_at,
                };
                ensure(plan.steps.contains(&report), || {
                    format!("H8: group {} not reported", group.0)
                })?;
            }
        }
    } else {
        // H5: an unconfirmed view creates, tears down, cancels and reports
        // nothing, and asks for a confirmation exactly when confirming the
        // same view would plan otherwise.
        ensure(
            plan.admitted
                .values()
                .all(|admission| *admission == Admission::Open)
                && plan
                    .steps
                    .iter()
                    .all(|step| *step == HostingStep::ConfirmView),
            || format!("H5: an unconfirmed view planned {plan:?}"),
        )?;
        if let Some(confirmed) = confirmed {
            let as_confirmed = plan_hosting(&HostingInput {
                view: ViewInput::Confirmed(confirmed),
                ..*input
            });
            let differs = as_confirmed.admitted != plan.admitted || !as_confirmed.steps.is_empty();
            ensure(
                plan.steps.contains(&HostingStep::ConfirmView) == differs,
                || format!("H5: confirmation {:?} against {as_confirmed:?}", plan.steps),
            )?;
        }
    }
    Ok(())
}

proptest::proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        rng_seed: RngSeed::Fixed(768),
        // The seed is fixed, so a failure reproduces without a regression file.
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    /// H1 to H8.
    #[test]
    fn every_hosting_plan_follows_the_rules(
        node_count in 1_u64..=6,
        group_count in 1_u32..=4,
        rf in 1_u8..=3,
        live in proptest::bool::weighted(0.8),
        steps in proptest::collection::vec(
            (0_u8..13, proptest::num::u8::ANY, proptest::num::u8::ANY, proptest::num::u8::ANY),
            0..60,
        ),
        inputs in proptest::collection::vec(
            (
                1_u64..=7,
                proptest::bool::weighted(0.1),
                0_u8..5,
                proptest::collection::vec(
                    proptest::option::of((0_u8..4, proptest::bool::ANY, 0_u8..5)),
                    6,
                ),
            ),
            8,
        ),
    ) {
        let driver = reachable(node_count, group_count, rf, live, &steps);
        let applied = meta(driver.term, driver.index);
        let history = meta(1, 0);
        for (me, removed, kind, replicas) in inputs {
            let state = match kind {
                0 => None,
                1 => Some(ControlPlaneState::default()),
                2 => Some(ControlPlaneState::FormationCut),
                _ => Some(driver.state.clone()),
            };
            let view = Arc::new(match state {
                None => PlacementView::empty(),
                Some(state) => PlacementView {
                    stage: ViewStage::Applied { applied, history },
                    state: Arc::new(state),
                },
            });
            let confirmed = ConfirmedView::new(Arc::clone(&view), ConfirmedStamp { history, applied }).ok();
            let local: BTreeMap<RaftGroupId, LocalReplica> = replicas
                .iter()
                .enumerate()
                .filter_map(|(group, replica)| {
                    let (log, voter, teardown) = (*replica)?;
                    Some((RaftGroupId(u32::try_from(group).unwrap()), local_replica(log, voter, teardown)))
                })
                .collect();
            let standing = if removed { Standing::Removed } else { Standing::Member };
            let views = [
                Some(ViewInput::Unconfirmed(&view)),
                confirmed.as_ref().map(ViewInput::Confirmed),
            ];
            for input_view in views.into_iter().flatten() {
                let input = HostingInput {
                    me: node(me),
                    standing,
                    view: input_view,
                    local: &local,
                };
                let plan = plan_hosting(&input);
                if let Err(violation) = check_plan(&input, &view, confirmed.as_ref(), &plan) {
                    proptest::prop_assert!(false, "{violation}: {plan:?} for {local:?} of node {me} on {:?}", view.state);
                }
            }
        }
    }

    /// H9: in every group the confirmed view shows `Seeded`, exactly one
    /// node is the `Initializer`, it is a seed voter, and the nodes with a
    /// seed role are exactly the seed voters. A `Live` or unknown group
    /// gives every node `None`.
    #[test]
    fn only_a_seeded_groups_voters_have_a_seed_role(
        node_count in 1_u64..=6,
        group_count in 1_u32..=4,
        rf in 1_u8..=3,
        steps in proptest::collection::vec(
            (0_u8..13, proptest::num::u8::ANY, proptest::num::u8::ANY, proptest::num::u8::ANY),
            0..30,
        ),
    ) {
        let driver = reachable(node_count, group_count, rf, false, &steps);
        let applied = meta(driver.term, driver.index);
        let history = meta(1, 0);
        let view = Arc::new(PlacementView {
            stage: ViewStage::Applied { applied, history },
            state: Arc::new(driver.state.clone()),
        });
        let confirmed = ConfirmedView::new(view, ConfirmedStamp { history, applied }).unwrap();
        let cluster = driver.cluster();
        for group in 0..=group_count {
            let group = RaftGroupId(group);
            let roles: BTreeMap<NodeId, SeedRoleOf> = (1..=node_count.checked_add(1).unwrap())
                .map(NodeId::new)
                .filter_map(|me| seed_role(&confirmed, me, group).map(|role| (me, role)))
                .collect();
            match cluster.group(group) {
                Some(GroupPlacement::Seeded { voters, .. }) => {
                    let initializers: Vec<NodeId> = roles
                        .iter()
                        .filter(|(_, role)| **role == SeedRoleOf::Initializer)
                        .map(|(me, _)| *me)
                        .collect();
                    proptest::prop_assert_eq!(initializers.len(), 1, "H9: initializers {:?}", initializers);
                    proptest::prop_assert!(
                        initializers.iter().all(|me| voters.contains(*me)),
                        "H9: initializer {:?} outside the seed voters {:?}", initializers, voters
                    );
                    proptest::prop_assert!(
                        roles.keys().copied().eq(voters.iter()),
                        "H9: seed roles {:?} for the seed voters {:?}", roles, voters
                    );
                }
                Some(GroupPlacement::Live { .. }) | None => {
                    proptest::prop_assert!(roles.is_empty(), "H9: seed roles {:?} outside a seed", roles);
                }
            }
        }
    }

    /// A teardown retires a log only after a live leader answered
    /// `NotMember` at its first step and again once its replica stopped,
    /// and is cancelled only at its first step.
    #[test]
    fn a_teardown_retires_only_on_two_answers(events in proptest::collection::vec(0_u8..7, 0..40)) {
        let mut teardown = Teardown::start();
        let mut path = vec![teardown];
        for event in events {
            let event = match event {
                0 => TeardownEvent::Answered(None),
                1 => TeardownEvent::Answered(Some(MembershipAnswer::Voter)),
                2 => TeardownEvent::Answered(Some(MembershipAnswer::Learner)),
                3 => TeardownEvent::Cancel,
                4 => TeardownEvent::Answered(Some(MembershipAnswer::NotMember)),
                _ => TeardownEvent::StepDone,
            };
            let before = teardown;
            match teardown.advance(event) {
                TeardownOutcome::Continue(next) => {
                    if next != before {
                        let entered_by_answer = matches!(
                            next,
                            Teardown::Running(TeardownStep::Fence | TeardownStep::Retire)
                        );
                        proptest::prop_assert_eq!(
                            entered_by_answer,
                            event == TeardownEvent::Answered(Some(MembershipAnswer::NotMember))
                        );
                        path.push(next);
                    }
                    teardown = next;
                }
                TeardownOutcome::Ended(end) => {
                    match end {
                        TeardownEnd::Cancelled => proptest::prop_assert_eq!(before, Teardown::Asking),
                        TeardownEnd::Readmitted => proptest::prop_assert_eq!(
                            before,
                            Teardown::Running(TeardownStep::AskAgain)
                        ),
                        TeardownEnd::Retired => {
                            proptest::prop_assert_eq!(path, vec![
                                Teardown::Asking,
                                Teardown::Running(TeardownStep::Fence),
                                Teardown::Running(TeardownStep::Unregister),
                                Teardown::Running(TeardownStep::Stop),
                                Teardown::Running(TeardownStep::AwaitIdle),
                                Teardown::Running(TeardownStep::AskAgain),
                                Teardown::Running(TeardownStep::Retire),
                                Teardown::Running(TeardownStep::ForgetMetrics),
                            ]);
                        }
                    }
                    break;
                }
            }
        }
    }
}
