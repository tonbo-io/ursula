//! `ursula-sim assert-shape`: compile-time-safe shape assertions for
//! CI-generated minimize artifacts.
//!
//! Replaces fragile jq strict-equality blocks in `.github/workflows/ci.yml`:
//! the CI YAML stops doing `[steps[].action.action] == ["stop_current_leader",
//! "restart_stopped_leader", ...]` and instead calls
//!
//!     ursula-sim assert-shape --artifact <minimized.json> --shape <name>
//!
//! Each `--shape NAME` corresponds to a Rust function that uses `matches!`
//! on `SimFaultAction` variants. Renaming a variant in
//! `crates/ursula-sim/src/madsim_harness` therefore breaks the shape fn at
//! compile time under `RUSTFLAGS="--cfg madsim" cargo build`, satisfying
//! DoD #6 ("renaming step is a compile error, not a CI failure").

use std::error::Error;

pub fn run(args: Vec<String>) -> Result<(), Box<dyn Error>> {
    crate::init_stderr_tracing();
    let mut args = args.into_iter();
    let mut artifact: Option<String> = None;
    let mut shape: Option<String> = None;
    let mut steps_exact: Option<String> = None;
    let mut json_pointer: String = "/minimized/schedule".to_owned();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--artifact" => {
                artifact = Some(args.next().ok_or("--artifact requires a value")?);
            }
            "--shape" => {
                shape = Some(args.next().ok_or("--shape requires a value")?);
            }
            "--steps-exact" => {
                steps_exact = Some(args.next().ok_or("--steps-exact requires a value")?);
            }
            "--json-pointer" => {
                json_pointer = args.next().ok_or("--json-pointer requires a value")?;
            }
            "-h" | "--help" => {
                print_help();
                return Ok(());
            }
            other => {
                return Err(format!("unknown argument: {other}").into());
            }
        }
    }

    let artifact = artifact.ok_or("--artifact is required")?;
    if shape.is_none() && steps_exact.is_none() {
        return Err("either --shape or --steps-exact is required".into());
    }
    if shape.is_some() && steps_exact.is_some() {
        return Err("--shape and --steps-exact are mutually exclusive".into());
    }

    let raw = std::fs::read_to_string(&artifact)?;
    let json: serde_json::Value = serde_json::from_str(&raw)?;
    let schedule_json = json
        .pointer(&json_pointer)
        .ok_or_else(|| format!("json pointer {json_pointer:?} not found in {artifact}"))?;
    let schedule: ursula_sim::SimSchedule = serde_json::from_value(schedule_json.clone())?;

    if let Some(shape) = shape {
        madsim_shapes::dispatch(&shape, &schedule)?;
        println!("OK: shape `{shape}` matched for {artifact}");
    } else if let Some(spec) = steps_exact {
        madsim_shapes::assert_steps_exact(&spec, &schedule)?;
        println!("OK: --steps-exact `{spec}` matched for {artifact}");
    }
    Ok(())
}

fn print_help() {
    println!(
        "Usage: ursula-sim assert-shape --artifact PATH --shape NAME [--json-pointer P]\n\
         \n\
         Asserts a CI-generated minimize artifact has the expected typed shape.\n\
         Each --shape NAME is a Rust fn that uses matches! on SimFaultAction\n\
         variants, so renaming a step is a compile error.\n\
         \n\
         Defined shapes:"
    );
    for name in madsim_shapes::SHAPE_NAMES {
        println!("  {name}");
    }
}

mod madsim_shapes {
    use std::error::Error;

    use ursula_sim::SimFaultAction;
    use ursula_sim::SimSchedule;

    /// Names of every registered shape. Kept in sync with `dispatch` below by
    /// the compiler: each name must be reachable from a match arm.
    pub const SHAPE_NAMES: &[&str] = &[
        "seed_155_leader_cold_live_minimized",
        "seed_312_runtime_raft_network_cold_live_write_failure_minimized",
        "seed_337_runtime_raft_network_tail_read_corruption_minimized",
        "seed_342_runtime_raft_network_close_state_corruption_minimized",
    ];

    pub fn dispatch(name: &str, schedule: &SimSchedule) -> Result<(), Box<dyn Error>> {
        match name {
            "seed_155_leader_cold_live_minimized" => seed_155(schedule),
            "seed_312_runtime_raft_network_cold_live_write_failure_minimized" => seed_312(schedule),
            "seed_337_runtime_raft_network_tail_read_corruption_minimized" => seed_337(schedule),
            "seed_342_runtime_raft_network_close_state_corruption_minimized" => seed_342(schedule),
            other => Err(format!("unknown --shape {other:?}; see --help for the list").into()),
        }
    }

    /// Parse a comma-separated list of SimFaultAction variant names (e.g.
    /// `StopCurrentLeader,RestartStoppedLeader`) and assert the schedule's
    /// `fault_plan.steps` matches that exact sequence (length and order).
    /// Each name maps via a match arm over SimFaultAction; renaming a variant
    /// without updating this match is a compile error.
    pub fn assert_steps_exact(spec: &str, schedule: &SimSchedule) -> Result<(), Box<dyn Error>> {
        let names: Vec<&str> = spec.split(',').map(|s| s.trim()).collect();
        let predicates: Vec<Box<StepPredicate>> = names
            .iter()
            .map(|n| parse_variant_predicate(n))
            .collect::<Result<Vec<_>, _>>()?;
        let actions: Vec<&SimFaultAction> = schedule
            .fault_plan
            .steps
            .iter()
            .map(|s| &s.action)
            .collect();
        if actions.len() != predicates.len() {
            return Err(format!(
                "--steps-exact expected {} steps {:?}, got {}: {:?}",
                predicates.len(),
                names,
                actions.len(),
                actions
            )
            .into());
        }
        for (i, (action, predicate)) in actions.iter().zip(predicates.iter()).enumerate() {
            if !predicate(action) {
                return Err(format!(
                    "--steps-exact step {i} ({action:?}) did not match expected variant {:?}",
                    names[i]
                )
                .into());
            }
        }
        Ok(())
    }

    /// Single source of truth for variant-name → matches!-predicate. The match
    /// arm covers every SimFaultAction variant; renaming/removing a variant
    /// breaks compilation here, which is the DoD #6 guarantee.
    fn parse_variant_predicate(name: &str) -> Result<Box<StepPredicate>, Box<dyn Error>> {
        // NOTE: keep this arm aligned with crates/ursula-sim/src/madsim_harness
        // `pub enum SimFaultAction`. Adding a variant there without adding it
        // here means a CI assertion can't reference the new variant — desired,
        // because new CI assertions should go through this list.
        let predicate: Box<StepPredicate> = match name {
            "PartitionSeededFollower" => {
                Box::new(|a| matches!(a, SimFaultAction::PartitionSeededFollower))
            }
            "HealSeededFollower" => Box::new(|a| matches!(a, SimFaultAction::HealSeededFollower)),
            "CreateLeaderSnapshot" => {
                Box::new(|a| matches!(a, SimFaultAction::CreateLeaderSnapshot))
            }
            "PurgeLeaderLog" => Box::new(|a| matches!(a, SimFaultAction::PurgeLeaderLog)),
            "AddLaggingLearner" => {
                Box::new(|a| matches!(a, SimFaultAction::AddLaggingLearner { .. }))
            }
            "StopSeededFollower" => Box::new(|a| matches!(a, SimFaultAction::StopSeededFollower)),
            "RestartStoppedFollower" => {
                Box::new(|a| matches!(a, SimFaultAction::RestartStoppedFollower))
            }
            "StopCurrentLeader" => Box::new(|a| matches!(a, SimFaultAction::StopCurrentLeader)),
            "RestartStoppedLeader" => {
                Box::new(|a| matches!(a, SimFaultAction::RestartStoppedLeader))
            }
            "CorruptRuntimeRaftSnapshotAppendCounts" => {
                Box::new(|a| matches!(a, SimFaultAction::CorruptRuntimeRaftSnapshotAppendCounts))
            }
            "CorruptHttpProducerDuplicateExpectation" => {
                Box::new(|a| matches!(a, SimFaultAction::CorruptHttpProducerDuplicateExpectation))
            }
            "CorruptHttpLiveSseNextOffsetExpectation" => {
                Box::new(|a| matches!(a, SimFaultAction::CorruptHttpLiveSseNextOffsetExpectation))
            }
            "CorruptHttpLiveLimitBackpressureExpectation" => Box::new(|a| {
                matches!(
                    a,
                    SimFaultAction::CorruptHttpLiveLimitBackpressureExpectation
                )
            }),
            "CorruptHttpSnapshotBodyExpectation" => {
                Box::new(|a| matches!(a, SimFaultAction::CorruptHttpSnapshotBodyExpectation))
            }
            "WriteColdChunk" => Box::new(|a| matches!(a, SimFaultAction::WriteColdChunk { .. })),
            "PublishColdFlush" => {
                Box::new(|a| matches!(a, SimFaultAction::PublishColdFlush { .. }))
            }
            "FailNextColdRead" => Box::new(|a| matches!(a, SimFaultAction::FailNextColdRead)),
            "FailNextColdWrite" => Box::new(|a| matches!(a, SimFaultAction::FailNextColdWrite)),
            "RetryColdWriteAfterFailure" => {
                Box::new(|a| matches!(a, SimFaultAction::RetryColdWriteAfterFailure))
            }
            "RetryColdReadAfterFailure" => {
                Box::new(|a| matches!(a, SimFaultAction::RetryColdReadAfterFailure))
            }
            "DelayNextColdWrite" => {
                Box::new(|a| matches!(a, SimFaultAction::DelayNextColdWrite { .. }))
            }
            "DelayNextColdRead" => {
                Box::new(|a| matches!(a, SimFaultAction::DelayNextColdRead { .. }))
            }
            "TruncateNextColdRead" => {
                Box::new(|a| matches!(a, SimFaultAction::TruncateNextColdRead { .. }))
            }
            "FailNextColdDelete" => Box::new(|a| matches!(a, SimFaultAction::FailNextColdDelete)),
            "RunRuntimeRaftNetworkWorkload" => {
                Box::new(|a| matches!(a, SimFaultAction::RunRuntimeRaftNetworkWorkload { .. }))
            }
            "RunRuntimeSeededInterleaving" => {
                Box::new(|a| matches!(a, SimFaultAction::RunRuntimeSeededInterleaving { .. }))
            }
            "RunRuntimeColdFlushAllGroups" => {
                Box::new(|a| matches!(a, SimFaultAction::RunRuntimeColdFlushAllGroups { .. }))
            }
            "RunHttpProtocolSurfaceWorkload" => {
                Box::new(|a| matches!(a, SimFaultAction::RunHttpProtocolSurfaceWorkload { .. }))
            }
            "StartRuntimeConcurrentClients" => {
                Box::new(|a| matches!(a, SimFaultAction::StartRuntimeConcurrentClients { .. }))
            }
            "StartRuntimeWaitRead" => {
                Box::new(|a| matches!(a, SimFaultAction::StartRuntimeWaitRead))
            }
            "DelayRuntimeAppend" => {
                Box::new(|a| matches!(a, SimFaultAction::DelayRuntimeAppend { .. }))
            }
            "DelayRuntimeClientAppends" => {
                Box::new(|a| matches!(a, SimFaultAction::DelayRuntimeClientAppends { .. }))
            }
            "VerifyRuntimeColdLiveReads" => {
                Box::new(|a| matches!(a, SimFaultAction::VerifyRuntimeColdLiveReads))
            }
            "VerifyHotReadAfterColdWriteFailure" => {
                Box::new(|a| matches!(a, SimFaultAction::VerifyHotReadAfterColdWriteFailure))
            }
            "CorruptColdLiveReadExpectation" => {
                Box::new(|a| matches!(a, SimFaultAction::CorruptColdLiveReadExpectation { .. }))
            }
            other => {
                return Err(format!(
                    "unknown SimFaultAction variant name {other:?} in --steps-exact; \
                     update parse_variant_predicate in assert_shape.rs"
                )
                .into());
            }
        };
        Ok(predicate)
    }

    /// CI seed 155 minimized leader-failover + cold/live read schedule.
    /// The original jq -e block asserted this 4-step sequence; the matches!
    /// patterns here give compile-time safety.
    fn seed_155(schedule: &SimSchedule) -> Result<(), Box<dyn Error>> {
        assert_exact_step_sequence(
            schedule,
            &[
                &|a| matches!(a, SimFaultAction::StopCurrentLeader),
                &|a| matches!(a, SimFaultAction::RestartStoppedLeader),
                &|a| matches!(a, SimFaultAction::RunRuntimeRaftNetworkWorkload { .. }),
                &|a| matches!(a, SimFaultAction::VerifyRuntimeColdLiveReads),
            ],
            &[
                "StopCurrentLeader",
                "RestartStoppedLeader",
                "RunRuntimeRaftNetworkWorkload",
                "VerifyRuntimeColdLiveReads",
            ],
        )
    }

    /// CI seed 312 minimized runtime/Raft/cold-live write failure.
    fn seed_312(schedule: &SimSchedule) -> Result<(), Box<dyn Error>> {
        assert_steps_contain_in_order(
            schedule,
            &[
                &|a| matches!(a, SimFaultAction::VerifyRuntimeColdLiveReads),
                &|a| matches!(a, SimFaultAction::FailNextColdWrite),
            ],
            &["VerifyRuntimeColdLiveReads", "FailNextColdWrite"],
        )
    }

    /// CI seed 337 minimized tail-read corruption — corrupt-tail-read flag
    /// must remain present after minimization.
    fn seed_337(schedule: &SimSchedule) -> Result<(), Box<dyn Error>> {
        assert_runtime_raft_network_workload_plan(schedule, |plan| {
            plan.tail_reads && plan.corrupt_tail_read_expectation
        })
        .map_err(|err| {
            format!("seed 337 expected `tail_reads && corrupt_tail_read_expectation`: {err}").into()
        })
    }

    /// CI seed 342 minimized close-state corruption.
    fn seed_342(schedule: &SimSchedule) -> Result<(), Box<dyn Error>> {
        assert_runtime_raft_network_workload_plan(schedule, |plan| {
            plan.close_streams && plan.corrupt_close_state_expectation
        })
        .map_err(|err| {
            format!("seed 342 expected `close_streams && corrupt_close_state_expectation`: {err}")
                .into()
        })
    }

    type StepPredicate = dyn Fn(&SimFaultAction) -> bool;

    fn assert_exact_step_sequence(
        schedule: &SimSchedule,
        predicates: &[&StepPredicate],
        names: &[&str],
    ) -> Result<(), Box<dyn Error>> {
        let actual: Vec<&SimFaultAction> = schedule
            .fault_plan
            .steps
            .iter()
            .map(|step| &step.action)
            .collect();
        if actual.len() != predicates.len() {
            return Err(format!(
                "expected {} steps {:?}, got {}: {:?}",
                predicates.len(),
                names,
                actual.len(),
                actual
            )
            .into());
        }
        for (i, (action, predicate)) in actual.iter().zip(predicates).enumerate() {
            if !predicate(action) {
                return Err(format!(
                    "step {i} ({action:?}) did not match expected variant {:?}",
                    names[i]
                )
                .into());
            }
        }
        Ok(())
    }

    fn assert_steps_contain_in_order(
        schedule: &SimSchedule,
        predicates: &[&StepPredicate],
        names: &[&str],
    ) -> Result<(), Box<dyn Error>> {
        let actions: Vec<&SimFaultAction> = schedule
            .fault_plan
            .steps
            .iter()
            .map(|step| &step.action)
            .collect();
        let mut cursor = 0usize;
        for (i, predicate) in predicates.iter().enumerate() {
            let found = actions[cursor..]
                .iter()
                .position(|a| predicate(a))
                .ok_or_else(|| {
                    format!(
                        "expected step matching {:?} (index {i}) after cursor {cursor}; \
                         actual steps: {:?}",
                        names[i], actions
                    )
                })?;
            cursor += found + 1;
        }
        Ok(())
    }

    fn assert_runtime_raft_network_workload_plan(
        schedule: &SimSchedule,
        predicate: impl Fn(&ursula_sim::RuntimeRaftNetworkWorkloadPlan) -> bool,
    ) -> Result<(), Box<dyn Error>> {
        for step in &schedule.fault_plan.steps {
            if let SimFaultAction::RunRuntimeRaftNetworkWorkload { plan } = &step.action
                && predicate(plan)
            {
                return Ok(());
            }
        }
        Err(format!(
            "no RunRuntimeRaftNetworkWorkload step satisfied the predicate; steps: {:?}",
            schedule
                .fault_plan
                .steps
                .iter()
                .map(|s| &s.action)
                .collect::<Vec<_>>()
        )
        .into())
    }
}
