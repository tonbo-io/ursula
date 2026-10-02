//! End-to-end smoke test: tiny versions of each workload run against the real
//! state machine and runtime and produce the gate's metrics and checks.

use clap::Args;
use clap::FromArgMatches;
use ursula_state_probe::alloc::Counting;
use ursula_state_probe::workloads::Workload;
use ursula_state_probe::workloads::l2::L2Args;
use ursula_state_probe::workloads::planner::PlannerArgs;
use ursula_state_probe::workloads::w1::W1Args;
use ursula_state_probe::workloads::w2::W2Args;
use ursula_state_probe::workloads::w3::W3Args;
use ursula_state_probe::workloads::w4::W4Args;
use ursula_state_probe::workloads::w5::W5Args;

#[global_allocator]
static GLOBAL: Counting = Counting;

fn parse<A: Args + FromArgMatches>(argv: &str) -> A {
    let command = A::augment_args(clap::Command::new("t").no_binary_name(true));
    A::from_arg_matches(&command.get_matches_from(argv.split_whitespace())).expect("args")
}

fn check_met(outcome: &ursula_state_probe::out::Outcome, name: &str) -> bool {
    outcome
        .checks
        .iter()
        .find(|c| c.name == name)
        .unwrap_or_else(|| panic!("check {name} missing: {:?}", outcome.checks))
        .met
}

#[test]
fn tiny_workloads_produce_metrics_and_checks() {
    let dir = std::env::temp_dir().join(format!("ursula-state-probe-smoke-{}", std::process::id()));

    let w1 = Workload::W1(parse::<W1Args>(
        "--records=4000 --checkpoints=1000,4000 --forced-flush --flush-mib=1",
    ))
    .run(&dir, false)
    .expect("w1");
    assert_eq!(w1.metrics["records"], 4000.0);
    assert_eq!(w1.metrics["dense_entries"], 4000.0);
    assert!(
        w1.metrics["heap_bytes"] > 0.0,
        "counting allocator installed"
    );
    // The dense index keeps every record today (F1 not built).
    assert!(!check_met(&w1, "f1_dense_entries_eq_unflushed"));
    assert!(!check_met(&w1, "residual_growth_n_to_4n"));
    assert!(dir.join("w1_inline.jsonl").exists());

    let w3 = Workload::W3(parse::<W3Args>("--appends=20 --recs-per-append=10"))
        .run(&dir, false)
        .expect("w3");
    assert_eq!(w3.metrics["message_records"], 200.0);
    assert!(!check_met(&w3, "f4_message_records_per_stream"));
    assert!(check_met(&w3, "f5_staged_external_refs_per_stream"));

    let w4 = Workload::W4(parse::<W4Args>("--appends=2000 --checkpoints=2000"))
        .run(&dir, false)
        .expect("w4");
    assert_eq!(w4.metrics["receipts"], 2000.0);
    assert!(!check_met(&w4, "f3_receipt_items_per_stream"));

    let w5 = Workload::W5(parse::<W5Args>("--mode=ttl-heap --appends=500"))
        .run(&dir, false)
        .expect("w5");
    // F8 (hygiene-runtime): one armed heap entry per TTL stream.
    assert_eq!(w5.metrics["sliding.ttl_heap_entries"], 1.0);
    assert!(check_met(&w5, "f8_ttl_heap_entries"));

    let w2 = Workload::W2(parse::<W2Args>(
        "--streams=5 --rate=1 --rec-bytes=200 --hours=0.05 --measure-every-h=0.05",
    ))
    .run(&dir, false)
    .expect("w2");
    assert!(w2.metrics["appends"] > 0.0);
    assert!(check_met(&w2, "f10_max_stream_hot"));

    let planner = Workload::Planner(parse::<PlannerArgs>("--streams=10 --starved-mib=1"))
        .run(&dir, false)
        .expect("planner");
    assert_eq!(planner.metrics["drain_10.candidates"], 10.0);

    let compact = Workload::L2(parse::<L2Args>("--mode=compact"))
        .run(&dir, false)
        .expect("l2 compact");
    assert!(check_met(&compact, "f2_shared_compact_cold_memory"));

    let _ = std::fs::remove_dir_all(&dir);
}
