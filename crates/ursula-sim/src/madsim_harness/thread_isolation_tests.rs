//! A seed's run does not depend on what ran before it in the process (#638).
//!
//! madsim runs a simulation on the calling OS thread and does not reset its
//! thread-locals. Lazily seeded ones draw from madsim's intercepted
//! `getrandom`, so from the run's own random stream, only on their first use
//! on a thread:
//!
//! - `rand`'s thread RNG. A journal-backed node that starts without a
//!   cluster id draws one from it (`generate_cluster_id`), and the first such
//!   draw on a thread seeds it from madsim's stream. A later run on the
//!   thread does not spend those bytes, every later draw of madsim's RNG
//!   shifts, and the run schedules otherwise. Seeding it before the run
//!   changed corpus seeds 59, 61 and 146, and a constant cluster id under
//!   madsim made that change vanish.
//! - std's `RandomState` keys: madsim seeds them from the runtime's seed only
//!   on a thread that has not drawn them yet, so a later run on the thread
//!   hashes with the keys the earlier run left.
//!
//! `run_with_madsim` therefore runs each simulation on a fresh thread
//! (`ursula_raft::run_isolated`). Process-wide state is not reset by a fresh
//! thread, so it must not reach the schedule: the cold object nonce comes
//! from a generator seeded with the run's seed under madsim, one sequence
//! per runtime, and tokio's thread RNG, which a process-wide counter seeds,
//! decides nothing (every sim-reachable tokio `select!` is `biased;` and the
//! `rt` shim's `watch` is `sim_watch`, both checked by the `nondeterminism`
//! audit).

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;

use super::sim_test_guard;
use crate::madsim_harness::SimReport;
use crate::madsim_harness::SimSchedule;
use crate::madsim_harness::SimScheduledRecord;
use crate::madsim_harness::SimTrace;
use crate::madsim_harness::run_with_madsim;

/// The seeds of the schedule corpus, one or more per scenario family the
/// sweep draws.
fn corpus_seeds() -> Vec<u64> {
    serde_json::from_str::<Vec<SimScheduledRecord>>(include_str!(
        "../../corpus/schedule-smoke.json"
    ))
    .expect("parse the schedule corpus")
    .into_iter()
    .map(|record| record.schedule.seed)
    .collect()
}

/// A run's report with every event it recorded, stable or not.
fn traced_run(seed: u64) -> (SimReport, SimTrace) {
    let report = SimSchedule::generate(seed).run();
    (report, SimTrace::last_recorded())
}

/// One seed run twice in a row on the test thread gives the same report and
/// the same full trace.
#[test]
fn a_seed_run_twice_on_one_thread_replays() {
    let _guard = sim_test_guard();
    for seed in corpus_seeds() {
        let first = traced_run(seed);
        let second = traced_run(seed);
        assert!(
            !first.1.events.is_empty(),
            "seed {seed}: the run recorded its events"
        );
        assert!(first == second, "seed {seed}: the second run diverged");
    }
}

/// Seeds swept one after another on one thread, as `ursula-sim smoke` sweeps
/// them, each give what that seed gives when it runs alone on a thread of
/// its own.
#[test]
fn sweep_seeds_in_sequence_replays() {
    let _guard = sim_test_guard();
    let seeds = corpus_seeds();
    let swept = seeds
        .iter()
        .map(|&seed| traced_run(seed))
        .collect::<Vec<_>>();
    for (seed, swept) in seeds.into_iter().zip(swept) {
        let alone = std::thread::spawn(move || traced_run(seed))
            .join()
            .expect("the seed's run alone");
        assert!(
            swept == alone,
            "seed {seed}: the sweep's run differs from the seed alone"
        );
    }
}

/// The std hash keys a run draws follow its seed, whatever ran on the
/// calling thread before it.
#[test]
fn std_hash_keys_follow_the_seed_replays() {
    let _guard = sim_test_guard();
    let keys = |seed| {
        run_with_madsim(seed, async {
            (
                RandomState::new().hash_one(0_u64),
                RandomState::new().hash_one(0_u64),
            )
        })
    };
    let first = keys(7);
    assert_ne!(keys(8), first, "the keys follow the seed");
    assert_eq!(keys(7), first, "seed 7 draws its keys again");
}
