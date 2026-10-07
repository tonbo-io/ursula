//! Strict replay of a three-node cluster on the per-core journal.
//!
//! `madsim::runtime::Runtime::check_determinism` runs a workload twice with
//! one seed, each run on a fresh thread, and fails at the first draw of
//! madsim's RNG that differs between them: a task woken in another order, a
//! `select!` branch picked differently, a timer firing at another instant.
//! The outcome-level replays compare only what a run reports, so they missed
//! the thread-local RNGs seeded from process-wide counters (tokio's watch
//! channel, futures' `select!`) that once made two runs of a seed schedule
//! differently while every outcome matched.

use std::future::Future;

use madsim::runtime::Handle;
use madsim::runtime::Runtime;
use ursula_raft::MadsimOpenRaftRuntime;

use super::disk_tests::segment_crash_rounds;
use super::seeds_from_env;
use super::sim_test_guard;
use crate::madsim_harness::ThreeNodeRaftSimConfig;
use crate::madsim_harness::ThreeNodeRaftSimOutcome;
use crate::madsim_harness::raft_scenarios::run_leader_failover_inner;
use crate::madsim_harness::raft_scenarios::run_no_fault_inner;
use crate::madsim_harness::raft_scenarios::run_restart_follower_inner;

/// Each seed runs its workload twice. A divergence shows only when a seed
/// reaches the racing wake-ups or `select!` branches, so the set is wide. A
/// seed costs a few milliseconds.
const STRICT_REPLAY_SEEDS: [u64; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];

/// Runs `scenario` under `check_determinism` for every strict-replay seed.
fn check_strict_replay<F>(scenario: fn() -> F)
where F: Future<Output = ThreeNodeRaftSimOutcome> + 'static {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("STRICT_REPLAY_SEEDS", &STRICT_REPLAY_SEEDS) {
        let outcome = Runtime::check_determinism(seed, madsim::Config::default(), scenario);
        assert_eq!(outcome.seed, seed);
        assert!(
            outcome.appended_log_index > 0,
            "seed {seed}: the write committed"
        );
    }
}

/// The scenario's configuration for the seed of the current runtime.
fn strict_replay_config() -> ThreeNodeRaftSimConfig {
    ThreeNodeRaftSimConfig::new(Handle::current().seed(), "ursula-sim-strict-replay")
}

/// A three-node group on the per-core journal elects a leader, commits a
/// write and reads it on every node.
#[test]
fn strict_replay_no_fault_on_the_journal() {
    check_strict_replay(|| async {
        let config = strict_replay_config();
        MadsimOpenRaftRuntime::scope(config.seed, run_no_fault_inner(config)).await
    });
}

/// A follower stops, the others commit a write without it, and it restarts
/// from its journal and reads the write.
#[test]
fn strict_replay_restart_follower_on_the_journal() {
    check_strict_replay(|| async {
        let config = strict_replay_config();
        MadsimOpenRaftRuntime::scope(config.seed, run_restart_follower_inner(config)).await
    });
}

/// The leader shuts down, the others elect a new one, and the old leader
/// restarts from its journal and follows.
#[test]
fn strict_replay_leader_failover_on_the_journal() {
    check_strict_replay(|| async {
        let config = strict_replay_config();
        MadsimOpenRaftRuntime::scope(config.seed, run_leader_failover_inner(config)).await
    });
}

/// Journals that rotate, purge and rewrite segments and read evicted entries
/// from disk, while followers crash, schedule the same in both runs of a
/// seed (`segment_crash_rounds`, under both fsync policies).
#[test]
fn strict_replay_segment_rotation_purge_and_disk_reads() {
    let _guard = sim_test_guard();
    for seed in seeds_from_env("STRICT_REPLAY_SEEDS", &STRICT_REPLAY_SEEDS[..4]) {
        let work = Runtime::check_determinism(seed, madsim::Config::default(), || async {
            let seed = Handle::current().seed();
            let fsync = if seed % 2 == 0 {
                ursula_config::WalFsync::Always
            } else {
                ursula_config::WalFsync::Never
            };
            MadsimOpenRaftRuntime::scope(seed, segment_crash_rounds(seed, fsync)).await
        });
        assert!(
            work.rotations > 0 && work.reclaims > 0,
            "seed {seed}: {work:?}"
        );
    }
}
