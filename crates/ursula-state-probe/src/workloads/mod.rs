//! Workload drivers. W1 to W6 and the planner probe drive the real
//! `StreamStateMachine` (L1); [`l2`] drives the real `ShardRuntime` on both
//! engines (L2); [`wal`] drives the Raft log stores on one core journal. Each returns an [`crate::out::Outcome`] for the gate and
//! writes one JSONL row per checkpoint.

pub mod cadence;
pub mod l2;
pub mod planner;
pub mod w1;
pub mod w2;
pub mod w3;
pub mod w4;
pub mod w5;
pub mod wal;

use std::path::Path;

use anyhow::Result;

use crate::out::Outcome;
use crate::out::Sink;

/// One workload invocation with its arguments.
#[derive(Debug, Clone)]
pub enum Workload {
    W1(w1::W1Args),
    Cadence(cadence::CadenceArgs),
    W2(w2::W2Args),
    W3(w3::W3Args),
    W4(w4::W4Args),
    W5(w5::W5Args),
    Planner(planner::PlannerArgs),
    L2(l2::L2Args),
    Wal(wal::WalArgs),
}

impl Workload {
    /// JSONL file stem for this invocation.
    pub fn name(&self) -> String {
        match self {
            Workload::W1(args) => w1::default_name(args),
            Workload::Cadence(args) => cadence::default_name(args),
            Workload::W2(args) => w2::default_name(args),
            Workload::W3(args) => w3::default_name(args),
            Workload::W4(args) => w4::default_name(args),
            Workload::W5(args) => w5::default_name(args),
            Workload::Planner(args) => args
                .name
                .clone()
                .unwrap_or_else(|| "planner_cost".to_owned()),
            Workload::L2(args) => l2::default_name(args),
            Workload::Wal(args) => wal::default_name(args),
        }
    }

    /// Run into `<out_dir>/<name>.jsonl`.
    pub fn run(&self, out_dir: &Path, echo: bool) -> Result<Outcome> {
        let mut sink = Sink::new(out_dir, &self.name(), echo)?;
        match self {
            Workload::W1(args) => w1::run(args, &mut sink),
            Workload::Cadence(args) => cadence::run(args, &mut sink),
            Workload::W2(args) => w2::run(args, &mut sink),
            Workload::W3(args) => w3::run(args, &mut sink),
            Workload::W4(args) => w4::run(args, &mut sink),
            Workload::W5(args) => w5::run(args, &mut sink),
            Workload::Planner(args) => planner::run(args, &mut sink),
            Workload::L2(args) => l2::run(args, &mut sink),
            Workload::Wal(args) => wal::run(args, &mut sink),
        }
    }
}
