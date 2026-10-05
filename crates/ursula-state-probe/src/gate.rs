//! Suites and the ratchet gate (§7.2, §7.3).
//!
//! The `pr` suite runs reduced-scale W1 and W3 to W6, the reduced starvation
//! reproduction, the planner probe and the L2 engine probes in a few minutes.
//! The `nightly` suite runs the full-scale audit set. The gate compares a
//! suite's [`Outcome`] with a ratchet file:
//!
//! - every metric must stay at or below its recorded baseline plus the
//!   tolerance (10%); one that drops below the baseline minus the tolerance is
//!   reported so the ratchet can be tightened;
//! - every formula check records whether its §7.2 target is met today; losing
//!   a met target fails, and newly meeting one is reported for tightening;
//! - metrics and checks missing from either side fail, so coverage cannot
//!   silently shrink.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use clap::Args;
use clap::FromArgMatches;
use serde::Deserialize;
use serde::Serialize;

use crate::out::Outcome;
use crate::workloads::Workload;
use crate::workloads::cadence::CadenceArgs;
use crate::workloads::l2::L2Args;
use crate::workloads::planner::PlannerArgs;
use crate::workloads::w1::W1Args;
use crate::workloads::w2::W2Args;
use crate::workloads::w3::W3Args;
use crate::workloads::w4::W4Args;
use crate::workloads::w5::W5Args;

/// Default ratchet tolerance (§7.2: today's value plus 10%).
pub const DEFAULT_TOLERANCE: f64 = 0.10;

fn parse<A: Args + FromArgMatches>(argv: &str) -> Result<A> {
    let command = A::augment_args(clap::Command::new("workload").no_binary_name(true));
    let matches = command
        .try_get_matches_from(argv.split_whitespace())
        .with_context(|| format!("parse workload args `{argv}`"))?;
    A::from_arg_matches(&matches).with_context(|| format!("workload args `{argv}`"))
}

/// A labelled workload in a suite.
pub struct Job {
    pub label: &'static str,
    pub workload: Workload,
}

fn job(label: &'static str, workload: Workload) -> Job {
    Job { label, workload }
}

/// Reduced-scale per-PR suite. Every run uses distinct stream names, so
/// counts and snapshot sizes are deterministic.
pub fn pr_suite() -> Result<Vec<Job>> {
    Ok(vec![
        job(
            "w1",
            Workload::W1(parse::<W1Args>(
                "--records=300000 --checkpoints=75000,300000 --forced-flush --name=w1_inline",
            )?),
        ),
        job(
            "w1_hot",
            Workload::W1(parse::<W1Args>(
                "--records=300000 --checkpoints=75000,300000 --name=w1_hot",
            )?),
        ),
        job(
            "w6_w1",
            Workload::W1(parse::<W1Args>(
                "--records=200000 --retain-every=10000 --retain-keep=2000 --checkpoints=100000,200000",
            )?),
        ),
        job("w3_r1", Workload::W3(parse::<W3Args>("--appends=1000")?)),
        job(
            "w3_r5000",
            Workload::W3(parse::<W3Args>("--appends=100 --recs-per-append=5000")?),
        ),
        job("w4_p1", Workload::W4(parse::<W4Args>("--appends=100000")?)),
        job(
            "w4_p10000",
            Workload::W4(parse::<W4Args>("--appends=20000 --producers=10000")?),
        ),
        job(
            "w4_epoch",
            Workload::W4(parse::<W4Args>("--appends=100000 --epoch-every=10000")?),
        ),
        job(
            "w5_delete",
            Workload::W5(parse::<W5Args>("--mode=delete --streams=2000 --cycles=3")?),
        ),
        job(
            "w5_ttl_expire",
            Workload::W5(parse::<W5Args>("--mode=ttl-expire --streams=5000")?),
        ),
        job(
            "w5_ttl_heap",
            Workload::W5(parse::<W5Args>("--mode=ttl-heap --appends=100000")?),
        ),
        job(
            "w5_purge",
            Workload::W5(parse::<W5Args>("--mode=purge --buckets=5000")?),
        ),
        job(
            "w2_starve",
            Workload::W2(parse::<W2Args>(
                "--streams=50 --rate=1 --rec-bytes=2000 --hours=2.5 --measure-every-h=0.5 --name=w2_starve",
            )?),
        ),
        job(
            "w2_driver",
            Workload::W2(parse::<W2Args>(
                "--streams=50 --rate=1 --rec-bytes=2000 --hours=2.5 --measure-every-h=0.5 --driver --name=w2_driver",
            )?),
        ),
        job(
            "planner",
            Workload::Planner(parse::<PlannerArgs>("--streams=100,1000")?),
        ),
        job(
            "l2_w1_memory",
            Workload::L2(parse::<L2Args>(
                "--mode=w1 --engine=memory --records=60000",
            )?),
        ),
        job(
            "l2_w1_raft",
            Workload::L2(parse::<L2Args>("--mode=w1 --engine=raft --records=60000")?),
        ),
        job(
            "l2_compact",
            Workload::L2(parse::<L2Args>("--mode=compact")?),
        ),
        job(
            "cadence_w1",
            Workload::Cadence(parse::<CadenceArgs>(
                "--groups=1 --node-groups=128 --appends=300000 --name=cadence_w1",
            )?),
        ),
        job(
            "cadence_g128",
            Workload::Cadence(parse::<CadenceArgs>(
                "--groups=128 --streams-per-group=4 --appends=1000000 --budget-mib=128 --flush-kib=256 --name=cadence_g128",
            )?),
        ),
    ])
}

/// Full-scale nightly suite: the audit's measurement set (§7.3).
pub fn nightly_suite() -> Result<Vec<Job>> {
    Ok(vec![
        job(
            "w1",
            Workload::W1(parse::<W1Args>("--records=3000000 --zstd --restore")?),
        ),
        job(
            "w1_hot",
            Workload::W1(parse::<W1Args>(
                "--records=3000000 --restore --name=w1_hot",
            )?),
        ),
        job(
            "w1_batch10",
            Workload::W1(parse::<W1Args>(
                "--records=1000000 --recs-per-append=10 --name=w1_inline_batch10",
            )?),
        ),
        job(
            "l2_w1_memory",
            Workload::L2(parse::<L2Args>(
                "--mode=w1 --engine=memory --records=1000000",
            )?),
        ),
        job(
            "l2_w1_raft",
            Workload::L2(parse::<L2Args>(
                "--mode=w1 --engine=raft --records=1000000",
            )?),
        ),
        job("w2", Workload::W2(parse::<W2Args>("--hours=24 --zstd")?)),
        job(
            "w2_maxflush16",
            Workload::W2(parse::<W2Args>(
                "--hours=24 --max-flush-mib=16 --compact --name=w2_packs_maxflush16",
            )?),
        ),
        job(
            "w2_driver",
            Workload::W2(parse::<W2Args>(
                "--hours=24 --driver --name=w2_packs_driver",
            )?),
        ),
        job(
            "w2_pressure85",
            Workload::W2(parse::<W2Args>("--hours=24 --pressure-groups=85")?),
        ),
        job(
            "w2_starve",
            Workload::W2(parse::<W2Args>(
                "--streams=50 --rate=1 --rec-bytes=2000 --hours=2 --measure-every-h=0.25 --name=w2_starve",
            )?),
        ),
        job(
            "l2_w2_memory",
            Workload::L2(parse::<L2Args>("--mode=w2 --engine=memory --minutes=120")?),
        ),
        job(
            "l2_w2_raft",
            Workload::L2(parse::<L2Args>("--mode=w2 --engine=raft --minutes=120")?),
        ),
        job(
            "l2_compact",
            Workload::L2(parse::<L2Args>("--mode=compact")?),
        ),
        job("planner", Workload::Planner(parse::<PlannerArgs>("")?)),
        job("w3_r1", Workload::W3(parse::<W3Args>("--appends=100000")?)),
        job(
            "w3_r5000",
            Workload::W3(parse::<W3Args>("--appends=1000 --recs-per-append=5000")?),
        ),
        job(
            "w3_inline100",
            Workload::W3(parse::<W3Args>("--appends=10000 --inline-every=100")?),
        ),
        job(
            "w4_p1",
            Workload::W4(parse::<W4Args>("--appends=1000000 --dedup-timing")?),
        ),
        job(
            "w4_epoch",
            Workload::W4(parse::<W4Args>("--appends=1000000 --epoch-every=10000")?),
        ),
        job(
            "w4_p100000",
            Workload::W4(parse::<W4Args>("--appends=1000000 --producers=100000")?),
        ),
        job(
            "w5_delete",
            Workload::W5(parse::<W5Args>(
                "--mode=delete --streams=10000 --cycles=10",
            )?),
        ),
        job(
            "w5_ttl_expire",
            Workload::W5(parse::<W5Args>("--mode=ttl-expire --streams=10000")?),
        ),
        job(
            "w5_ttl_heap",
            Workload::W5(parse::<W5Args>("--mode=ttl-heap --appends=1000000")?),
        ),
        job(
            "cadence_w1",
            Workload::Cadence(parse::<CadenceArgs>(
                "--groups=1 --node-groups=128 --appends=3000000 --name=cadence_w1",
            )?),
        ),
        job(
            "cadence_g128",
            Workload::Cadence(parse::<CadenceArgs>(
                "--groups=128 --streams-per-group=4 --appends=20000000 --flush-kib=1024 --name=cadence_g128",
            )?),
        ),
        job(
            "w5_purge",
            Workload::W5(parse::<W5Args>("--mode=purge --buckets=100000")?),
        ),
        job(
            "w6_w1",
            Workload::W1(parse::<W1Args>(
                "--records=1000000 --retain-every=10000 --retain-keep=2000",
            )?),
        ),
        job(
            "w6_w2",
            Workload::W2(parse::<W2Args>(
                "--hours=24 --max-flush-mib=16 --retain-every-sec=600 --retain-keep=500",
            )?),
        ),
        job(
            "w6_w3_r1",
            Workload::W3(parse::<W3Args>(
                "--appends=10000 --retain-every=100 --retain-keep=100",
            )?),
        ),
        job(
            "w6_w3_r5000",
            Workload::W3(parse::<W3Args>(
                "--appends=1000 --recs-per-append=5000 --retain-every=10 --retain-keep=1000",
            )?),
        ),
        job(
            "w6_w4",
            Workload::W4(parse::<W4Args>("--appends=1000000 --retain-every=10000")?),
        ),
        job(
            "w6_w5_ttl_heap",
            Workload::W5(parse::<W5Args>(
                "--mode=ttl-heap --appends=1000000 --retain-every=10000 --name=w6_w5_ttl_heap_retention",
            )?),
        ),
    ])
}

/// Run every job, writing JSONL under `out_dir`; returns the merged,
/// label-prefixed outcome.
pub fn run_suite(jobs: &[Job], out_dir: &Path, echo: bool) -> Result<Outcome> {
    let mut merged = Outcome::default();
    for job in jobs {
        let started = std::time::Instant::now();
        eprintln!(
            "state-probe: running {} ({})",
            job.label,
            job.workload.name()
        );
        let outcome = job
            .workload
            .run(out_dir, echo)
            .with_context(|| format!("workload {}", job.label))?;
        eprintln!(
            "state-probe: {} done in {:.1}s",
            job.label,
            started.elapsed().as_secs_f64()
        );
        merged.merge(outcome.prefixed(job.label));
    }
    Ok(merged)
}

/// The checked-in ratchet: today's values and which targets are met today.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ratchet {
    #[serde(default = "default_tolerance")]
    pub tolerance: f64,
    pub metrics: BTreeMap<String, f64>,
    pub targets: BTreeMap<String, bool>,
}

fn default_tolerance() -> f64 {
    DEFAULT_TOLERANCE
}

impl Ratchet {
    pub fn from_outcome(outcome: &Outcome) -> Self {
        Self {
            tolerance: DEFAULT_TOLERANCE,
            metrics: outcome
                .metrics
                .iter()
                .map(|(name, value)| (name.clone(), (value * 1000.0).round() / 1000.0))
                .collect(),
            targets: outcome
                .checks
                .iter()
                .map(|check| (check.name.clone(), check.met))
                .collect(),
        }
    }

    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read ratchet {}", path.display()))?;
        serde_json::from_str(&text).with_context(|| format!("parse ratchet {}", path.display()))
    }
}

/// Severity of one gate finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    /// Fails the gate.
    Fail,
    /// An improvement: the ratchet can be tightened.
    Improved,
}

#[derive(Debug, Clone, Serialize)]
pub struct Finding {
    pub severity: Severity,
    pub name: String,
    pub message: String,
}

fn allowed_max(baseline: f64, tolerance: f64) -> f64 {
    baseline + baseline.abs() * tolerance
}

fn improved_below(baseline: f64, tolerance: f64) -> f64 {
    baseline - baseline.abs() * tolerance
}

/// Compare a suite outcome with the ratchet.
pub fn compare(outcome: &Outcome, ratchet: &Ratchet) -> Vec<Finding> {
    let tolerance = ratchet.tolerance;
    let mut findings = Vec::new();
    for (name, value) in &outcome.metrics {
        let Some(baseline) = ratchet.metrics.get(name) else {
            findings.push(Finding {
                severity: Severity::Fail,
                name: name.clone(),
                message: format!(
                    "metric {name} = {value} has no ratchet entry; regenerate the ratchet"
                ),
            });
            continue;
        };
        let max = allowed_max(*baseline, tolerance);
        if *value > max {
            findings.push(Finding {
                severity: Severity::Fail,
                name: name.clone(),
                message: format!(
                    "metric {name} regressed: {value} > {max:.3} (baseline {baseline} + {:.0}%)",
                    tolerance * 100.0
                ),
            });
        } else if *value < improved_below(*baseline, tolerance) {
            findings.push(Finding {
                severity: Severity::Improved,
                name: name.clone(),
                message: format!(
                    "metric {name} improved: {value} < baseline {baseline}; tighten the ratchet"
                ),
            });
        }
    }
    for name in ratchet.metrics.keys() {
        if !outcome.metrics.contains_key(name) {
            findings.push(Finding {
                severity: Severity::Fail,
                name: name.clone(),
                message: format!("ratcheted metric {name} was not produced"),
            });
        }
    }
    for check in &outcome.checks {
        match ratchet.targets.get(&check.name) {
            None => findings.push(Finding {
                severity: Severity::Fail,
                name: check.name.clone(),
                message: format!(
                    "check {} has no ratchet entry; regenerate the ratchet",
                    check.name
                ),
            }),
            Some(true) if !check.met => findings.push(Finding {
                severity: Severity::Fail,
                name: check.name.clone(),
                message: format!(
                    "target lost: {} ({}): {} > {}",
                    check.name, check.target, check.value, check.bound
                ),
            }),
            Some(false) if check.met => findings.push(Finding {
                severity: Severity::Improved,
                name: check.name.clone(),
                message: format!(
                    "target now met: {} ({}); set it to true in the ratchet",
                    check.name, check.target
                ),
            }),
            Some(_) => {}
        }
    }
    for name in ratchet.targets.keys() {
        if !outcome.checks.iter().any(|check| &check.name == name) {
            findings.push(Finding {
                severity: Severity::Fail,
                name: name.clone(),
                message: format!("ratcheted check {name} was not evaluated"),
            });
        }
    }
    findings
}

/// Write the outcome and findings to `<out_dir>/gate.json`, print them (as
/// GitHub annotations under Actions), and fail on any `Fail` finding.
pub fn report(outcome: &Outcome, findings: &[Finding], out_dir: &Path) -> Result<()> {
    let summary = serde_json::json!({
        "metrics": outcome.metrics,
        "checks": outcome.checks,
        "findings": findings,
    });
    std::fs::create_dir_all(out_dir)?;
    std::fs::write(
        out_dir.join("gate.json"),
        serde_json::to_string_pretty(&summary)?,
    )?;
    let github = std::env::var_os("GITHUB_ACTIONS").is_some();
    for check in &outcome.checks {
        let status = if check.met { "met" } else { "not met" };
        println!(
            "check {:<55} {:<8} value={} bound={}",
            check.name, status, check.value, check.bound
        );
    }
    for finding in findings {
        match (finding.severity, github) {
            (Severity::Fail, true) => println!("::error title=state-growth::{}", finding.message),
            (Severity::Improved, true) => {
                println!("::notice title=state-growth::{}", finding.message)
            }
            (Severity::Fail, false) => println!("FAIL {}", finding.message),
            (Severity::Improved, false) => println!("IMPROVED {}", finding.message),
        }
    }
    let failures = findings
        .iter()
        .filter(|f| f.severity == Severity::Fail)
        .count();
    if failures > 0 {
        bail!("state-growth gate: {failures} failure(s)");
    }
    println!(
        "state-growth gate: ok ({} metrics, {} checks)",
        outcome.metrics.len(),
        outcome.checks.len()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Ratchet;
    use super::Severity;
    use super::compare;
    use super::pr_suite;
    use crate::out::Outcome;

    fn outcome(value: f64, met: bool) -> Outcome {
        let mut outcome = Outcome::default();
        outcome.metric("w1.heap_bytes", value);
        outcome.check("w1.f8", "x", if met { 0.0 } else { 2.0 }, 1.0);
        outcome
    }

    #[test]
    fn unchanged_outcome_passes() {
        let ratchet = Ratchet::from_outcome(&outcome(100.0, false));
        assert!(compare(&outcome(100.0, false), &ratchet).is_empty());
        assert!(compare(&outcome(109.0, false), &ratchet).is_empty());
    }

    #[test]
    fn regression_beyond_tolerance_fails() {
        let ratchet = Ratchet::from_outcome(&outcome(100.0, false));
        let findings = compare(&outcome(111.0, false), &ratchet);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].severity, Severity::Fail);
    }

    #[test]
    fn improvements_are_reported_not_failed() {
        let ratchet = Ratchet::from_outcome(&outcome(100.0, false));
        let findings = compare(&outcome(50.0, true), &ratchet);
        assert_eq!(findings.len(), 2);
        assert!(findings.iter().all(|f| f.severity == Severity::Improved));
    }

    #[test]
    fn lost_target_and_missing_entries_fail() {
        let ratchet = Ratchet::from_outcome(&outcome(100.0, true));
        let findings = compare(&outcome(100.0, false), &ratchet);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].severity, Severity::Fail);

        let mut extra = outcome(100.0, true);
        extra.metric("w1.new_metric", 1.0);
        let findings = compare(&extra, &ratchet);
        assert!(
            findings
                .iter()
                .any(|f| f.severity == Severity::Fail && f.name == "w1.new_metric")
        );

        let findings = compare(&Outcome::default(), &ratchet);
        assert_eq!(
            findings
                .iter()
                .filter(|f| f.severity == Severity::Fail)
                .count(),
            2
        );
    }

    #[test]
    fn suites_parse() {
        assert!(!pr_suite().expect("pr suite").is_empty());
        assert!(!super::nightly_suite().expect("nightly suite").is_empty());
    }
}
