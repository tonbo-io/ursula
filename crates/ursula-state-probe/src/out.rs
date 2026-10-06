//! Result sink (JSON lines per workload), per-checkpoint measurement of a
//! state machine, and the [`Outcome`] each workload hands to the gate.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::Context;
use anyhow::Result;
use serde::Serialize;
use serde_json::Value;
use serde_json::json;
use ursula_runtime::StreamAppendCount;
use ursula_stream::GroupStateGauges;
use ursula_stream::StreamStateMachine;

use crate::alloc;
use crate::codec;
use crate::codec::SnapStats;

/// Writes one JSON object per line to `<out_dir>/<name>.jsonl`. Rows are not
/// kept in memory, so they do not perturb heap measurements.
pub struct Sink {
    file: BufWriter<File>,
    echo: bool,
    pub path: PathBuf,
}

impl Sink {
    pub fn new(out_dir: &Path, name: &str, echo: bool) -> Result<Self> {
        std::fs::create_dir_all(out_dir)
            .with_context(|| format!("create {}", out_dir.display()))?;
        let path = out_dir.join(format!("{name}.jsonl"));
        let file = File::create(&path).with_context(|| format!("create {}", path.display()))?;
        Ok(Self {
            file: BufWriter::new(file),
            echo,
            path,
        })
    }

    pub fn row(&mut self, value: &Value) -> Result<()> {
        let line = serde_json::to_string(value)?;
        if self.echo {
            println!("{line}");
        }
        writeln!(self.file, "{line}")?;
        self.file.flush()?;
        Ok(())
    }
}

/// Heap baseline taken before a workload builds its state machine.
pub struct Baseline {
    pub heap: alloc::Heap,
    pub big: Vec<usize>,
}

impl Baseline {
    pub fn now() -> Self {
        Self {
            heap: alloc::heap(),
            big: alloc::big_allocs(),
        }
    }
}

pub fn round3(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}

pub fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

/// One checkpoint's measurement of a state machine.
pub struct Measured {
    /// Requested live heap since the baseline.
    pub heap: alloc::Heap,
    /// Heap of a deep clone (no capacity slack).
    pub tight: alloc::Heap,
    /// Big allocations (at least 64 KiB) that appeared since the baseline.
    pub big: Vec<usize>,
    pub snap: SnapStats,
    pub gauges: GroupStateGauges,
    pub snapshot_clone_ms: f64,
}

impl Measured {
    pub fn slack_bytes(&self) -> i64 {
        self.heap.bytes.saturating_sub(self.tight.bytes)
    }

    pub fn to_json(&self) -> Value {
        json!({
            "heap_actual_bytes": self.heap.bytes,
            "heap_actual_blocks": self.heap.blocks,
            "heap_tight_bytes": self.tight.bytes,
            "heap_tight_blocks": self.tight.blocks,
            "heap_slack_bytes": self.slack_bytes(),
            "big_allocs": self.big.iter().take(12).collect::<Vec<_>>(),
            "sm_snapshot_clone_ms": round3(self.snapshot_clone_ms),
            "snapshot": self.snap,
            "gauges": self.gauges,
        })
    }
}

/// Heap (actual, tight via clone, slack), big-allocation census, the bounded
/// state gauges, and the real codec's snapshot size and breakdown.
pub fn measure_sm(
    m: &StreamStateMachine,
    base: &Baseline,
    counts: Vec<StreamAppendCount>,
    zstd: bool,
) -> Result<Measured> {
    let heap = alloc::heap().saturating_sub(base.heap);
    let big = alloc::big_allocs_diff(&base.big, &alloc::big_allocs());
    let tight = alloc::tight_size(m);
    let gauges = m.state_gauges();
    let started = Instant::now();
    let stream_snapshot = m.snapshot();
    let snapshot_clone_ms = started.elapsed().as_secs_f64() * 1e3;
    let snap = codec::measure(codec::group_snapshot(stream_snapshot, counts, 0), zstd)?;
    Ok(Measured {
        heap,
        tight,
        big,
        snap,
        gauges,
        snapshot_clone_ms,
    })
}

/// One §7.2 assertion evaluated against its target formula. `met` is whether
/// today's code already meets the target; the gate compares it with the
/// ratchet's expectation.
#[derive(Debug, Clone, Serialize)]
pub struct Check {
    pub name: String,
    pub target: String,
    pub met: bool,
    /// Worst observed value (or margin) across checkpoints.
    pub value: f64,
    /// The bound the target formula allows at that point.
    pub bound: f64,
}

/// What a workload hands to the gate: deterministic scalar metrics (ratcheted)
/// and formula checks (targets).
#[derive(Debug, Default, Clone, Serialize)]
pub struct Outcome {
    pub metrics: BTreeMap<String, f64>,
    pub checks: Vec<Check>,
}

impl Outcome {
    pub fn metric(&mut self, name: &str, value: impl Into<f64>) {
        self.metrics.insert(name.to_owned(), value.into());
    }

    pub fn metric_u64(&mut self, name: &str, value: u64) {
        self.metrics.insert(name.to_owned(), value as f64);
    }

    pub fn metric_i64(&mut self, name: &str, value: i64) {
        self.metrics.insert(name.to_owned(), value as f64);
    }

    /// Record a check; repeated calls under one name keep the worst result
    /// (unmet beats met; otherwise the larger `value - bound`).
    pub fn check(&mut self, name: &str, target: &str, value: f64, bound: f64) {
        let met = value <= bound;
        if let Some(existing) = self.checks.iter_mut().find(|c| c.name == name) {
            let worse = (!met && existing.met)
                || (met == existing.met && value - bound > existing.value - existing.bound);
            if worse {
                existing.met = met;
                existing.value = value;
                existing.bound = bound;
            }
            return;
        }
        self.checks.push(Check {
            name: name.to_owned(),
            target: target.to_owned(),
            met,
            value,
            bound,
        });
    }

    /// Prefix every metric and check name with `prefix.`.
    pub fn prefixed(self, prefix: &str) -> Outcome {
        Outcome {
            metrics: self
                .metrics
                .into_iter()
                .map(|(name, value)| (format!("{prefix}.{name}"), value))
                .collect(),
            checks: self
                .checks
                .into_iter()
                .map(|check| Check {
                    name: format!("{prefix}.{}", check.name),
                    ..check
                })
                .collect(),
        }
    }

    pub fn merge(&mut self, other: Outcome) {
        self.metrics.extend(other.metrics);
        self.checks.extend(other.checks);
    }
}

#[cfg(test)]
mod tests {
    use super::Outcome;

    #[test]
    #[expect(
        clippy::float_cmp,
        reason = "the check stores an exact integer-valued metric"
    )]
    fn repeated_checks_keep_the_worst_result() {
        let mut outcome = Outcome::default();
        outcome.check("c", "x <= 10", 3.0, 10.0);
        outcome.check("c", "x <= 10", 12.0, 10.0);
        outcome.check("c", "x <= 10", 5.0, 10.0);
        assert_eq!(outcome.checks.len(), 1);
        let check = &outcome.checks[0];
        assert!(!check.met);
        assert_eq!(check.value, 12.0);
    }
}
