use std::time::Duration;

use ursula_raft::LeadershipShedReason;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_runtime::ShardRuntime;

use crate::bootstrap::health_gate::CounterGrowth;
use crate::bootstrap::health_gate::HealthHysteresis;
use crate::bootstrap::health_gate::HealthTransition;
use crate::bootstrap::health_gate::LeadershipShed;
use crate::bootstrap::health_gate::ShedMark;
use crate::bootstrap::health_gate::ShedPace;
use crate::bootstrap::health_gate::TickHealth;
use crate::bootstrap::participation::PeerParticipation;

/// M4 — cold-health leadership gate. Two signals declare this node
/// "cold-impaired":
///
/// 1. `cold_flush_write_errors` climbed since last tick — S3 PUT or its
///    network path is failing under load.
/// 2. `cold_hot_group_bytes_max` stayed ≥ HOT_HIGH bytes — the per-group
///    hot tier isn't draining back, so we're queued up against the cap
///    and writes will reactively backpressure at the cliff.
///
/// Either condition sustained for `unhealthy_ticks` ticks triggers a soft
/// shed: the node hands off the groups it leads, a few per tick and the
/// hottest first, each to a peer whose replica of the group may take its
/// leadership, but it stays campaign-eligible.
/// Cold-health is often cluster-wide during backlog; disabling elections on
/// every hot peer can freeze leadership movement. The node heals when error
/// growth stops AND hot_max drops below HOT_LOW for `heal_ticks` ticks.
///
/// Why this matters: ColdBackpressure 503 is a per-write reactive gate —
/// once hot pegs at cap, the leader keeps accepting writes that immediately
/// 503, and the cold queue can only drain at the per-publish raft RTT.
/// Shedding leadership routes new client writes to a peer instead, giving
/// this node's cold queue an actual catch-up window. That's the gap that
/// dominated the long-running chaos test — many `s3_unavailable` injections
/// on the same node drove cumulative deficit because writes kept arriving.
/// Per-tick input for the M4 tracker. Both fields are cumulative counters
/// straight from `RuntimeMetricsSnapshot`; the tracker handles the delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ColdHealthSample {
    pub cold_flush_write_errors: u64,
    pub cold_hot_group_bytes_max: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ColdHealthDecision {
    NoChange,
    Shed { reason: String },
    Heal,
}

/// Pure decision core for the M4 cold-health gate, split from the spawn so
/// tests drive it with synthetic samples instead of a live runtime.
pub(crate) struct ColdHealthTracker {
    hysteresis: HealthHysteresis,
    flush_errors: CounterGrowth,
    hot_high_bytes: u64,
    hot_low_bytes: u64,
    errors_per_tick_high: u64,
}

impl ColdHealthTracker {
    pub fn new(
        unhealthy_ticks: usize,
        heal_ticks: usize,
        hot_high_bytes: u64,
        hot_low_bytes: u64,
        errors_per_tick_high: u64,
    ) -> Self {
        Self {
            hysteresis: HealthHysteresis::new(unhealthy_ticks, heal_ticks),
            flush_errors: CounterGrowth::default(),
            hot_high_bytes,
            hot_low_bytes,
            errors_per_tick_high,
        }
    }

    pub fn evaluate(&mut self, sample: ColdHealthSample) -> ColdHealthDecision {
        let delta_errors = self.flush_errors.advance(sample.cold_flush_write_errors);
        let errors = TickHealth::of_new_errors(delta_errors, self.errors_per_tick_high);
        let hot = if sample.cold_hot_group_bytes_max >= self.hot_high_bytes {
            TickHealth::Bad
        } else if sample.cold_hot_group_bytes_max <= self.hot_low_bytes {
            TickHealth::Good
        } else {
            TickHealth::Inconclusive
        };

        match self.hysteresis.observe(errors.max(hot)) {
            HealthTransition::Shed => {
                let reason = if errors == TickHealth::Bad {
                    format!(
                        "cold_flush_write_errors +{delta_errors}/tick > {}",
                        self.errors_per_tick_high
                    )
                } else {
                    format!(
                        "cold_hot_max {} ≥ HIGH {}",
                        sample.cold_hot_group_bytes_max, self.hot_high_bytes
                    )
                };
                ColdHealthDecision::Shed { reason }
            }
            HealthTransition::Heal => ColdHealthDecision::Heal,
            HealthTransition::Hold => ColdHealthDecision::NoChange,
        }
    }

    #[cfg(test)]
    pub fn yielded(&self) -> bool {
        self.hysteresis.is_shedding()
    }
}

/// Config-driven cold health gate.
pub fn spawn_cold_health_gate(
    runtime: &ShardRuntime,
    registry: &RaftGroupHandleRegistry,
    participation: &PeerParticipation,
    ch_cfg: &ursula_config::ColdHealthConfig,
) {
    let interval_ms = ch_cfg.interval.as_duration().as_millis() as usize;
    if interval_ms == 0 {
        return;
    }
    let unhealthy_ticks = ch_cfg.unhealthy_ticks;
    let heal_ticks = ch_cfg.heal_ticks;
    let hot_high_bytes = ch_cfg.hot_size_high.as_bytes();
    let hot_low_bytes = ch_cfg.hot_size_low.as_bytes();
    let errors_per_tick_high = u64::try_from(ch_cfg.errors_per_tick_high).unwrap_or(1);
    let metrics = runtime.metrics();
    let registry = registry.clone();
    let participation = participation.clone();
    let node_id = participation.node_id();
    tokio::spawn(async move {
        let interval = Duration::from_millis(u64::try_from(interval_ms).unwrap_or(2_000));
        let mut tracker = ColdHealthTracker::new(
            unhealthy_ticks,
            heal_ticks,
            hot_high_bytes,
            hot_low_bytes,
            errors_per_tick_high,
        );
        let mut shed = LeadershipShed::new(
            &registry,
            LeadershipShedReason::ColdHealth,
            node_id,
            ShedPace::Gradual,
            ShedMark::UntilEnd,
        );
        loop {
            tokio::time::sleep(interval).await;
            let snap = metrics.snapshot();
            let sample = ColdHealthSample {
                cold_flush_write_errors: snap.cold_flush_write_errors,
                cold_hot_group_bytes_max: snap.cold_hot_group_bytes_max,
            };
            match tracker.evaluate(sample) {
                ColdHealthDecision::Shed { reason } => {
                    tracing::warn!(
                        "cold-health: node {node_id} cold-impaired ({reason}); yielding leadership"
                    );
                    shed.begin();
                }
                ColdHealthDecision::Heal => shed.end(),
                ColdHealthDecision::NoChange => {}
            }
            if shed.is_draining() {
                let targets = participation
                    .campaign_stances(&registry)
                    .await
                    .shed_targets();
                shed.step(&targets, &snap.per_group_cold_hot_bytes).await;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use proptest::prelude::ProptestConfig;
    use proptest::prelude::Strategy;
    use proptest::prelude::prop;
    use proptest::prop_assert_eq;
    use proptest::prop_oneof;
    use proptest::proptest;

    use super::ColdHealthDecision;
    use super::ColdHealthSample;
    use super::ColdHealthTracker;

    /// `ColdHealthTracker::evaluate` as it was before the gates shared one
    /// hysteresis: streak counters and a `yielded` flag.
    struct CountingTracker {
        unhealthy_ticks: usize,
        heal_ticks: usize,
        hot_high_bytes: u64,
        hot_low_bytes: u64,
        errors_per_tick_high: u64,
        last_errors: Option<u64>,
        consecutive_bad: usize,
        consecutive_good: usize,
        yielded: bool,
    }

    impl CountingTracker {
        fn new(
            unhealthy_ticks: usize,
            heal_ticks: usize,
            hot_high_bytes: u64,
            hot_low_bytes: u64,
            errors_per_tick_high: u64,
        ) -> Self {
            Self {
                unhealthy_ticks: unhealthy_ticks.max(1),
                heal_ticks: heal_ticks.max(1),
                hot_high_bytes,
                hot_low_bytes,
                errors_per_tick_high,
                last_errors: None,
                consecutive_bad: 0,
                consecutive_good: 0,
                yielded: false,
            }
        }

        fn evaluate(&mut self, sample: ColdHealthSample) -> ColdHealthDecision {
            let prev_errors = self.last_errors.unwrap_or(sample.cold_flush_write_errors);
            let delta_errors = sample.cold_flush_write_errors.saturating_sub(prev_errors);
            self.last_errors = Some(sample.cold_flush_write_errors);

            let errors_unhealthy = delta_errors > self.errors_per_tick_high;
            let hot_unhealthy = sample.cold_hot_group_bytes_max >= self.hot_high_bytes;
            let unhealthy = errors_unhealthy || hot_unhealthy;
            let healthy =
                delta_errors == 0 && sample.cold_hot_group_bytes_max <= self.hot_low_bytes;

            if unhealthy {
                self.consecutive_bad = self.consecutive_bad.saturating_add(1);
                self.consecutive_good = 0;
            } else if healthy {
                self.consecutive_good = self.consecutive_good.saturating_add(1);
                self.consecutive_bad = 0;
            } else {
                self.consecutive_good = 0;
            }

            if !self.yielded && self.consecutive_bad >= self.unhealthy_ticks {
                self.yielded = true;
                let reason = if errors_unhealthy {
                    format!(
                        "cold_flush_write_errors +{delta_errors}/tick > {}",
                        self.errors_per_tick_high
                    )
                } else {
                    format!(
                        "cold_hot_max {} ≥ HIGH {}",
                        sample.cold_hot_group_bytes_max, self.hot_high_bytes
                    )
                };
                return ColdHealthDecision::Shed { reason };
            }

            if self.yielded && self.consecutive_good >= self.heal_ticks {
                self.yielded = false;
                return ColdHealthDecision::Heal;
            }

            ColdHealthDecision::NoChange
        }
    }

    const HOT_LOW: u64 = 32;
    const HOT_HIGH: u64 = 48;
    const HOT_MAX: u64 = 96;

    /// Hot bytes below, inside or above the watermark band, with both
    /// watermarks themselves.
    fn hot_bytes() -> impl Strategy<Value = u64> {
        prop_oneof![0..=HOT_LOW, HOT_LOW..=HOT_HIGH, HOT_HIGH..=HOT_MAX]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2_048))]

        #[test]
        fn cold_health_decides_as_its_counting_predecessor(
            base_errors in 0u64..1_000,
            ticks in prop::collection::vec((0u64..4, hot_bytes()), 0..64),
            unhealthy_ticks in 0usize..5,
            heal_ticks in 0usize..5,
            errors_per_tick_high in 0u64..5,
        ) {
            let mut tracker = ColdHealthTracker::new(
                unhealthy_ticks,
                heal_ticks,
                HOT_HIGH,
                HOT_LOW,
                errors_per_tick_high,
            );
            let mut oracle = CountingTracker::new(
                unhealthy_ticks,
                heal_ticks,
                HOT_HIGH,
                HOT_LOW,
                errors_per_tick_high,
            );
            let mut errors = base_errors;
            for (new_errors, hot) in ticks {
                errors = errors.saturating_add(new_errors);
                let sample = ColdHealthSample {
                    cold_flush_write_errors: errors,
                    cold_hot_group_bytes_max: hot,
                };
                prop_assert_eq!(tracker.evaluate(sample), oracle.evaluate(sample));
                prop_assert_eq!(tracker.yielded(), oracle.yielded);
            }
        }
    }
}
