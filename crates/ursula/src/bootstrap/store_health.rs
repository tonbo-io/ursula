//! Snapshot-store health gate: every tick it probes the external snapshot
//! store and reads the node's cold-flush outcomes, and while the store stays
//! unhealthy it sheds the node's leadership.

use std::collections::VecDeque;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use ursula_raft::LeadershipShedReason;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_runtime::RuntimeMetricsSnapshot;
use ursula_runtime::ShardRuntime;
use ursula_runtime::SharedSnapshotStore;
use ursula_runtime::s3_operation_budget;

use crate::bootstrap::health_gate::CounterGrowth;
use crate::bootstrap::health_gate::HealthHysteresis;
use crate::bootstrap::health_gate::HealthTransition;
use crate::bootstrap::health_gate::LeadershipShed;
use crate::bootstrap::health_gate::ShedPace;
use crate::bootstrap::health_gate::TickHealth;
use crate::bootstrap::participation::PeerParticipation;

/// What one tick of the snapshot-store gate read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StoreHealthSample {
    /// Whether the store answered the health probe before its deadline.
    pub probe_answered: bool,
    /// The node's failed cold-flush writes so far. Cold flushes write to the
    /// same object store, and writes can fail while the probe answers: an
    /// idle pooled connection can answer it, and the store can refuse PUTs
    /// while it serves the probe's `stat`.
    pub cold_flush_write_errors: u64,
    /// The node's successful cold-flush writes so far.
    pub cold_flush_uploads: u64,
}

impl StoreHealthSample {
    /// The sample of a tick whose probe `probe_answered`, with the node's
    /// cold-flush counters from `metrics`.
    pub(crate) fn read(probe_answered: bool, metrics: &RuntimeMetricsSnapshot) -> Self {
        Self {
            probe_answered,
            cold_flush_write_errors: metrics.cold_flush_write_errors,
            cold_flush_uploads: metrics.cold_flush_uploads,
        }
    }
}

/// The failed and successful cold-flush writes one tick recorded.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct FlushOutcomes {
    pub errors: u64,
    pub uploads: u64,
}

/// What the flush window shows of the store's writes on one tick.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct FlushEvidence {
    /// Failed writes in the window.
    pub errors: u64,
    /// Successful writes since the window's latest failed write, counted from
    /// the start of the tick that recorded it. 0 when the window holds no
    /// failed write.
    pub uploads_since_failure: u64,
    /// This tick's own writes.
    pub tick: FlushOutcomes,
}

impl FlushEvidence {
    /// Bad when a write failed within the window and no write succeeded
    /// since. Otherwise the tick is judged by its own writes: inconclusive
    /// when it recorded a failed write (and so also a successful one), good
    /// when it recorded none. Successful writes from before a failure prove
    /// nothing about the store after it, and a failure that a later success
    /// answered weighs on its own tick only.
    pub(crate) fn health(self) -> TickHealth {
        if self.errors == 0 {
            TickHealth::Good
        } else if self.uploads_since_failure == 0 {
            TickHealth::Bad
        } else if self.tick.errors > 0 {
            TickHealth::Inconclusive
        } else {
            TickHealth::Good
        }
    }
}

/// How many ticks a failed cold-flush write counts for while no write
/// succeeds: enough to span the longest pause between two failed writes
/// while the store refuses every write, so no tick of such an outage looks
/// good. The flush worker records a failed write only once the S3 client gave
/// up on it, which can take [`s3_operation_budget`] (every attempt runs into
/// `s3.timeout`, and each backoff before one of the `s3.max_retries` retries
/// runs to its maximum), and then pauses `cold_flush_interval` before its next
/// pass. With the defaults that is 55 s, or 11 ticks of 5 s.
pub(crate) fn flush_window_ticks(
    s3: &ursula_config::S3Config,
    cold_flush_interval: Duration,
    tick: Duration,
) -> usize {
    let gap = cold_flush_interval.saturating_add(s3_operation_budget(
        s3.timeout.as_duration(),
        s3.max_retries,
    ));
    let ticks = gap.as_nanos().div_ceil(tick.as_nanos().max(1));
    usize::try_from(ticks).unwrap_or(usize::MAX).max(1)
}

/// The cold-flush outcomes of the latest ticks.
#[derive(Debug, Clone)]
struct FlushWindow {
    ticks: usize,
    recent: VecDeque<FlushOutcomes>,
    errors: CounterGrowth,
    uploads: CounterGrowth,
}

impl FlushWindow {
    fn new(ticks: usize) -> Self {
        Self {
            ticks: ticks.max(1),
            recent: VecDeque::new(),
            errors: CounterGrowth::default(),
            uploads: CounterGrowth::default(),
        }
    }

    /// Takes one tick's cumulative counters and returns the evidence of the
    /// window that ends with it.
    fn advance(&mut self, errors_total: u64, uploads_total: u64) -> FlushEvidence {
        if self.recent.len() >= self.ticks {
            self.recent.pop_front();
        }
        let tick = FlushOutcomes {
            errors: self.errors.advance(errors_total),
            uploads: self.uploads.advance(uploads_total),
        };
        self.recent.push_back(tick);
        let errors = self
            .recent
            .iter()
            .fold(0_u64, |errors, tick| errors.saturating_add(tick.errors));
        let uploads_since_failure =
            self.recent
                .iter()
                .rposition(|tick| tick.errors > 0)
                .map_or(0, |latest_failure| {
                    self.recent
                        .iter()
                        .skip(latest_failure)
                        .fold(0_u64, |uploads, tick| uploads.saturating_add(tick.uploads))
                });
        FlushEvidence {
            errors,
            uploads_since_failure,
            tick,
        }
    }
}

/// The snapshot-store gate's verdict on one tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StoreHealthTick {
    /// The worse of the probe's verdict and the flush window's.
    pub health: TickHealth,
    /// The cold-flush evidence of the window that ends with this tick.
    pub flush: FlushEvidence,
    pub transition: HealthTransition,
}

/// Decision core of the snapshot-store gate, free of I/O so tests drive it
/// with synthetic samples. A tick is bad when the probe failed, or when a
/// cold-flush write failed within the flush window and no write succeeded
/// since.
#[derive(Debug, Clone)]
pub(crate) struct StoreHealthTracker {
    hysteresis: HealthHysteresis,
    flush: FlushWindow,
}

impl StoreHealthTracker {
    pub(crate) fn new(
        unhealthy_ticks: usize,
        heal_ticks: usize,
        flush_window_ticks: usize,
    ) -> Self {
        Self {
            hysteresis: HealthHysteresis::new(unhealthy_ticks, heal_ticks),
            flush: FlushWindow::new(flush_window_ticks),
        }
    }

    pub(crate) fn evaluate(&mut self, sample: StoreHealthSample) -> StoreHealthTick {
        let flush = self
            .flush
            .advance(sample.cold_flush_write_errors, sample.cold_flush_uploads);
        let probe = if sample.probe_answered {
            TickHealth::Good
        } else {
            TickHealth::Bad
        };
        let health = probe.max(flush.health());
        StoreHealthTick {
            health,
            flush,
            transition: self.hysteresis.observe(health),
        }
    }
}

/// The snapshot-store gate's settings, resolved from `[storage.cold.s3]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StoreGateConfig {
    /// Time between ticks.
    pub interval: Duration,
    /// Deadline of one probe, retries of the S3 client included.
    pub probe_timeout: Duration,
    pub unhealthy_ticks: usize,
    pub heal_ticks: usize,
    /// See [`flush_window_ticks`].
    pub flush_window_ticks: usize,
}

impl StoreGateConfig {
    pub(crate) fn new(
        s3: &ursula_config::S3Config,
        cold_flush_interval: Duration,
        interval: Duration,
    ) -> Self {
        Self {
            interval,
            probe_timeout: s3.probe_timeout.as_duration(),
            unhealthy_ticks: s3.unhealthy_ticks,
            heal_ticks: s3.heal_ticks,
            flush_window_ticks: flush_window_ticks(s3, cold_flush_interval, interval),
        }
    }
}

/// The snapshot-store gate stopped, so nothing judges the store any more.
#[derive(Debug, thiserror::Error)]
#[error("the snapshot-store health gate stopped")]
pub(crate) struct StoreGateStopped;

/// A running snapshot-store gate: its task and the verdict of its latest
/// tick. Dropping it stops the gate, and a shed of the gate ends with it.
#[derive(Debug)]
pub(crate) struct StoreHealthGate {
    latest_bad: watch::Receiver<bool>,
    task: JoinHandle<()>,
}

impl StoreHealthGate {
    /// Whether the gate's latest tick judged the store unhealthy.
    pub(crate) fn latest_bad(&mut self) -> Result<bool, StoreGateStopped> {
        match self.latest_bad.has_changed() {
            Ok(_) => Ok(*self.latest_bad.borrow_and_update()),
            Err(_closed) => Err(StoreGateStopped),
        }
    }

    /// Waits for the verdict of the gate's next tick.
    #[cfg(test)]
    pub(crate) async fn next_verdict(&mut self) -> Result<bool, StoreGateStopped> {
        match self.latest_bad.changed().await {
            Ok(()) => Ok(*self.latest_bad.borrow_and_update()),
            Err(_closed) => Err(StoreGateStopped),
        }
    }
}

impl Drop for StoreHealthGate {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Spawns the snapshot-store gate. Every `config.interval` it probes the
/// store within `config.probe_timeout`, and reads the node's failed and
/// successful cold-flush writes. After `config.unhealthy_ticks` bad ticks
/// with no good tick between them the node stops campaigning and hands its
/// leaderships off a few per tick, hottest first, to the peers that may
/// campaign. After `config.heal_ticks` good ticks in a row it may lead again.
pub(crate) fn spawn_store_health_gate(
    runtime: &ShardRuntime,
    registry: &RaftGroupHandleRegistry,
    snapshot_store: SharedSnapshotStore,
    participation: PeerParticipation,
    config: StoreGateConfig,
) -> StoreHealthGate {
    let metrics = runtime.metrics();
    let registry = registry.clone();
    let (verdicts, latest_bad) = watch::channel(false);
    let task = tokio::spawn(async move {
        let node_id = participation.node_id();
        let mut tracker = StoreHealthTracker::new(
            config.unhealthy_ticks,
            config.heal_ticks,
            config.flush_window_ticks,
        );
        // The shed ends when the task does, however it stops.
        let mut shed = LeadershipShed::new(
            &registry,
            LeadershipShedReason::SnapshotDriverS3,
            node_id,
            ShedPace::Gradual,
        );
        loop {
            let tick_started = Instant::now();
            let probe_answered =
                match tokio::time::timeout(config.probe_timeout, snapshot_store.health_check())
                    .await
                {
                    Ok(Ok(())) => true,
                    Ok(Err(err)) => {
                        tracing::warn!(node_id, %err, "snapshot store health probe failed");
                        false
                    }
                    Err(_elapsed) => {
                        tracing::warn!(
                            node_id,
                            probe_timeout = ?config.probe_timeout,
                            "snapshot store health probe timed out"
                        );
                        false
                    }
                };
            let snap = metrics.snapshot();
            let tick = tracker.evaluate(StoreHealthSample::read(probe_answered, &snap));
            verdicts.send_replace(tick.health == TickHealth::Bad);
            match tick.transition {
                HealthTransition::Shed => {
                    tracing::warn!(
                        node_id,
                        unhealthy_ticks = config.unhealthy_ticks,
                        probe_answered,
                        flush_errors = tick.flush.errors,
                        flush_uploads_since_failure = tick.flush.uploads_since_failure,
                        flush_window_ticks = config.flush_window_ticks,
                        "snapshot store unhealthy; shedding leadership"
                    );
                    shed.begin();
                }
                HealthTransition::Heal => {
                    tracing::info!(node_id, "snapshot store healthy again");
                    shed.end();
                }
                HealthTransition::Hold => {}
            }
            if shed.is_draining() {
                let targets = participation
                    .campaign_stances(&registry)
                    .await
                    .shed_targets();
                shed.step(&targets, &snap.per_group_cold_hot_bytes).await;
            }
            tokio::time::sleep(config.interval.saturating_sub(tick_started.elapsed())).await;
        }
    });
    StoreHealthGate { latest_bad, task }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use axum::body::Bytes;
    use proptest::prelude::ProptestConfig;
    use proptest::prelude::prop;
    use proptest::prop_assert_eq;
    use proptest::proptest;
    use tokio::sync::watch;
    use ursula_raft::LeadershipShedReason;
    use ursula_raft::RaftGroupHandleRegistry;
    use ursula_raft::snapshot_cadence::SnapshotCadence;
    use ursula_runtime::AppendRequest;
    use ursula_runtime::ColdStore;
    use ursula_runtime::ColdStoreFaultEffect;
    use ursula_runtime::CreateStreamRequest;
    use ursula_runtime::InMemoryGroupEngineFactory;
    use ursula_runtime::PlanColdFlushRequest;
    use ursula_runtime::RuntimeConfig;
    use ursula_runtime::RuntimeError;
    use ursula_runtime::RuntimeThreading;
    use ursula_runtime::ShardRuntime;
    use ursula_runtime::SnapshotKey;
    use ursula_runtime::SnapshotLocation;
    use ursula_runtime::SnapshotStore;
    use ursula_runtime::SnapshotStoreError;
    use ursula_runtime::SnapshotStoreFuture;
    use ursula_shard::BucketStreamId;

    use super::FlushEvidence;
    use super::FlushOutcomes;
    use super::StoreGateConfig;
    use super::StoreHealthSample;
    use super::StoreHealthTracker;
    use super::flush_window_ticks;
    use super::spawn_store_health_gate;
    use crate::bootstrap::health_gate::HealthTransition;
    use crate::bootstrap::health_gate::TickHealth;
    use crate::bootstrap::participation::PeerParticipation;
    use crate::bootstrap::snapshot::StoreGateInputs;

    /// The snapshot driver's default interval with an external store.
    const TICK: Duration = Duration::from_secs(5);
    /// `storage.cold.flush_interval`'s default.
    const FLUSH_INTERVAL: Duration = Duration::from_secs(1);

    /// The gate of a node with the default `[storage.cold.s3]` settings:
    /// three bad ticks shed, two good ticks heal, and the flush window spans
    /// 11 ticks.
    fn default_tracker() -> StoreHealthTracker {
        let defaults = ursula_config::S3Config::default();
        StoreHealthTracker::new(
            defaults.unhealthy_ticks,
            defaults.heal_ticks,
            flush_window_ticks(&defaults, FLUSH_INTERVAL, TICK),
        )
    }

    fn sample(answered: bool, errors: u64, uploads: u64) -> StoreHealthSample {
        StoreHealthSample {
            probe_answered: answered,
            cold_flush_write_errors: errors,
            cold_flush_uploads: uploads,
        }
    }

    fn probe(answered: bool) -> StoreHealthSample {
        sample(answered, 0, 0)
    }

    fn run(
        tracker: &mut StoreHealthTracker,
        samples: &[StoreHealthSample],
    ) -> Vec<HealthTransition> {
        samples
            .iter()
            .map(|sample| tracker.evaluate(*sample).transition)
            .collect()
    }

    #[test]
    fn the_flush_window_spans_the_longest_pause_between_failed_writes() {
        let defaults = ursula_config::S3Config::default();
        // Four attempts of 10 s, backoffs of at most 2, 4 and 8 s, and the
        // flush worker's 1 s pause: 55 s.
        assert_eq!(flush_window_ticks(&defaults, FLUSH_INTERVAL, TICK), 11);
        assert_eq!(
            flush_window_ticks(&defaults, FLUSH_INTERVAL, Duration::from_secs(1)),
            55
        );
        assert_eq!(
            flush_window_ticks(&defaults, FLUSH_INTERVAL, Duration::from_secs(60)),
            1
        );
        let no_retries = ursula_config::S3Config {
            max_retries: 0,
            timeout: ursula_config::HumanDuration::sec(2),
            ..ursula_config::S3Config::default()
        };
        assert_eq!(flush_window_ticks(&no_retries, FLUSH_INTERVAL, TICK), 1);
    }

    #[test]
    fn flush_evidence_is_bad_only_while_no_write_succeeded_after_the_last_failure() {
        let evidence = |errors, uploads_since_failure, tick_errors, tick_uploads| FlushEvidence {
            errors,
            uploads_since_failure,
            tick: FlushOutcomes {
                errors: tick_errors,
                uploads: tick_uploads,
            },
        };
        assert_eq!(evidence(0, 0, 0, 0).health(), TickHealth::Good);
        assert_eq!(evidence(0, 0, 0, 9).health(), TickHealth::Good);
        assert_eq!(evidence(1, 0, 1, 0).health(), TickHealth::Bad);
        assert_eq!(
            evidence(1, 0, 0, 0).health(),
            TickHealth::Bad,
            "an earlier failure no write answered"
        );
        assert_eq!(
            evidence(3, 1, 2, 1).health(),
            TickHealth::Inconclusive,
            "this tick failed and succeeded"
        );
        assert_eq!(
            evidence(3, 4, 0, 1).health(),
            TickHealth::Good,
            "a write succeeded after the last failure, and this tick failed none"
        );
    }

    #[test]
    fn one_failed_probe_does_not_shed() {
        let mut tracker = default_tracker();
        let transitions = run(&mut tracker, &[
            probe(true),
            probe(false),
            probe(true),
            probe(false),
            probe(false),
            probe(true),
        ]);
        assert!(
            transitions.iter().all(|t| *t == HealthTransition::Hold),
            "{transitions:?}"
        );
    }

    #[test]
    fn an_idle_node_is_judged_by_the_probe_alone() {
        // No cold-flush write fails or succeeds: only the probe counts.
        let mut tracker = default_tracker();
        assert_eq!(
            run(&mut tracker, &[probe(false), probe(false), probe(false)]),
            vec![
                HealthTransition::Hold,
                HealthTransition::Hold,
                HealthTransition::Shed
            ]
        );
        assert_eq!(
            run(&mut tracker, &[
                probe(false),
                probe(true),
                probe(true),
                probe(true),
            ]),
            vec![
                HealthTransition::Hold,
                HealthTransition::Hold,
                HealthTransition::Heal,
                HealthTransition::Hold
            ]
        );
        for answered in [true, false] {
            let tick = tracker.evaluate(probe(answered));
            assert_eq!(tick.flush, FlushEvidence::default());
            assert_eq!(tick.health == TickHealth::Bad, !answered);
        }
    }

    /// Ticks of `TICK` from 0 s while a failed write is recorded every
    /// `every` from `every` on, and nothing uploads. Returns each tick's
    /// time, health and transition.
    fn failing_flushes(every: Duration, ticks: u32) -> Vec<(u64, TickHealth, HealthTransition)> {
        let mut tracker = default_tracker();
        (0..ticks)
            .map(|tick| {
                let now = TICK.saturating_mul(tick);
                let errors =
                    u64::try_from(now.as_nanos().checked_div(every.as_nanos()).unwrap()).unwrap();
                let verdict = tracker.evaluate(sample(true, errors.saturating_add(1_000), 7));
                (now.as_secs(), verdict.health, verdict.transition)
            })
            .collect()
    }

    #[test]
    fn failed_flush_writes_at_the_rate_the_flush_worker_records_them_shed() {
        // The S3 client retries a write 3 times, 7 to 14 s of backoff, and
        // the flush worker records one failure when it gives up: at most one
        // failure per 8 s, with failure-free ticks between, and the probe
        // answers throughout.
        for (every_secs, first_bad_secs) in [(8, 10), (14, 15)] {
            let ticks = failing_flushes(Duration::from_secs(every_secs), 24);
            let first_bad = ticks
                .iter()
                .position(|(_, health, _)| *health == TickHealth::Bad)
                .unwrap();
            assert_eq!(ticks[first_bad].0, first_bad_secs, "{ticks:?}");
            assert!(
                ticks[first_bad..]
                    .iter()
                    .all(|(_, health, _)| *health == TickHealth::Bad),
                "no tick of the outage looks good: {ticks:?}"
            );
            let sheds: Vec<_> = ticks
                .iter()
                .enumerate()
                .filter(|(_, (_, _, transition))| *transition == HealthTransition::Shed)
                .map(|(index, _)| index)
                .collect();
            assert_eq!(
                sheds,
                vec![first_bad.saturating_add(2)],
                "sheds on the third bad tick: {ticks:?}"
            );
        }
    }

    #[test]
    fn failed_flush_writes_that_hang_to_every_timeout_shed_and_stay_shed() {
        // Every attempt runs into the 10 s timeout: a failure every 54 s.
        let ticks = failing_flushes(Duration::from_secs(54), 48);
        let first_bad = ticks
            .iter()
            .position(|(_, health, _)| *health == TickHealth::Bad)
            .unwrap();
        assert_eq!(ticks[first_bad].0, 55);
        assert!(
            ticks[first_bad..]
                .iter()
                .all(|(_, health, _)| *health == TickHealth::Bad),
            "{ticks:?}"
        );
        let transitions: Vec<_> = ticks
            .iter()
            .map(|(_, _, transition)| *transition)
            .filter(|transition| *transition != HealthTransition::Hold)
            .collect();
        assert_eq!(transitions, vec![HealthTransition::Shed], "{ticks:?}");
    }

    #[test]
    fn failed_writes_never_shed_while_other_writes_succeed() {
        let mut tracker = default_tracker();
        let mut errors = 0_u64;
        let mut uploads = 0_u64;
        for tick in 0..200_u64 {
            // Every tick uploads, most ticks also record failures, and every
            // seventh probe fails.
            uploads = uploads.saturating_add(1);
            errors = errors.saturating_add(tick.checked_rem(3).unwrap());
            let answered = tick.checked_rem(7) != Some(0);
            let verdict = tracker.evaluate(sample(answered, errors, uploads));
            assert_eq!(verdict.health == TickHealth::Bad, !answered, "tick {tick}");
            assert_eq!(verdict.transition, HealthTransition::Hold, "tick {tick}");
        }
    }

    #[test]
    fn a_write_that_failed_beside_successful_ones_does_not_keep_later_ticks_from_being_good() {
        use TickHealth::Bad;
        use TickHealth::Good;
        use TickHealth::Inconclusive;

        // Ticks of 5 s from 0 s, with an upload on every tick: the probes at
        // 15 s and 20 s fail, one write fails at 25 s, and the probe at 55 s
        // fails. The failed write weighs on its own tick only, so the good
        // ticks after it end the bad streak.
        let mut tracker = default_tracker();
        let mut errors = 0_u64;
        let mut ticks = Vec::new();
        for (index, at_secs) in (0..=60_u64).step_by(5).enumerate() {
            if at_secs == 25 {
                errors = errors.saturating_add(1);
            }
            let answered = ![15, 20, 55].contains(&at_secs);
            let uploads = u64::try_from(index).unwrap();
            ticks.push(tracker.evaluate(sample(answered, errors, uploads)));
        }
        let health: Vec<_> = ticks.iter().map(|tick| tick.health).collect();
        assert_eq!(health, vec![
            Good,
            Good,
            Good,
            Bad,
            Bad,
            Inconclusive,
            Good,
            Good,
            Good,
            Good,
            Good,
            Bad,
            Good
        ]);
        assert!(
            ticks
                .iter()
                .all(|tick| tick.transition == HealthTransition::Hold),
            "{ticks:?}"
        );

        // A failed write every 50 s among uploads and a failed probe every
        // 10 minutes, the probe failing right after a failed write: no shed
        // in two hours.
        let mut tracker = default_tracker();
        let mut errors = 0_u64;
        for tick in 0..1_440_u64 {
            if tick.checked_rem(10) == Some(0) {
                errors = errors.saturating_add(1);
            }
            let answered = tick.checked_rem(120) != Some(1);
            let verdict = tracker.evaluate(sample(answered, errors, tick));
            assert_eq!(verdict.transition, HealthTransition::Hold, "tick {tick}");
        }
    }

    #[test]
    fn uploads_from_before_an_outage_do_not_hide_it() {
        // Ticks of 5 s from -60 s. Writes upload until 0 s, then every write
        // fails: the flush worker records a failure at 10 s and every 8 s
        // after, and the probe answers throughout.
        let mut tracker = default_tracker();
        let mut uploads = 0_u64;
        let mut ticks = Vec::new();
        for at_secs in (-60..=30_i64).step_by(5) {
            if at_secs <= 0 {
                uploads = uploads.saturating_add(1);
            }
            let errors = if at_secs < 10 {
                0
            } else {
                u64::try_from(at_secs.saturating_sub(10).checked_div(8).unwrap())
                    .unwrap()
                    .saturating_add(1)
            };
            let verdict = tracker.evaluate(sample(true, errors, uploads));
            ticks.push((at_secs, verdict.health, verdict.transition));
        }
        let first_bad = ticks
            .iter()
            .position(|(_, health, _)| *health == TickHealth::Bad)
            .unwrap();
        assert_eq!(ticks[first_bad].0, 10, "{ticks:?}");
        assert!(
            ticks[first_bad..]
                .iter()
                .all(|(_, health, _)| *health == TickHealth::Bad),
            "{ticks:?}"
        );
        let sheds: Vec<_> = ticks
            .iter()
            .filter(|(_, _, transition)| *transition == HealthTransition::Shed)
            .map(|(at_secs, _, _)| *at_secs)
            .collect();
        assert_eq!(sheds, vec![20], "{ticks:?}");

        // Nor does a write that succeeded after an earlier failure: at 0 s a
        // write fails beside a successful one, writes upload until 5 s, and
        // from then on every write fails, recorded at 15 s and every 8 s.
        let mut tracker = default_tracker();
        let mut ticks = Vec::new();
        for at_secs in (-5..=35_i64).step_by(5) {
            let uploads = if at_secs <= 5 {
                u64::try_from(at_secs.saturating_add(5)).unwrap()
            } else {
                10
            };
            let outage_errors = if at_secs < 15 {
                0
            } else {
                u64::try_from(at_secs.saturating_sub(15).checked_div(8).unwrap())
                    .unwrap()
                    .saturating_add(1)
            };
            let errors = u64::from(at_secs >= 0).saturating_add(outage_errors);
            let verdict = tracker.evaluate(sample(true, errors, uploads));
            ticks.push((at_secs, verdict.health, verdict.transition));
        }
        let sheds: Vec<_> = ticks
            .iter()
            .filter(|(_, _, transition)| *transition == HealthTransition::Shed)
            .map(|(at_secs, _, _)| *at_secs)
            .collect();
        assert_eq!(sheds, vec![25], "{ticks:?}");
    }

    /// A node that sheds after failed writes: one failure at its second tick
    /// and one at its fourth, nothing uploading, and the probe answering.
    fn shed_by_failed_writes() -> StoreHealthTracker {
        let mut tracker = default_tracker();
        let mut transitions = run(&mut tracker, &[
            sample(true, 0, 0),
            sample(true, 1, 0),
            sample(true, 1, 0),
            sample(true, 2, 0),
        ]);
        assert_eq!(transitions.pop(), Some(HealthTransition::Shed));
        tracker
    }

    #[test]
    fn the_node_heals_two_good_ticks_after_a_write_succeeds_again() {
        let mut tracker = shed_by_failed_writes();
        let healing =
            [sample(true, 2, 1), sample(true, 2, 1)].map(|sample| tracker.evaluate(sample));
        assert_eq!(healing[0].health, TickHealth::Good);
        assert_eq!(healing[0].flush.uploads_since_failure, 1);
        assert_eq!(healing[0].transition, HealthTransition::Hold);
        assert_eq!(healing[1].transition, HealthTransition::Heal);
    }

    #[test]
    fn a_node_that_writes_nothing_heals_once_its_last_failed_write_leaves_the_window() {
        // A node that handed off every leadership flushes nothing, so no
        // successful write can answer its last failure.
        let mut tracker = shed_by_failed_writes();
        // The last failure stays in the window for 11 ticks, then two good
        // ticks heal.
        let after: Vec<_> = (0..13)
            .map(|_| tracker.evaluate(sample(true, 2, 0)))
            .collect();
        assert!(
            after[..10]
                .iter()
                .all(|tick| tick.health == TickHealth::Bad),
            "{after:?}"
        );
        assert_eq!(after[10].health, TickHealth::Good);
        assert_eq!(after[10].transition, HealthTransition::Hold);
        assert_eq!(after[11].transition, HealthTransition::Heal);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// Against the gate's definition, written out over the tick history:
        /// a tick is bad when the probe went unanswered, or when the last
        /// `window` ticks hold a failed write and no write succeeded from the
        /// tick of the latest one on; it is good when the probe answered and
        /// the flush part is not bad and this tick recorded no failed write.
        /// The node sheds once the ticks since the last good one hold
        /// `unhealthy_ticks` bad ones.
        #[test]
        fn sheds_only_after_unhealthy_ticks_bad_ticks(
            base in 0u64..1_000,
            ticks in prop::collection::vec(
                (prop::bool::weighted(0.8), 0u64..3, prop::bool::weighted(0.3)),
                1..64,
            ),
            unhealthy_ticks in 1usize..5,
            window in 1usize..6,
        ) {
            let mut tracker = StoreHealthTracker::new(unhealthy_ticks, 2, window);
            let mut errors = base;
            let mut uploads = base;
            let mut history: Vec<(u64, u64)> = Vec::new();
            let mut bad_since_good = 0_usize;
            let mut shedding = false;
            for (index, (answered, new_errors, uploaded)) in ticks.into_iter().enumerate() {
                errors = errors.saturating_add(new_errors);
                uploads = uploads.saturating_add(u64::from(uploaded));
                let tick = tracker.evaluate(sample(answered, errors, uploads));
                // The first tick is the counters' baseline.
                let this_tick = if index == 0 {
                    (0, 0)
                } else {
                    (new_errors, u64::from(uploaded))
                };
                history.push(this_tick);
                let recent = &history[history.len().saturating_sub(window)..];
                let window_errors: u64 = recent.iter().map(|(errors, _)| errors).sum();
                let uploads_since_failure: u64 = recent
                    .iter()
                    .rposition(|(errors, _)| *errors > 0)
                    .map_or(0, |latest| recent[latest..].iter().map(|(_, uploads)| uploads).sum());
                prop_assert_eq!(tick.flush, FlushEvidence {
                    errors: window_errors,
                    uploads_since_failure,
                    tick: FlushOutcomes {
                        errors: this_tick.0,
                        uploads: this_tick.1,
                    },
                });
                let flush_bad = window_errors > 0 && uploads_since_failure == 0;
                let bad = !answered || flush_bad;
                let good = answered && !flush_bad && this_tick.0 == 0;
                prop_assert_eq!(tick.health == TickHealth::Bad, bad);
                prop_assert_eq!(tick.health == TickHealth::Good, good);
                if shedding {
                    if tick.transition == HealthTransition::Heal {
                        shedding = false;
                        bad_since_good = 0;
                    }
                    continue;
                }
                if bad {
                    bad_since_good = bad_since_good.saturating_add(1);
                } else if good {
                    bad_since_good = 0;
                }
                let sheds = bad_since_good >= unhealthy_ticks;
                prop_assert_eq!(tick.transition == HealthTransition::Shed, sheds);
                shedding = sheds;
            }
        }
    }

    fn shedding_for_the_store(registry: &RaftGroupHandleRegistry) -> bool {
        registry
            .leadership_shed_state()
            .contains(LeadershipShedReason::SnapshotDriverS3.into())
    }

    /// Whether a scripted cold-flush write fails or uploads.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Flush {
        Fails,
        Uploads,
    }

    /// A runtime with one group and a memory cold store on the test's own
    /// tokio runtime, and a stream it writes to the cold store on demand.
    #[derive(Debug, Clone)]
    struct ColdFlusher {
        runtime: ShardRuntime,
        cold_store: Arc<ColdStore>,
        stream: BucketStreamId,
    }

    impl ColdFlusher {
        async fn new() -> Self {
            let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
            let mut config = RuntimeConfig::new(1, 1);
            config.threading = RuntimeThreading::HostedTokio;
            let runtime = ShardRuntime::spawn_with_engine_factory_and_cold_store(
                config,
                InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
                Some(cold_store.clone()),
            )
            .expect("runtime");
            let stream = BucketStreamId::new("benchcmp", "store-gate");
            runtime
                .create_stream(CreateStreamRequest::new(
                    stream.clone(),
                    "application/octet-stream",
                ))
                .await
                .expect("create the stream");
            Self {
                runtime,
                cold_store,
                stream,
            }
        }

        /// Appends a payload and writes it to the cold store through the
        /// runtime's flush path, with the store refusing the write or not.
        async fn attempt(&self, flush: Flush) {
            self.runtime
                .append(AppendRequest::from_bytes(
                    self.stream.clone(),
                    b"payload".to_vec(),
                ))
                .await
                .expect("append");
            match flush {
                Flush::Fails => self.cold_store.set_fault_policy(|_| {
                    Some(ColdStoreFaultEffect::fail("the store refuses writes"))
                }),
                Flush::Uploads => self.cold_store.clear_fault_policy(),
            }
            let flushed = self
                .runtime
                .flush_cold_once(PlanColdFlushRequest {
                    stream_id: self.stream.clone(),
                    min_hot_bytes: 1,
                    max_flush_bytes: 1 << 20,
                })
                .await;
            match flush {
                Flush::Fails => assert!(
                    matches!(flushed, Err(RuntimeError::ColdStoreIo { .. })),
                    "{flushed:?}"
                ),
                Flush::Uploads => assert!(matches!(flushed, Ok(Some(_))), "{flushed:?}"),
            }
        }
    }

    /// A snapshot store whose health probes follow a script. Each probe
    /// records whether the node was shedding for the snapshot store when the
    /// probe ran, publishes how many probes ran, attempts the cold-flush
    /// writes scripted for it on the node's runtime, and then answers.
    #[derive(Debug)]
    struct ScriptedProbeStore {
        registry: RaftGroupHandleRegistry,
        /// The node's runtime, whose cold-flush counters the gate reads.
        runtime: ShardRuntime,
        flusher: Option<ColdFlusher>,
        failing: BTreeSet<usize>,
        /// How long each probe takes, unless `slow` lists it.
        latency: Duration,
        slow: BTreeMap<usize, Duration>,
        flushes: BTreeMap<usize, Vec<Flush>>,
        shedding_at_probe: Mutex<Vec<bool>>,
        probes: watch::Sender<usize>,
    }

    impl ScriptedProbeStore {
        /// Probes of a node without a cold store.
        fn new(
            registry: &RaftGroupHandleRegistry,
            failing: impl IntoIterator<Item = usize>,
        ) -> Self {
            Self::with_runtime(registry, hosted_runtime(), None, failing)
        }

        /// Probes of the node `flusher` writes for.
        fn flushing(
            registry: &RaftGroupHandleRegistry,
            flusher: &ColdFlusher,
            flushes: impl IntoIterator<Item = (usize, Vec<Flush>)>,
        ) -> Self {
            let mut store =
                Self::with_runtime(registry, flusher.runtime.clone(), Some(flusher.clone()), []);
            store.flushes = flushes.into_iter().collect();
            store
        }

        fn with_runtime(
            registry: &RaftGroupHandleRegistry,
            runtime: ShardRuntime,
            flusher: Option<ColdFlusher>,
            failing: impl IntoIterator<Item = usize>,
        ) -> Self {
            Self {
                registry: registry.clone(),
                runtime,
                flusher,
                failing: failing.into_iter().collect(),
                latency: Duration::ZERO,
                slow: BTreeMap::new(),
                flushes: BTreeMap::new(),
                shedding_at_probe: Mutex::new(Vec::new()),
                probes: watch::channel(0).0,
            }
        }

        fn shedding_at_probe(&self) -> Vec<bool> {
            self.shedding_at_probe.lock().unwrap().clone()
        }
    }

    impl SnapshotStore for ScriptedProbeStore {
        fn upload<'a>(
            &'a self,
            _key: SnapshotKey,
            _bytes: Bytes,
        ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
            Box::pin(async {
                Err(SnapshotStoreError::Io(std::io::Error::other(
                    "probe-only store",
                )))
            })
        }

        fn download<'a>(
            &'a self,
            _location: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, Vec<u8>> {
            Box::pin(async {
                Err(SnapshotStoreError::Io(std::io::Error::other(
                    "probe-only store",
                )))
            })
        }

        fn delete<'a>(&'a self, _location: &'a SnapshotLocation) -> SnapshotStoreFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }

        fn health_check(&self) -> SnapshotStoreFuture<'_, ()> {
            let index = {
                let mut seen = self.shedding_at_probe.lock().unwrap();
                let index = seen.len();
                seen.push(shedding_for_the_store(&self.registry));
                index
            };
            self.probes.send_replace(index.saturating_add(1));
            let fails = self.failing.contains(&index);
            let latency = self.slow.get(&index).copied().unwrap_or(self.latency);
            let flushes = self.flushes.get(&index).cloned().unwrap_or_default();
            Box::pin(async move {
                for flush in flushes {
                    self.flusher
                        .as_ref()
                        .expect("a flushing store")
                        .attempt(flush)
                        .await;
                }
                tokio::time::sleep(latency).await;
                if fails {
                    Err(SnapshotStoreError::Io(std::io::Error::other(
                        "scripted probe failure",
                    )))
                } else {
                    Ok(())
                }
            })
        }
    }

    /// A runtime with one group on the test's own tokio runtime.
    fn hosted_runtime() -> ShardRuntime {
        let mut config = RuntimeConfig::new(1, 1);
        config.threading = RuntimeThreading::HostedTokio;
        ShardRuntime::spawn(config).expect("runtime")
    }

    fn no_peers() -> PeerParticipation {
        PeerParticipation::new(1, &[], reqwest::Client::new())
    }

    /// Runs the snapshot driver on `store`'s runtime, with `s3` as
    /// `[storage.cold.s3]` and `cold_flush_interval` as
    /// `storage.cold.flush_interval`, until it has probed `store` `probes`
    /// times.
    async fn drive_with(
        store: Arc<ScriptedProbeStore>,
        registry: &RaftGroupHandleRegistry,
        s3: Option<&ursula_config::S3Config>,
        cold_flush_interval: Duration,
        probes: usize,
    ) -> Vec<bool> {
        let participation = no_peers();
        crate::bootstrap::snapshot::spawn_snapshot_driver(
            &store.runtime,
            registry,
            Some((store.clone(), StoreGateInputs {
                s3,
                cold_flush_interval,
                participation: &participation,
            })),
            crate::bootstrap::snapshot::LogWriteGate::RefuseAtHardLimit,
            5_000,
            SnapshotCadence::new(1 << 30, 1, 100_000),
            None,
            16,
        );
        let mut probed = store.probes.subscribe();
        tokio::time::timeout(
            Duration::from_secs(600),
            probed.wait_for(|count| *count >= probes),
        )
        .await
        .expect("the gate probes the store")
        .expect("the store outlives the wait");
        store.shedding_at_probe()[..probes].to_vec()
    }

    /// [`drive_with`] the default `[storage.cold.s3]` settings.
    async fn drive(
        store: Arc<ScriptedProbeStore>,
        registry: &RaftGroupHandleRegistry,
        probes: usize,
    ) -> Vec<bool> {
        drive_with(store, registry, None, FLUSH_INTERVAL, probes).await
    }

    #[tokio::test(start_paused = true)]
    async fn the_driver_rides_out_a_single_failed_probe() {
        let registry = RaftGroupHandleRegistry::default();
        let store = Arc::new(ScriptedProbeStore::new(&registry, [1]));
        assert_eq!(drive(store, &registry, 8).await, vec![false; 8]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_probe_answered_within_the_deadline_is_healthy() {
        // A probe that answers 3 s in, as one the S3 client retried after a
        // fast failure, answers before the 5 s deadline.
        let registry = RaftGroupHandleRegistry::default();
        let mut store = ScriptedProbeStore::new(&registry, []);
        store.latency = Duration::from_secs(3);
        assert_eq!(drive(Arc::new(store), &registry, 6).await, vec![false; 6]);
    }

    #[tokio::test(start_paused = true)]
    async fn the_driver_sheds_when_probes_hang() {
        // Every probe outlasts the 5 s probe deadline (but not the S3 client's
        // 10 s timeout), so every tick is bad.
        let registry = RaftGroupHandleRegistry::default();
        let mut store = ScriptedProbeStore::new(&registry, []);
        store.latency = Duration::from_secs(6);
        assert_eq!(drive(Arc::new(store), &registry, 6).await, vec![
            false, false, false, true, true, true
        ]);
    }

    #[tokio::test(start_paused = true)]
    async fn the_driver_sheds_on_sustained_probe_failures_and_heals() {
        let registry = RaftGroupHandleRegistry::default();
        let store = Arc::new(ScriptedProbeStore::new(&registry, [1, 2, 3]));
        assert_eq!(drive(store, &registry, 8).await, vec![
            false, false, false, false, true, true, false, false
        ]);
        assert!(!shedding_for_the_store(&registry));
    }

    #[tokio::test(start_paused = true)]
    async fn the_driver_hands_its_s3_settings_to_the_gate() {
        // With these settings a probe that answers after 2 s misses its
        // deadline, one bad tick sheds and one good tick heals. The defaults
        // would take that probe as answered.
        let s3 = ursula_config::S3Config {
            probe_timeout: ursula_config::HumanDuration::sec(1),
            unhealthy_ticks: 1,
            heal_ticks: 1,
            ..ursula_config::S3Config::default()
        };
        let registry = RaftGroupHandleRegistry::default();
        let mut store = ScriptedProbeStore::new(&registry, []);
        store.slow.insert(1, Duration::from_secs(2));
        assert_eq!(
            drive_with(Arc::new(store), &registry, Some(&s3), FLUSH_INTERVAL, 5).await,
            vec![false, false, true, false, false]
        );
    }

    #[tokio::test]
    async fn a_sample_reads_the_flush_counters_of_a_runtime_snapshot() {
        let mut metrics = hosted_runtime().metrics().snapshot();
        metrics.cold_flush_write_errors = 3;
        metrics.cold_flush_uploads = 5;
        assert_eq!(
            StoreHealthSample::read(false, &metrics),
            StoreHealthSample {
                probe_answered: false,
                cold_flush_write_errors: 3,
                cold_flush_uploads: 5,
            }
        );
        assert!(StoreHealthSample::read(true, &metrics).probe_answered);
    }

    #[tokio::test(start_paused = true)]
    async fn the_driver_judges_the_runtimes_failed_and_successful_flush_writes() {
        // Every probe answers. A write the store refuses at the first tick
        // makes that tick and the two after it bad, so the node sheds, and a
        // write that succeeds at the fourth tick starts the good ticks that
        // heal it.
        let registry = RaftGroupHandleRegistry::default();
        let flusher = ColdFlusher::new().await;
        let store = Arc::new(ScriptedProbeStore::flushing(&registry, &flusher, [
            (1, vec![Flush::Fails]),
            (4, vec![Flush::Uploads]),
        ]));
        assert_eq!(drive(store, &registry, 7).await, vec![
            false, false, false, false, true, true, false
        ]);
        let metrics = flusher.runtime.metrics().snapshot();
        assert_eq!(
            (metrics.cold_flush_write_errors, metrics.cold_flush_uploads),
            (1, 1)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_driver_sizes_the_flush_window_from_its_settings() {
        // An S3 client that gives up on a write after one 1 s attempt, and a
        // flush worker that pauses 9 s between passes: a failed write counts
        // for 10 s, two ticks. So the write the store refuses at the first
        // tick makes two ticks bad, which sheds with these settings, and the
        // third tick heals. With the default S3 settings it would count for
        // eleven ticks, and without the flush worker's pause for one.
        let s3 = ursula_config::S3Config {
            timeout: ursula_config::HumanDuration::sec(1),
            max_retries: 0,
            unhealthy_ticks: 2,
            heal_ticks: 1,
            ..ursula_config::S3Config::default()
        };
        let registry = RaftGroupHandleRegistry::default();
        let flusher = ColdFlusher::new().await;
        let store = Arc::new(ScriptedProbeStore::flushing(&registry, &flusher, [(
            1,
            vec![Flush::Fails],
        )]));
        assert_eq!(
            drive_with(store, &registry, Some(&s3), Duration::from_secs(9), 6).await,
            vec![false, false, false, true, false, false]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_gate_publishes_each_ticks_verdict() {
        let registry = RaftGroupHandleRegistry::default();
        let flusher = ColdFlusher::new().await;
        // Tick 1 fails its probe. Tick 3 records a failed write beside a
        // successful one, which is inconclusive, not bad. Tick 4 records a
        // failed write and no successful one.
        let mut store = ScriptedProbeStore::flushing(&registry, &flusher, [
            (3, vec![Flush::Uploads, Flush::Fails]),
            (4, vec![Flush::Fails]),
        ]);
        store.failing.insert(1);
        let store = Arc::new(store);
        let mut gate = spawn_store_health_gate(
            &flusher.runtime,
            &registry,
            store.clone(),
            no_peers(),
            StoreGateConfig::new(&ursula_config::S3Config::default(), FLUSH_INTERVAL, TICK),
        );
        assert!(!gate.latest_bad().unwrap(), "no tick yet");
        let mut verdicts = Vec::new();
        for _ in 0..5 {
            verdicts.push(gate.next_verdict().await.unwrap());
            assert_eq!(gate.latest_bad().unwrap(), *verdicts.last().unwrap());
        }
        assert_eq!(verdicts, vec![false, true, false, false, true]);
        assert!(
            !shedding_for_the_store(&registry),
            "two bad ticks are no shed"
        );

        // A gate whose task stopped reports that it stopped, and leaves no
        // shed behind.
        registry.mark_leadership_shed(LeadershipShedReason::SnapshotDriverS3);
        gate.task.abort();
        tokio::task::yield_now().await;
        let mut waited = 0_u32;
        while gate.latest_bad().is_ok() {
            assert!(waited < 100, "the gate does not stop");
            waited = waited.saturating_add(1);
            tokio::task::yield_now().await;
        }
        assert!(!shedding_for_the_store(&registry));
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_the_gate_stops_it_and_ends_its_shed() {
        let registry = RaftGroupHandleRegistry::default();
        let store = Arc::new(ScriptedProbeStore::new(&registry, []));
        let mut gate = spawn_store_health_gate(
            &store.runtime,
            &registry,
            store.clone(),
            no_peers(),
            StoreGateConfig::new(&ursula_config::S3Config::default(), FLUSH_INTERVAL, TICK),
        );
        assert!(!gate.next_verdict().await.unwrap());
        registry.mark_leadership_shed(LeadershipShedReason::SnapshotDriverS3);
        drop(gate);
        let mut waited = 0_u32;
        while shedding_for_the_store(&registry) {
            assert!(waited < 100, "the dropped gate keeps running");
            waited = waited.saturating_add(1);
            tokio::task::yield_now().await;
        }
    }
}
