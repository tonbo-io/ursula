//! Shared core of the leadership health gates: the snapshot-store gate, the
//! cold-health gate and the cluster-egress gate. It holds the shed/heal
//! hysteresis, the planner that picks the target of each handoff, and the
//! drain that hands a shedding node's leaderships to its peers, a few per
//! tick or, for the egress gate, all at once.

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;

// `tokio::time::Instant` so the drain's deadlines follow tokio's clock, which
// tests pause and madsim simulates.
use tokio::time::Instant;
use ursula_control::Cluster;
use ursula_raft::LeadershipShedReason;
use ursula_raft::LeadershipTransferError;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_raft::RaftGroupMetricsSnapshot;
use ursula_shard::RaftGroupId;

use crate::bootstrap::util::handoff_targets;
use crate::bootstrap::util::leader_counts;

/// Most handoffs one gate submits per tick at [`ShedPace::Gradual`]. A node
/// that sheds moves its leaderships over several ticks, so its peers take
/// them over a few at a time rather than all at once (#508). The cap is per
/// gate: two gates that shed at once drain independently.
const SHED_HANDOFFS_PER_TICK: usize = 4;

/// How many handoffs a shed submits per step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShedPace {
    /// At most [`SHED_HANDOFFS_PER_TICK`] new handoffs per step.
    Gradual,
    /// Every pending leadership a peer can take over, at every step. For a
    /// node that cannot reach a quorum: it cannot replicate, so its
    /// leaderships stall where they are.
    AllAtOnce,
}

impl ShedPace {
    fn handoffs_per_step(self) -> usize {
        match self {
            Self::Gradual => SHED_HANDOFFS_PER_TICK,
            Self::AllAtOnce => usize::MAX,
        }
    }
}

/// How often a pending leadership that no peer can take over is reported
/// again.
const STRANDED_REPORT_INTERVAL: Duration = Duration::from_secs(60);

/// What one tick of a health gate observed. Ordered from healthy to failing,
/// so the verdict of several signals is the maximum of theirs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum TickHealth {
    /// Every signal is healthy.
    Good,
    /// No signal fails, but not every signal is healthy. The tick continues
    /// a bad streak without lengthening it, and restarts a good streak.
    Inconclusive,
    /// A signal fails.
    Bad,
    /// A signal fails that rules out serving by itself: the tick sheds at
    /// once, without the streak of `unhealthy_ticks` a bad tick waits for.
    Disqualifying,
}

impl TickHealth {
    /// Classifies the errors one tick added to a counter: none is good, up
    /// to `threshold` is inconclusive, and more is bad.
    pub(crate) fn of_new_errors(errors: u64, threshold: u64) -> Self {
        if errors == 0 {
            Self::Good
        } else if errors <= threshold {
            Self::Inconclusive
        } else {
            Self::Bad
        }
    }
}

/// How a tick changed a health gate's stance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HealthTransition {
    /// The stance did not change.
    Hold,
    /// The node starts shedding its leadership.
    Shed,
    /// The node stops shedding.
    Heal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stance {
    /// The node may lead. Counts the bad ticks since the last good tick.
    Serving { bad_ticks: usize },
    /// The node sheds. Counts the good ticks in a row.
    Shedding { good_ticks: usize },
}

/// The hysteresis of every leadership health gate: shed after
/// `unhealthy_ticks` bad ticks with no good tick between them, or on one
/// disqualifying tick, and heal after `heal_ticks` good ticks in a row. A
/// count of 0 acts as 1.
#[derive(Debug, Clone)]
pub(crate) struct HealthHysteresis {
    unhealthy_ticks: usize,
    heal_ticks: usize,
    stance: Stance,
}

impl HealthHysteresis {
    pub(crate) fn new(unhealthy_ticks: usize, heal_ticks: usize) -> Self {
        Self {
            unhealthy_ticks: unhealthy_ticks.max(1),
            heal_ticks: heal_ticks.max(1),
            stance: Stance::Serving { bad_ticks: 0 },
        }
    }

    pub(crate) fn observe(&mut self, tick: TickHealth) -> HealthTransition {
        let (stance, transition) = match (self.stance, tick) {
            (Stance::Serving { bad_ticks }, TickHealth::Bad) => {
                let bad_ticks = bad_ticks.saturating_add(1);
                if bad_ticks >= self.unhealthy_ticks {
                    (Stance::Shedding { good_ticks: 0 }, HealthTransition::Shed)
                } else {
                    (Stance::Serving { bad_ticks }, HealthTransition::Hold)
                }
            }
            (Stance::Serving { .. }, TickHealth::Disqualifying) => {
                (Stance::Shedding { good_ticks: 0 }, HealthTransition::Shed)
            }
            (Stance::Serving { bad_ticks }, TickHealth::Inconclusive) => {
                (Stance::Serving { bad_ticks }, HealthTransition::Hold)
            }
            (Stance::Serving { .. }, TickHealth::Good) => {
                (Stance::Serving { bad_ticks: 0 }, HealthTransition::Hold)
            }
            (Stance::Shedding { good_ticks }, TickHealth::Good) => {
                let good_ticks = good_ticks.saturating_add(1);
                if good_ticks >= self.heal_ticks {
                    (Stance::Serving { bad_ticks: 0 }, HealthTransition::Heal)
                } else {
                    (Stance::Shedding { good_ticks }, HealthTransition::Hold)
                }
            }
            (
                Stance::Shedding { .. },
                TickHealth::Inconclusive | TickHealth::Bad | TickHealth::Disqualifying,
            ) => (Stance::Shedding { good_ticks: 0 }, HealthTransition::Hold),
        };
        self.stance = stance;
        transition
    }

    #[cfg(test)]
    pub(crate) fn is_shedding(&self) -> bool {
        matches!(self.stance, Stance::Shedding { .. })
    }
}

/// What a cumulative counter gained between ticks. The first reading is the
/// baseline, so counts from before the gate started are not new.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct CounterGrowth {
    last: Option<u64>,
}

impl CounterGrowth {
    pub(crate) fn advance(&mut self, total: u64) -> u64 {
        let growth = self.last.map_or(0, |last| total.saturating_sub(last));
        self.last = Some(total);
        growth
    }
}

/// One planned handoff of a leadership the node sheds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ShedHandoff {
    pub group_id: u32,
    pub target: u64,
}

/// A leadership the planner may hand off.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ShedCandidate {
    pub group_id: u32,
    /// The target of an earlier handoff that did not move this leadership.
    /// The planner passes it over while another target qualifies.
    pub avoid: Option<u64>,
}

impl ShedCandidate {
    /// Every group `node_id` leads in `snaps`, by group id.
    #[cfg(test)]
    pub(crate) fn every_group_led_by(
        snaps: &[RaftGroupMetricsSnapshot],
        node_id: u64,
    ) -> Vec<Self> {
        let mut candidates: Vec<Self> = snaps
            .iter()
            .filter(|snap| snap.current_leader == Some(node_id))
            .map(|snap| Self {
                group_id: snap.raft_group_id,
                avoid: None,
            })
            .collect();
        candidates.sort_by_key(|candidate| candidate.group_id);
        candidates
    }
}

/// The nodes a shed may hand a leadership to on one tick: the ones that
/// answered that they may campaign
/// ([`crate::bootstrap::participation::CampaignStances::shed_targets`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ShedTargets {
    nodes: HashSet<u64>,
}

impl ShedTargets {
    pub(crate) fn new(nodes: HashSet<u64>) -> Self {
        Self { nodes }
    }

    /// Whether `node` may take a handoff.
    fn admit(&self, node: u64) -> bool {
        self.nodes.contains(&node)
    }
}

/// A tick's handoffs, and the candidates that no peer could take over.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct ShedPlan {
    pub handoffs: Vec<ShedHandoff>,
    /// Candidates the planner reached that had no qualifying target.
    pub stranded: Vec<u32>,
}

/// Plans at most `limit` handoffs of `candidates`, the groups `node_id`
/// leads that it may hand off, in the order given. A target must be one of
/// the group's [`handoff_targets`] under the bootstrapped view `cluster`
/// that `targets` admits for the candidate and that `can_take_over` accepts
/// (the registry's handoff check). Of those, each
/// handoff goes to the one that leads the fewest groups, counting the
/// `in_flight` handoffs (submitted, with the leadership not moved yet) and
/// the ones planned before it, so the handoffs spread over the peers. A
/// candidate's `avoid` target is passed over while another target qualifies.
/// A candidate without a target is stranded, and the candidates after it are
/// still planned.
pub(crate) fn plan_shed_handoffs(
    snaps: &[RaftGroupMetricsSnapshot],
    node_id: u64,
    cluster: Option<&Cluster>,
    candidates: impl IntoIterator<Item = ShedCandidate>,
    in_flight: &[ShedHandoff],
    limit: usize,
    targets: &ShedTargets,
    mut can_take_over: impl FnMut(RaftGroupId, u64) -> bool,
) -> ShedPlan {
    let by_group: HashMap<u32, &RaftGroupMetricsSnapshot> = snaps
        .iter()
        .map(|snap| (snap.raft_group_id, snap))
        .collect();
    let mut load = leader_counts(snaps);
    for handoff in in_flight {
        add_load(&mut load, handoff.target);
    }

    let mut plan = ShedPlan::default();
    for candidate in candidates {
        if plan.handoffs.len() >= limit {
            break;
        }
        let Some(snap) = by_group
            .get(&candidate.group_id)
            .filter(|snap| snap.current_leader == Some(node_id))
        else {
            continue;
        };
        let group = RaftGroupId(snap.raft_group_id);
        let qualifying = handoff_targets(snap, node_id, cluster, |voter| {
            targets.admit(voter) && can_take_over(group, voter)
        });
        let preferred = qualifying
            .iter()
            .copied()
            .filter(|voter| Some(*voter) != candidate.avoid);
        let Some(target) = least_loaded(preferred, &load)
            .or_else(|| least_loaded(qualifying.iter().copied(), &load))
        else {
            plan.stranded.push(candidate.group_id);
            continue;
        };
        plan.handoffs.push(ShedHandoff {
            group_id: candidate.group_id,
            target,
        });
        add_load(&mut load, target);
    }
    plan
}

fn least_loaded(targets: impl Iterator<Item = u64>, load: &HashMap<u64, usize>) -> Option<u64> {
    targets.min_by_key(|target| (load.get(target).copied().unwrap_or(0), *target))
}

fn add_load(load: &mut HashMap<u64, usize>, node: u64) {
    let count = load.entry(node).or_insert(0);
    *count = count.saturating_add(1);
}

/// A group's hot bytes in `heat` (indexed by group id), 0 when unknown.
fn heat_of(heat: &[u64], group_id: u32) -> u64 {
    usize::try_from(group_id)
        .ok()
        .and_then(|index| heat.get(index))
        .copied()
        .unwrap_or(0)
}

/// Where a leadership the node still has to hand off stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pending {
    /// Waits for a tick with a target. `avoid` is the target of the last
    /// handoff that did not move the leadership.
    Queued { avoid: Option<u64> },
    /// Submitted to `target`. The leadership has moved once another node
    /// leads the group. If this node still leads it after `settle_by`, the
    /// transfer did not move it.
    Submitted { target: u64, settle_by: Instant },
}

/// The bookkeeping of one gate's drain: each leadership the node held when
/// the gate shed, until another node leads the group. Free of I/O and of
/// clocks, so tests drive it with synthetic snapshots.
#[derive(Debug, Default)]
pub(crate) struct ShedDrain {
    pending: BTreeMap<u32, Pending>,
}

/// What one tick of a drain saw and plans.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct DrainTick {
    /// Pending groups that another node leads now, or that this node no
    /// longer hosts. They have left the drain.
    pub moved: Vec<u32>,
    /// Submitted handoffs this node still leads after their deadline. They
    /// are queued again, to go to another target if one qualifies.
    pub stalled: Vec<ShedHandoff>,
    pub plan: ShedPlan,
}

impl ShedDrain {
    /// A drain of the groups `node_id` leads in `snaps`.
    pub(crate) fn of_groups_led_by(snaps: &[RaftGroupMetricsSnapshot], node_id: u64) -> Self {
        Self {
            pending: snaps
                .iter()
                .filter(|snap| snap.current_leader == Some(node_id))
                .map(|snap| (snap.raft_group_id, Pending::Queued { avoid: None }))
                .collect(),
        }
    }

    /// Takes on the groups `node_id` leads in `snaps` that the drain does
    /// not hold yet, queued for a first handoff. A shutdown's shed hands
    /// off the leaderships the node gains while it runs too.
    fn adopt_groups_led_by(&mut self, snaps: &[RaftGroupMetricsSnapshot], node_id: u64) {
        for snap in snaps {
            if snap.current_leader == Some(node_id) {
                self.pending
                    .entry(snap.raft_group_id)
                    .or_insert(Pending::Queued { avoid: None });
            }
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.pending.len()
    }

    fn contains(&self, group_id: u32) -> bool {
        self.pending.contains_key(&group_id)
    }

    /// Follows the pending leaderships to `snaps`, taken at `now`, and plans
    /// this tick's handoffs with [`plan_shed_handoffs`]: at most `limit` new
    /// submissions of the queued groups this node leads, the hottest first
    /// by `heat` (hot bytes by group id), ties by group id, each to a node
    /// that `targets` admits.
    ///
    /// A group another node leads, or that this node no longer hosts, has
    /// moved and leaves the drain. A group no node leads stays as it is
    /// until an election settles. A submitted group this node still leads
    /// after its deadline is queued again.
    pub(crate) fn tick(
        &mut self,
        snaps: &[RaftGroupMetricsSnapshot],
        node_id: u64,
        cluster: Option<&Cluster>,
        now: Instant,
        limit: usize,
        heat: &[u64],
        targets: &ShedTargets,
        can_take_over: impl FnMut(RaftGroupId, u64) -> bool,
    ) -> DrainTick {
        let leaders: HashMap<u32, Option<u64>> = snaps
            .iter()
            .map(|snap| (snap.raft_group_id, snap.current_leader))
            .collect();
        let mut moved = Vec::new();
        let mut stalled = Vec::new();
        self.pending
            .retain(|group_id, pending| match leaders.get(group_id) {
                None => {
                    moved.push(*group_id);
                    false
                }
                Some(Some(leader)) if *leader != node_id => {
                    moved.push(*group_id);
                    false
                }
                Some(Some(_)) => {
                    if let Pending::Submitted { target, settle_by } = *pending
                        && now >= settle_by
                    {
                        stalled.push(ShedHandoff {
                            group_id: *group_id,
                            target,
                        });
                        *pending = Pending::Queued {
                            avoid: Some(target),
                        };
                    }
                    true
                }
                Some(None) => true,
            });

        let mut candidates: Vec<ShedCandidate> = self
            .pending
            .iter()
            .filter(|(group_id, _)| leaders.get(group_id) == Some(&Some(node_id)))
            .filter_map(|(group_id, pending)| match pending {
                Pending::Queued { avoid } => Some(ShedCandidate {
                    group_id: *group_id,
                    avoid: *avoid,
                }),
                Pending::Submitted { .. } => None,
            })
            .collect();
        candidates.sort_by_key(|candidate| {
            (
                Reverse(heat_of(heat, candidate.group_id)),
                candidate.group_id,
            )
        });
        let in_flight: Vec<ShedHandoff> = self
            .pending
            .iter()
            .filter_map(|(group_id, pending)| match pending {
                Pending::Submitted { target, .. } => Some(ShedHandoff {
                    group_id: *group_id,
                    target: *target,
                }),
                Pending::Queued { .. } => None,
            })
            .collect();
        let plan = plan_shed_handoffs(
            snaps,
            node_id,
            cluster,
            candidates,
            &in_flight,
            limit,
            targets,
            can_take_over,
        );
        DrainTick {
            moved,
            stalled,
            plan,
        }
    }

    /// Records how submitting `handoff` at `now` went. An accepted handoff
    /// waits `settle` for its leadership to move. A refused one stays queued,
    /// so the next tick plans it again.
    pub(crate) fn record<E>(
        &mut self,
        handoff: ShedHandoff,
        outcome: &Result<(), E>,
        now: Instant,
        settle: Duration,
    ) {
        if outcome.is_ok() {
            self.submitted(handoff, now.checked_add(settle).unwrap_or(now));
        }
    }

    /// Records a submitted handoff. Until `settle_by` the group waits for its
    /// leadership to move.
    fn submitted(&mut self, handoff: ShedHandoff, settle_by: Instant) {
        if let Some(pending) = self.pending.get_mut(&handoff.group_id) {
            *pending = Pending::Submitted {
                target: handoff.target,
                settle_by,
            };
        }
    }

    /// The pending groups `node_id` still leads in `snaps`.
    pub(crate) fn still_led(&self, snaps: &[RaftGroupMetricsSnapshot], node_id: u64) -> usize {
        snaps
            .iter()
            .filter(|snap| {
                snap.current_leader == Some(node_id) && self.contains(snap.raft_group_id)
            })
            .count()
    }
}

/// What one [`LeadershipShed::step`] did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct ShedStep {
    /// Pending groups whose leadership left this node.
    pub moved: Vec<u32>,
    /// Submitted handoffs that did not move their leadership in time.
    pub stalled: Vec<ShedHandoff>,
    /// Handoffs submitted this step.
    pub submitted: Vec<ShedHandoff>,
    /// Handoffs the registry refused this step. They are retried.
    pub failed: Vec<ShedHandoff>,
    /// Pending groups this node leads that no peer could take over.
    pub stranded: Vec<u32>,
}

impl ShedStep {
    fn changed(&self) -> bool {
        !(self.moved.is_empty()
            && self.stalled.is_empty()
            && self.submitted.is_empty()
            && self.failed.is_empty())
    }
}

/// One health gate's shed of this node: the leadership-shed mark of the
/// gate's reason, and the drain of the leaderships the node held when the
/// gate shed. Leaderships the node gains later are left alone, so a shed does
/// not fight the leadership balancer over a node that may still campaign.
///
/// A gate's shed clears the mark when it is dropped, so a gate that stops
/// does not keep the node out of elections. A shutdown's keeps it
/// ([`ShedMark::UntilExit`]).
#[derive(Debug)]
pub(crate) struct LeadershipShed {
    registry: RaftGroupHandleRegistry,
    reason: LeadershipShedReason,
    node_id: u64,
    pace: ShedPace,
    mark: ShedMark,
    drain: ShedDrain,
    /// When each stranded group was last reported.
    stranded_reported: BTreeMap<u32, Instant>,
    /// The handoff check's refusals of each pending group, from the last
    /// step that looked for a target for it.
    refusals: BTreeMap<u32, Vec<(u64, LeadershipTransferError)>>,
}

/// How long a shed's mark holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShedMark {
    /// Until the shed ends or is dropped: a health gate's.
    UntilEnd,
    /// Until the process exits: a shutdown's, whose node must not campaign
    /// again however its handoff ends. Its drain also takes on the
    /// leaderships the node gains while it runs.
    UntilExit,
}

impl LeadershipShed {
    /// A shed of `reason`, whose mark holds as `mark` says.
    pub(crate) fn new(
        registry: &RaftGroupHandleRegistry,
        reason: LeadershipShedReason,
        node_id: u64,
        pace: ShedPace,
        mark: ShedMark,
    ) -> Self {
        Self {
            registry: registry.clone(),
            reason,
            node_id,
            pace,
            mark,
            drain: ShedDrain::default(),
            stranded_reported: BTreeMap::new(),
            refusals: BTreeMap::new(),
        }
    }

    /// Marks the shed and starts handing off the leaderships the node holds.
    pub(crate) fn begin(&mut self) {
        self.registry.mark_leadership_shed(self.reason);
        self.drain = ShedDrain::of_groups_led_by(&self.registry.metrics_snapshot(), self.node_id);
        self.stranded_reported.clear();
        self.refusals.clear();
        tracing::warn!(
            reason = %self.reason,
            node_id = self.node_id,
            leaderships = self.drain.len(),
            "leadership-shed: handing off this node's leaderships"
        );
    }

    /// Clears the mark and stops handing off leaderships.
    pub(crate) fn end(&mut self) {
        self.registry.clear_leadership_shed(self.reason);
        self.drain = ShedDrain::default();
        self.stranded_reported.clear();
        self.refusals.clear();
    }

    /// Whether leaderships are left to hand off. A gate asks its peers which
    /// of them may campaign only while they are.
    pub(crate) fn is_draining(&self) -> bool {
        !self.drain.is_empty()
    }

    /// Hands off this tick's share of the pending leaderships: as many new
    /// submissions as the shed's [`ShedPace`] allows, the hottest group first
    /// (`heat` holds the node's hot bytes by group id), each to a voter that
    /// `targets` admits and the registry's handoff check accepts. A submitted
    /// handoff stays pending until another node leads the group, and is
    /// submitted again, to another target while one qualifies, if this node
    /// still leads it after Raft gave up on the transfer.
    pub(crate) async fn step(&mut self, targets: &ShedTargets, heat: &[u64]) -> ShedStep {
        let now = Instant::now();
        let snaps = self.registry.metrics_snapshot();
        if self.mark == ShedMark::UntilExit {
            self.drain.adopt_groups_led_by(&snaps, self.node_id);
        }
        let registry = &self.registry;
        let view = registry.placement_watch().current();
        // Why the handoff check refused each target it was asked about, for
        // the report of the groups no peer could take over.
        let mut refusals = BTreeMap::<u32, Vec<(u64, LeadershipTransferError)>>::new();
        let tick = self.drain.tick(
            &snaps,
            self.node_id,
            view.cluster(),
            now,
            self.pace.handoffs_per_step(),
            heat,
            targets,
            |group, target| match registry.check_handoff(group, target) {
                Ok(()) => true,
                Err(refusal) => {
                    refusals.entry(group.0).or_default().push((target, refusal));
                    false
                }
            },
        );
        let drain = &self.drain;
        self.refusals
            .retain(|group_id, _| drain.contains(*group_id));
        for handoff in &tick.plan.handoffs {
            self.refusals.remove(&handoff.group_id);
        }
        self.refusals.extend(refusals);
        for handoff in &tick.stalled {
            tracing::warn!(
                reason = %self.reason,
                node_id = self.node_id,
                raft_group_id = handoff.group_id,
                target = handoff.target,
                "leadership-shed: a handoff did not move the leadership; queued again"
            );
        }
        let mut step = ShedStep {
            moved: tick.moved,
            stalled: tick.stalled,
            stranded: tick.plan.stranded,
            ..ShedStep::default()
        };
        for handoff in tick.plan.handoffs {
            let group = RaftGroupId(handoff.group_id);
            let outcome = self.registry.transfer_leader(group, handoff.target).await;
            let settle = self
                .registry
                .get(group)
                .map_or(Duration::ZERO, |raft| raft.handoff_settle_time());
            self.drain.record(handoff, &outcome, Instant::now(), settle);
            match outcome {
                Ok(()) => {
                    tracing::warn!(
                        reason = %self.reason,
                        node_id = self.node_id,
                        raft_group_id = handoff.group_id,
                        target = handoff.target,
                        "leadership-shed: handing off a leadership"
                    );
                    step.submitted.push(handoff);
                }
                Err(err) => {
                    tracing::error!(
                        reason = %self.reason,
                        node_id = self.node_id,
                        raft_group_id = handoff.group_id,
                        target = handoff.target,
                        %err,
                        "leadership-shed: handoff failed; retrying next tick"
                    );
                    step.failed.push(handoff);
                }
            }
        }
        self.report_stranded(&step.stranded, targets, now);
        if step.changed() {
            if self.drain.is_empty() {
                // A gate's shed leaves alone the leaderships the node gained
                // after it began, so the node can still lead some.
                let leaders = snaps
                    .iter()
                    .filter(|snap| snap.current_leader == Some(self.node_id))
                    .count();
                tracing::info!(
                    reason = %self.reason,
                    node_id = self.node_id,
                    leaders,
                    "leadership-shed: no leadership left to hand off"
                );
            } else {
                tracing::info!(
                    reason = %self.reason,
                    node_id = self.node_id,
                    pending = self.drain.len(),
                    still_led = self.drain.still_led(&snaps, self.node_id),
                    submitted = step.submitted.len(),
                    "leadership-shed: handing off leaderships"
                );
            }
        }
        step
    }

    /// Reports the stranded groups in one line per step, each group at most
    /// once per [`STRANDED_REPORT_INTERVAL`], with the handoff check's
    /// refusal of each peer it asked about.
    fn report_stranded(&mut self, stranded: &[u32], targets: &ShedTargets, now: Instant) {
        let drain = &self.drain;
        self.stranded_reported
            .retain(|group_id, _| drain.contains(*group_id));
        let due: Vec<u32> = stranded
            .iter()
            .copied()
            .filter(|group_id| {
                self.stranded_reported.get(group_id).is_none_or(|reported| {
                    now.saturating_duration_since(*reported) >= STRANDED_REPORT_INTERVAL
                })
            })
            .collect();
        if due.is_empty() {
            return;
        }
        for group_id in &due {
            self.stranded_reported.insert(*group_id, now);
        }
        let may_campaign: BTreeSet<u64> = targets
            .nodes
            .iter()
            .copied()
            .filter(|node| *node != self.node_id)
            .collect();
        let refused = self.refusals_of(&due);
        tracing::warn!(
            reason = %self.reason,
            node_id = self.node_id,
            groups = ?due,
            ?may_campaign,
            handoff_check_refused = ?refused,
            "leadership-shed: no peer can take over these leaderships now, because each other \
             voter of these groups answered that it may not campaign, did not answer, or failed \
             the handoff check; they stay pending"
        );
    }
}

impl LeadershipShed {
    /// The handoff check's last refusals of `groups`, each as the refused
    /// target and the reason, for a report.
    fn refusals_of(&self, groups: &[u32]) -> BTreeMap<u32, Vec<String>> {
        groups
            .iter()
            .filter_map(|group_id| {
                let refusals = self.refusals.get(group_id)?;
                Some((
                    *group_id,
                    refusals
                        .iter()
                        .map(|(target, refusal)| format!("node {target}: {refusal}"))
                        .collect(),
                ))
            })
            .collect()
    }

    /// Reports every pending leadership this node still leads, with the
    /// handoff check's last refusals of each, once: a shutdown's report of
    /// what its handoff could not move before it steps down from it.
    pub(crate) fn report_pending(&self) {
        let snaps = self.registry.metrics_snapshot();
        let still_led: Vec<u32> = snaps
            .iter()
            .filter(|snap| {
                snap.current_leader == Some(self.node_id) && self.drain.contains(snap.raft_group_id)
            })
            .map(|snap| snap.raft_group_id)
            .collect();
        if still_led.is_empty() {
            return;
        }
        tracing::warn!(
            reason = %self.reason,
            node_id = self.node_id,
            groups = ?still_led,
            handoff_check_refused = ?self.refusals_of(&still_led),
            "leadership-shed: these leaderships were not handed off"
        );
    }
}

impl Drop for LeadershipShed {
    fn drop(&mut self) {
        match self.mark {
            ShedMark::UntilEnd => self.registry.clear_leadership_shed(self.reason),
            ShedMark::UntilExit => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use proptest::prelude::ProptestConfig;
    use proptest::prelude::Strategy;
    use proptest::prelude::prop;
    use proptest::prop_assert_eq;
    use proptest::prop_oneof;
    use proptest::proptest;
    use proptest::strategy::Just;
    use tokio::time::Instant;
    use ursula_raft::RaftGroupMetricsSnapshot;

    use super::CounterGrowth;
    use super::DrainTick;
    use super::HealthHysteresis;
    use super::HealthTransition;
    use super::SHED_HANDOFFS_PER_TICK;
    use super::ShedCandidate;
    use super::ShedDrain;
    use super::ShedHandoff;
    use super::ShedPlan;
    use super::ShedTargets;
    use super::TickHealth;
    use super::plan_shed_handoffs;

    const ME: u64 = 1;
    /// The transfer deadline the drain tests submit with.
    const SETTLE: Duration = Duration::from_secs(8);

    fn snap(group_id: u32, leader: Option<u64>) -> RaftGroupMetricsSnapshot {
        RaftGroupMetricsSnapshot {
            apply_failure: None,
            raft_group_id: group_id,
            node_id: ME,
            current_term: 1,
            current_leader: leader,
            last_log: None,
            committed: None,
            last_applied: None,
            snapshot: None,
            purged: None,
            voter_ids: vec![1, 2, 3],
            voter_configurations: vec![[1, 2, 3].into()],
            committed_voter_configurations: vec![[1, 2, 3].into()],
            learner_ids: Vec::new(),
            maintenance: ursula_raft::RaftGroupMaintenanceState::default(),
            log: Default::default(),
            history: None,
            recovery: None,
            recovery_gate: ursula_proto::admin::RecoveryGateStatus::Open,
            leads_live: false,
            reconcile: None,
            needs_vote_floor: false,
        }
    }

    /// Targets that admit `nodes` for every handoff.
    fn only(nodes: &[u64]) -> ShedTargets {
        ShedTargets::new(nodes.iter().copied().collect())
    }

    fn every_node() -> ShedTargets {
        only(&[1, 2, 3])
    }

    fn handoff(group_id: u32, target: u64) -> ShedHandoff {
        ShedHandoff { group_id, target }
    }

    fn at(start: Instant, secs: u64) -> Instant {
        start
            .checked_add(Duration::from_secs(secs))
            .expect("test instant")
    }

    /// One drain tick at `now` that may hand off to every peer.
    fn tick(
        drain: &mut ShedDrain,
        snaps: &[RaftGroupMetricsSnapshot],
        now: Instant,
        heat: &[u64],
    ) -> DrainTick {
        drain.tick(
            snaps,
            ME,
            None,
            now,
            SHED_HANDOFFS_PER_TICK,
            heat,
            &every_node(),
            |_, _| true,
        )
    }

    /// Records every planned handoff as accepted at `now`.
    fn submit_all(drain: &mut ShedDrain, plan: &ShedPlan, now: Instant) {
        for handoff in &plan.handoffs {
            drain.record(*handoff, &Ok::<(), ()>(()), now, SETTLE);
        }
    }

    fn transitions(
        hysteresis: &mut HealthHysteresis,
        ticks: &[TickHealth],
    ) -> Vec<HealthTransition> {
        ticks.iter().map(|tick| hysteresis.observe(*tick)).collect()
    }

    /// The hysteresis spelled out over the tick history: shed on a
    /// disqualifying tick, or once the ticks since the last good tick (and
    /// since the last heal) hold `unhealthy_ticks` bad ones, heal once the
    /// ticks since the shed end in `heal_ticks` good ones.
    fn reference(
        ticks: &[TickHealth],
        unhealthy_ticks: usize,
        heal_ticks: usize,
    ) -> Vec<HealthTransition> {
        let (unhealthy_ticks, heal_ticks) = (unhealthy_ticks.max(1), heal_ticks.max(1));
        let mut shedding = false;
        let mut since = 0;
        let mut out = Vec::new();
        for now in 0..ticks.len() {
            let window = &ticks[since..=now];
            let transition = if shedding {
                let good_run = window
                    .iter()
                    .rev()
                    .take_while(|tick| **tick == TickHealth::Good)
                    .count();
                if good_run >= heal_ticks {
                    HealthTransition::Heal
                } else {
                    HealthTransition::Hold
                }
            } else {
                let streak_start = window
                    .iter()
                    .rposition(|tick| *tick == TickHealth::Good)
                    .map_or(0, |good| good.saturating_add(1));
                let bad = window[streak_start..]
                    .iter()
                    .filter(|tick| **tick == TickHealth::Bad)
                    .count();
                if bad >= unhealthy_ticks || ticks[now] == TickHealth::Disqualifying {
                    HealthTransition::Shed
                } else {
                    HealthTransition::Hold
                }
            };
            if transition != HealthTransition::Hold {
                shedding = !shedding;
                since = now.saturating_add(1);
            }
            out.push(transition);
        }
        out
    }

    fn tick_health() -> impl Strategy<Value = TickHealth> {
        prop_oneof![
            Just(TickHealth::Good),
            Just(TickHealth::Inconclusive),
            Just(TickHealth::Bad),
            Just(TickHealth::Disqualifying),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        #[test]
        fn hysteresis_follows_its_definition(
            ticks in prop::collection::vec(tick_health(), 0..64),
            unhealthy_ticks in 0usize..5,
            heal_ticks in 0usize..5,
        ) {
            let mut hysteresis = HealthHysteresis::new(unhealthy_ticks, heal_ticks);
            prop_assert_eq!(
                transitions(&mut hysteresis, &ticks),
                reference(&ticks, unhealthy_ticks, heal_ticks)
            );
        }
    }

    #[test]
    fn sheds_after_unhealthy_ticks_bad_ticks_and_heals_after_heal_ticks() {
        use HealthTransition::Heal;
        use HealthTransition::Hold;
        use HealthTransition::Shed;
        use TickHealth::Bad;
        use TickHealth::Good;

        let mut hysteresis = HealthHysteresis::new(3, 2);
        assert_eq!(
            transitions(&mut hysteresis, &[Bad, Bad, Good, Bad, Bad]),
            vec![Hold, Hold, Hold, Hold, Hold],
            "a good tick restarts the bad streak"
        );
        assert_eq!(transitions(&mut hysteresis, &[Bad]), vec![Shed]);
        assert!(hysteresis.is_shedding());
        assert_eq!(
            transitions(&mut hysteresis, &[Bad, Good, Bad, Good, Good, Good]),
            vec![Hold, Hold, Hold, Hold, Heal, Hold],
        );
        assert!(!hysteresis.is_shedding());
    }

    /// A disqualifying tick sheds without a streak, and breaks a good one
    /// as a bad tick does.
    #[test]
    fn a_disqualifying_tick_sheds_at_once() {
        use HealthTransition::Heal;
        use HealthTransition::Hold;
        use HealthTransition::Shed;
        use TickHealth::Bad;
        use TickHealth::Disqualifying;
        use TickHealth::Good;

        let mut hysteresis = HealthHysteresis::new(3, 2);
        assert_eq!(transitions(&mut hysteresis, &[Bad, Disqualifying]), vec![
            Hold, Shed
        ]);
        assert_eq!(
            transitions(&mut hysteresis, &[Good, Disqualifying, Good, Good]),
            vec![Hold, Hold, Hold, Heal],
        );
        assert_eq!(transitions(&mut hysteresis, &[Disqualifying]), vec![Shed]);
    }

    #[test]
    fn an_inconclusive_tick_neither_breaks_nor_lengthens_a_bad_streak() {
        use HealthTransition::Hold;
        use HealthTransition::Shed;
        use TickHealth::Bad;
        use TickHealth::Good;
        use TickHealth::Inconclusive;

        let mut hysteresis = HealthHysteresis::new(2, 1);
        assert_eq!(
            transitions(&mut hysteresis, &[Inconclusive, Inconclusive, Bad]),
            vec![Hold, Hold, Hold],
        );
        assert_eq!(transitions(&mut hysteresis, &[Inconclusive, Bad]), vec![
            Hold, Shed
        ]);
        assert_eq!(
            transitions(&mut hysteresis, &[Inconclusive, Inconclusive]),
            vec![Hold, Hold],
            "an inconclusive tick never heals"
        );
        assert!(hysteresis.is_shedding());
        assert_eq!(transitions(&mut hysteresis, &[Good]), vec![
            HealthTransition::Heal
        ]);
    }

    #[test]
    fn new_errors_up_to_the_threshold_are_inconclusive() {
        assert_eq!(TickHealth::of_new_errors(0, 1), TickHealth::Good);
        assert_eq!(TickHealth::of_new_errors(1, 1), TickHealth::Inconclusive);
        assert_eq!(TickHealth::of_new_errors(2, 1), TickHealth::Bad);
        assert_eq!(TickHealth::of_new_errors(1, 0), TickHealth::Bad);
        assert_eq!(
            TickHealth::Good
                .max(TickHealth::Inconclusive)
                .max(TickHealth::Bad),
            TickHealth::Bad
        );
    }

    #[test]
    fn counter_growth_starts_from_its_first_reading() {
        let mut growth = CounterGrowth::default();
        assert_eq!(growth.advance(1_000), 0, "errors from before the gate");
        assert_eq!(growth.advance(1_000), 0);
        assert_eq!(growth.advance(1_003), 3);
        assert_eq!(growth.advance(5), 0, "a reset counter is no growth");
        assert_eq!(growth.advance(6), 1);
    }

    #[test]
    fn handoff_plan_honors_its_limit_and_order() {
        let snaps: Vec<_> = (0..6).map(|group| snap(group, Some(ME))).collect();
        let candidates = [5, 0, 2, 3].map(|group_id| ShedCandidate {
            group_id,
            avoid: None,
        });
        let plan = plan_shed_handoffs(
            &snaps,
            ME,
            None,
            candidates,
            &[],
            3,
            &every_node(),
            |_, _| true,
        );
        assert_eq!(plan.handoffs, vec![
            handoff(5, 2),
            handoff(0, 3),
            handoff(2, 2)
        ]);
        assert!(plan.stranded.is_empty());
    }

    #[test]
    fn a_group_without_a_target_does_not_stop_the_plan() {
        let snaps: Vec<_> = (0..=3).map(|group| snap(group, Some(ME))).collect();
        let plan = plan_shed_handoffs(
            &snaps,
            ME,
            None,
            ShedCandidate::every_group_led_by(&snaps, ME),
            &[],
            SHED_HANDOFFS_PER_TICK,
            &every_node(),
            |group, _| group.0 != 0,
        );
        assert_eq!(plan.handoffs, vec![
            handoff(1, 2),
            handoff(2, 3),
            handoff(3, 2)
        ]);
        assert_eq!(plan.stranded, vec![0]);
    }

    #[test]
    fn handoffs_go_only_to_peers_that_may_campaign() {
        // Node 3 leads nothing, so it would win every pick, but it may not
        // campaign (it is draining, gated or shedding for a hard reason) and
        // would refuse each transfer without telling the leader.
        let mut snaps: Vec<_> = (0..6).map(|group| snap(group, Some(ME))).collect();
        snaps.extend((6..9).map(|group| snap(group, Some(2))));
        let plan = plan_shed_handoffs(
            &snaps,
            ME,
            None,
            ShedCandidate::every_group_led_by(&snaps, ME),
            &[],
            usize::MAX,
            &only(&[1, 2]),
            |_, _| true,
        );
        assert_eq!(plan.handoffs.len(), 6);
        assert!(
            plan.handoffs.iter().all(|handoff| handoff.target == 2),
            "{plan:?}"
        );

        // With no peer that may campaign, every group stays pending.
        let plan = plan_shed_handoffs(
            &snaps,
            ME,
            None,
            ShedCandidate::every_group_led_by(&snaps, ME),
            &[],
            usize::MAX,
            &only(&[1]),
            |_, _| true,
        );
        assert!(plan.handoffs.is_empty());
        assert_eq!(plan.stranded, (0..6).collect::<Vec<_>>());
    }

    #[test]
    fn handoffs_in_flight_count_toward_their_target() {
        let snaps: Vec<_> = (0..4).map(|group| snap(group, Some(ME))).collect();
        let plan = plan_shed_handoffs(
            &snaps,
            ME,
            None,
            [2, 3].map(|group_id| ShedCandidate {
                group_id,
                avoid: None,
            }),
            &[handoff(0, 2), handoff(1, 2)],
            usize::MAX,
            &every_node(),
            |_, _| true,
        );
        assert_eq!(plan.handoffs, vec![handoff(2, 3), handoff(3, 3)]);
    }

    #[test]
    fn drain_hands_off_a_few_leaderships_per_tick_until_none_is_left() {
        let groups = 10_u32;
        let mut snaps: Vec<_> = (0..groups).map(|group| snap(group, Some(ME))).collect();
        snaps.push(snap(groups, Some(2)));
        let mut drain = ShedDrain::of_groups_led_by(&snaps, ME);
        let start = Instant::now();

        let mut ticks = 0_u64;
        let mut handed_off = Vec::new();
        loop {
            let now = at(start, ticks.saturating_mul(5));
            let tick = tick(&mut drain, &snaps, now, &[]);
            if drain.is_empty() {
                break;
            }
            ticks = ticks.saturating_add(1);
            assert!(
                tick.plan.handoffs.len() <= SHED_HANDOFFS_PER_TICK,
                "tick {ticks} planned {:?}",
                tick.plan
            );
            submit_all(&mut drain, &tick.plan, now);
            for handoff in tick.plan.handoffs {
                // The transfer lands before the next tick.
                snaps[handoff.group_id as usize].current_leader = Some(handoff.target);
                handed_off.push(handoff.group_id);
            }
        }
        handed_off.sort_unstable();
        assert_eq!(handed_off, (0..groups).collect::<Vec<_>>());
        assert_eq!(ticks, 3, "10 leaderships at 4 per tick");
    }

    #[test]
    fn a_submitted_handoff_stays_pending_until_the_leadership_moves() {
        let mut snaps: Vec<_> = (0..2).map(|group| snap(group, Some(ME))).collect();
        let mut drain = ShedDrain::of_groups_led_by(&snaps, ME);
        let start = Instant::now();

        let first = tick(&mut drain, &snaps, start, &[]);
        assert_eq!(first.plan.handoffs, vec![handoff(0, 2), handoff(1, 3)]);
        submit_all(&mut drain, &first.plan, start);

        // Submitted is not moved: both stay pending and are not planned
        // again before their deadline.
        let second = tick(&mut drain, &snaps, at(start, 5), &[]);
        assert_eq!(second, DrainTick::default());
        assert_eq!(drain.len(), 2);
        assert_eq!(drain.still_led(&snaps, ME), 2);

        // An election in progress settles nothing either.
        snaps[0].current_leader = None;
        assert_eq!(
            tick(&mut drain, &snaps, at(start, 6), &[]),
            DrainTick::default()
        );
        assert_eq!(drain.len(), 2);

        // Completion: another node leads the group.
        snaps[0].current_leader = Some(2);
        let third = tick(&mut drain, &snaps, at(start, 7), &[]);
        assert_eq!(third.moved, vec![0]);
        assert_eq!(drain.len(), 1);
        assert_eq!(drain.still_led(&snaps, ME), 1);
    }

    #[test]
    fn a_handoff_that_does_not_move_is_queued_again_for_another_target() {
        let snaps = vec![snap(0, Some(ME))];
        let mut drain = ShedDrain::of_groups_led_by(&snaps, ME);
        let start = Instant::now();

        let first = tick(&mut drain, &snaps, start, &[]);
        assert_eq!(first.plan.handoffs, vec![handoff(0, 2)]);
        submit_all(&mut drain, &first.plan, start);

        // Before the deadline the handoff is still in flight.
        let waiting = tick(&mut drain, &snaps, at(start, 7), &[]);
        assert!(waiting.plan.handoffs.is_empty());
        assert!(waiting.stalled.is_empty());

        // Past it, this node still leads: the transfer was abandoned, so
        // the group is queued again and goes to the other peer.
        let abandoned = tick(&mut drain, &snaps, at(start, 8), &[]);
        assert_eq!(abandoned.stalled, vec![handoff(0, 2)]);
        assert_eq!(abandoned.plan.handoffs, vec![handoff(0, 3)]);
        submit_all(&mut drain, &abandoned.plan, at(start, 8));

        // When the avoided target is the only one that qualifies, it is
        // tried again.
        let only_three = drain.tick(
            &snaps,
            ME,
            None,
            at(start, 16),
            SHED_HANDOFFS_PER_TICK,
            &[],
            &only(&[1, 3]),
            |_, _| true,
        );
        assert_eq!(only_three.stalled, vec![handoff(0, 3)]);
        assert_eq!(only_three.plan.handoffs, vec![handoff(0, 3)]);
    }

    #[test]
    fn a_refused_submission_stays_queued_for_the_next_tick() {
        let snaps = vec![snap(0, Some(ME)), snap(1, Some(ME))];
        let mut drain = ShedDrain::of_groups_led_by(&snaps, ME);
        let start = Instant::now();

        let first = tick(&mut drain, &snaps, start, &[]);
        assert_eq!(first.plan.handoffs, vec![handoff(0, 2), handoff(1, 3)]);
        // The registry refuses group 0's transfer and accepts group 1's.
        drain.record(handoff(0, 2), &Err("refused"), start, SETTLE);
        drain.record(handoff(1, 3), &Ok::<(), &str>(()), start, SETTLE);

        let retry = tick(&mut drain, &snaps, at(start, 1), &[]);
        assert_eq!(retry.plan.handoffs, vec![handoff(0, 2)]);
        assert!(retry.stalled.is_empty());
        drain.record(handoff(0, 2), &Ok::<(), &str>(()), at(start, 1), SETTLE);

        // The accepted one waits out its settle time, then stalls.
        let waiting = tick(&mut drain, &snaps, at(start, 7), &[]);
        assert_eq!(waiting, DrainTick::default());
        let stalled = tick(&mut drain, &snaps, at(start, 8), &[]);
        assert_eq!(stalled.stalled, vec![handoff(1, 3)]);
    }

    #[test]
    fn new_submissions_count_against_the_cap_and_go_hottest_first() {
        let snaps: Vec<_> = (0..8).map(|group| snap(group, Some(ME))).collect();
        let mut drain = ShedDrain::of_groups_led_by(&snaps, ME);
        let start = Instant::now();
        let heat = [5, 0, 70, 0, 70, 9, 0, 1];

        let first = tick(&mut drain, &snaps, start, &heat);
        assert_eq!(
            first
                .plan
                .handoffs
                .iter()
                .map(|handoff| handoff.group_id)
                .collect::<Vec<_>>(),
            vec![2, 4, 5, 0],
            "hottest first, ties by group id"
        );
        submit_all(&mut drain, &first.plan, start);

        // The four in flight do not use up the next tick's cap.
        let second = tick(&mut drain, &snaps, at(start, 5), &heat);
        assert_eq!(
            second
                .plan
                .handoffs
                .iter()
                .map(|handoff| handoff.group_id)
                .collect::<Vec<_>>(),
            vec![7, 1, 3, 6]
        );
    }

    #[test]
    fn drain_ignores_leaderships_gained_later_and_forgets_unhosted_groups() {
        let mut snaps = vec![snap(0, Some(ME)), snap(1, Some(ME)), snap(2, Some(2))];
        let mut drain = ShedDrain::of_groups_led_by(&snaps, ME);
        let start = Instant::now();

        // No target qualifies for group 1 yet: it is stranded and stays
        // queued. Group 0 goes to node 3, which leads nothing.
        let first = drain.tick(
            &snaps,
            ME,
            None,
            start,
            SHED_HANDOFFS_PER_TICK,
            &[],
            &every_node(),
            |group, _| group.0 != 1,
        );
        assert_eq!(first.plan.handoffs, vec![handoff(0, 3)]);
        assert_eq!(first.plan.stranded, vec![1]);
        submit_all(&mut drain, &first.plan, start);
        snaps[0].current_leader = Some(3);

        // The node gains group 2: it was never pending, so it stays.
        snaps[2].current_leader = Some(ME);
        let second = tick(&mut drain, &snaps, at(start, 1), &[]);
        assert_eq!(second.moved, vec![0]);
        assert_eq!(second.plan.handoffs, vec![handoff(1, 2)]);

        // A pending group the node no longer hosts leaves the drain.
        snaps.remove(1);
        let third = tick(&mut drain, &snaps, at(start, 2), &[]);
        assert_eq!(third.moved, vec![1]);
        assert!(drain.is_empty());
    }
}
