//! When a core journal gives disk space back, decided without I/O.
//!
//! The writer feeds [`plan`] the journal's segments and, for every group, the
//! oldest segment holding one of its live records and the bytes it holds
//! live there, plus which segments hold dead frames and since when. The plan:
//!
//! - deletes every sealed segment older than the oldest one any group needs;
//! - frees the oldest remaining segment once the journal holds more than
//!   twice its live bytes (and at least [`ReclaimLimits::min_journal_bytes`]),
//!   or once a kept segment has held dead frames for the age the last
//!   maintenance pass gave ([`AgeRule`], #669): groups holding at most
//!   [`ReclaimLimits::group_rewrite_bytes`] live there have those records
//!   rewritten into the newest segment, and groups holding more are reported
//!   as lagging, so the snapshot driver snapshots or purges them and their
//!   purge frees the segment.
//!
//! A dead frame is one whose entry stopped being live: purged, truncated,
//! written again, or copied away by a rewrite. A journal is a run of
//! consecutive segments that only gives back its oldest, so a segment that
//! held dead frames too long is freed by freeing every segment before it.
//! A journal whose kept segments hold no old dead frames is never rewritten
//! for their age, so an idle core settles after one round.
//!
//! Rewriting only small remainders keeps rewrite traffic a fraction of what
//! the segment frees. A group holding much of an old segment is one whose
//! snapshots fell behind, and copying its log forward would only move the
//! problem.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

use super::segment::SegmentId;
use crate::rt::time::Instant;

/// Thresholds of [`plan`], derived from the target segment size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReclaimLimits {
    /// The journal may always hold this much before an old segment is
    /// reclaimed.
    pub(crate) min_journal_bytes: u64,
    /// Live bytes a group may hold in the oldest segment and still be
    /// rewritten out of it.
    pub(crate) group_rewrite_bytes: u64,
    /// Live bytes one pass rewrites at most, so a pass never holds the
    /// writer for long and a mass removal never saturates the disk: a pass
    /// stops copying once it copied this much.
    pub(crate) pass_rewrite_bytes: u64,
}

impl ReclaimLimits {
    pub(crate) fn for_segment_bytes(segment_bytes: u64) -> Self {
        Self {
            min_journal_bytes: segment_bytes.saturating_mul(4),
            group_rewrite_bytes: segment_bytes / 4,
            pass_rewrite_bytes: (segment_bytes / 8).max(1024),
        }
    }
}

/// A group that holds live records in a sealed segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GroupPin {
    /// Stopped apply cannot snapshot. Rewrite under the usual pass budget.
    pub(crate) apply_stopped: bool,
    pub(crate) group_id: u32,
    /// The oldest segment holding one of the group's live records.
    pub(crate) oldest: SegmentId,
    /// The bytes the group holds live in `oldest`.
    pub(crate) live_in_oldest: u64,
}

/// Since when a segment holds dead frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DirtySince {
    /// Not yet on the snapshot driver's clock: `age` is how old the dead
    /// frames were when the writer learned of them, zero for a death since
    /// the last maintenance pass, and the segment's age by its modification
    /// time for one replay found. The next maintenance pass stamps it
    /// ([`DirtySince::stamped`]).
    Unstamped { age: Duration },
    /// Since this time on the driver's clock.
    At(Instant),
}

impl DirtySince {
    /// This on the clock of a maintenance pass at `now`. An age longer than
    /// `max_age` counts as `max_age`, which already frees the segment, so
    /// it fits the clock however long ago the frames died.
    pub(crate) fn stamped(self, now: Instant, max_age: Duration) -> Self {
        match self {
            Self::Unstamped { age } => Self::At(now.checked_sub(age.min(max_age)).unwrap_or(now)),
            at @ Self::At(_) => at,
        }
    }
}

/// Whether a pass frees segments for how long they held dead frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AgeRule {
    /// Only the journal's budget frees segments: no maintenance pass has
    /// run yet.
    Off,
    /// As of the last maintenance pass, at `now`: a kept segment that has
    /// held dead frames for `max_dirty_age` is freed.
    At {
        now: Instant,
        max_dirty_age: Duration,
    },
}

impl AgeRule {
    /// Whether a segment dirty `since` has held dead frames too long.
    pub(crate) fn expired(self, since: Option<DirtySince>) -> bool {
        match (self, since) {
            (Self::At { now, max_dirty_age }, Some(DirtySince::At(at))) => {
                now.saturating_duration_since(at) >= max_dirty_age
            }
            _ => false,
        }
    }
}

/// The journal as [`plan`] sees it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct JournalShape<'a> {
    /// Sealed segments and their lengths, oldest first.
    pub(crate) sealed: &'a [(SegmentId, u64)],
    pub(crate) active_bytes: u64,
    /// The bytes every group holds live, summed.
    pub(crate) live_bytes: u64,
    /// One pin per group that holds a live record.
    pub(crate) pins: &'a [GroupPin],
    /// The segments that hold dead frames, and since when.
    pub(crate) dirty: &'a BTreeMap<SegmentId, DirtySince>,
    /// The time and the age of dead frames that free a segment.
    pub(crate) age: AgeRule,
}

/// Why a pass frees the oldest kept segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FreeCause {
    /// The journal holds more than its budget.
    OverBudget,
    /// A kept segment, this one or a later one, has held dead frames for
    /// the age the [`AgeRule`] gives.
    DirtyAge,
}

/// The rewrite a pass plans out of the oldest kept segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Rewrite {
    pub(crate) target: SegmentId,
    /// The groups whose records in `target` are copied into the newest
    /// segment.
    pub(crate) groups: Vec<u32>,
    pub(crate) cause: FreeCause,
    /// What the pass copies at most ([`ReclaimLimits::pass_rewrite_bytes`]).
    pub(crate) budget: u64,
}

/// What a reclaim pass does.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ReclaimPlan {
    /// Sealed segments no group needs, oldest first.
    pub(crate) delete: Vec<(SegmentId, u64)>,
    /// The oldest segment kept and the groups whose records in it are
    /// rewritten into the newest segment.
    pub(crate) rewrite: Option<Rewrite>,
    /// Groups that keep the oldest segment alive with more than a rewrite
    /// copies.
    pub(crate) lagging: BTreeSet<u32>,
    /// Sealed segments kept only for lagging groups.
    pub(crate) pinned_segments: u64,
}

/// Plans one reclaim pass; see the module documentation.
pub(crate) fn plan(shape: JournalShape<'_>, limits: ReclaimLimits) -> ReclaimPlan {
    let needed = shape.pins.iter().map(|pin| pin.oldest).min();
    let (delete, kept): (Vec<_>, Vec<_>) = shape
        .sealed
        .iter()
        .copied()
        .partition(|(id, _)| needed.is_none_or(|needed| *id < needed));
    let journal_bytes = kept.iter().fold(shape.active_bytes, |total, (_, len)| {
        total.saturating_add(*len)
    });
    let budget = shape
        .live_bytes
        .saturating_mul(2)
        .max(limits.min_journal_bytes);
    let mut plan = ReclaimPlan {
        delete,
        ..ReclaimPlan::default()
    };
    let Some((oldest, _)) = kept.first().copied() else {
        return plan;
    };
    let cause = if journal_bytes > budget {
        FreeCause::OverBudget
    } else if kept
        .iter()
        .any(|(id, _)| shape.age.expired(shape.dirty.get(id).copied()))
    {
        FreeCause::DirtyAge
    } else {
        return plan;
    };
    let mut rewrite = Vec::new();
    for pin in shape.pins.iter().filter(|pin| pin.oldest == oldest) {
        if pin.apply_stopped || pin.live_in_oldest <= limits.group_rewrite_bytes {
            rewrite.push(pin.group_id);
        } else {
            plan.lagging.insert(pin.group_id);
        }
    }
    if !plan.lagging.is_empty() {
        // Groups rewritten out of the oldest segment no longer need it.
        let needed_by_others = shape
            .pins
            .iter()
            .filter(|pin| pin.oldest != oldest)
            .map(|pin| pin.oldest)
            .min();
        plan.pinned_segments = u64::try_from(
            kept.iter()
                .filter(|(id, _)| needed_by_others.is_none_or(|needed| *id < needed))
                .count(),
        )
        .unwrap_or(u64::MAX);
    }
    if !rewrite.is_empty() {
        plan.rewrite = Some(Rewrite {
            target: oldest,
            groups: rewrite,
            cause,
            budget: limits.pass_rewrite_bytes,
        });
    }
    plan
}

/// Whether a maintenance pass under `age` seals the active segment, which
/// holds dead frames `since` then: once they are as old as the rule frees,
/// so that the segment can be freed like a sealed one.
pub(crate) fn rotates_active(age: AgeRule, since: Option<DirtySince>) -> bool {
    age.expired(since)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use super::AgeRule;
    use super::DirtySince;
    use super::FreeCause;
    use super::GroupPin;
    use super::JournalShape;
    use super::ReclaimLimits;
    use super::ReclaimPlan;
    use super::Rewrite;
    use super::SegmentId;
    use super::plan;
    use super::rotates_active;
    use crate::rt::time::Instant;

    const SEGMENT: u64 = 64;
    /// The age of dead frames that frees a segment.
    const MAX_AGE: Duration = Duration::from_secs(900);

    fn limits() -> ReclaimLimits {
        ReclaimLimits::for_segment_bytes(SEGMENT)
    }

    fn sealed(ids: std::ops::RangeInclusive<u64>) -> Vec<(SegmentId, u64)> {
        ids.map(|id| (SegmentId(id), SEGMENT)).collect()
    }

    fn pin(group_id: u32, oldest: u64, live_in_oldest: u64) -> GroupPin {
        GroupPin {
            apply_stopped: false,
            group_id,
            oldest: SegmentId(oldest),
            live_in_oldest,
        }
    }

    fn rewrite(target: u64, groups: &[u32], cause: FreeCause) -> Option<Rewrite> {
        Some(Rewrite {
            target: SegmentId(target),
            groups: groups.to_vec(),
            cause,
            budget: limits().pass_rewrite_bytes,
        })
    }

    /// A journal of `sealed` segments whose groups hold `live_bytes`, with
    /// no dead frames and no maintenance pass yet.
    fn shape<'a>(
        sealed: &'a [(SegmentId, u64)],
        live_bytes: u64,
        pins: &'a [GroupPin],
        dirty: &'a BTreeMap<SegmentId, DirtySince>,
        age: AgeRule,
    ) -> JournalShape<'a> {
        JournalShape {
            sealed,
            active_bytes: 10,
            live_bytes,
            pins,
            dirty,
            age,
        }
    }

    fn no_dirt() -> BTreeMap<SegmentId, DirtySince> {
        BTreeMap::new()
    }

    /// Segment `id` holding dead frames since `since`.
    fn dirty_at(id: u64, since: Instant) -> BTreeMap<SegmentId, DirtySince> {
        BTreeMap::from([(SegmentId(id), DirtySince::At(since))])
    }

    /// `by` after `time`.
    fn later(time: Instant, by: Duration) -> Instant {
        time.checked_add(by).expect("the clock reaches it")
    }

    /// `by` before `time`.
    fn earlier(time: Instant, by: Duration) -> Instant {
        time.checked_sub(by).expect("the clock reaches back to it")
    }

    fn at(now: Instant) -> AgeRule {
        AgeRule::At {
            now,
            max_dirty_age: MAX_AGE,
        }
    }

    #[test]
    fn segments_older_than_every_group_needs_are_deleted() {
        let sealed = sealed(1..=5);
        let pins = [pin(1, 3, 10), pin(2, 4, 10)];
        let dirty = no_dirt();
        let plan = plan(shape(&sealed, 1_000, &pins, &dirty, AgeRule::Off), limits());
        assert_eq!(plan, ReclaimPlan {
            delete: vec![(SegmentId(1), SEGMENT), (SegmentId(2), SEGMENT)],
            ..ReclaimPlan::default()
        });
    }

    #[test]
    fn a_journal_without_live_records_keeps_only_its_newest_segment() {
        let sealed = sealed(1..=3);
        let dirty = no_dirt();
        let plan = plan(shape(&sealed, 0, &[], &dirty, AgeRule::Off), limits());
        assert_eq!(plan.delete, sealed);
        assert_eq!(plan.rewrite, None);
    }

    /// Within twice the live bytes nothing is rewritten, however old the
    /// oldest segment, while no kept segment holds old dead frames.
    #[test]
    fn a_journal_within_its_budget_rewrites_nothing() {
        let sealed = sealed(1..=6);
        let pins = [pin(1, 1, 1), pin(2, 6, 200)];
        let dirty = no_dirt();
        let plan = plan(
            shape(&sealed, 300, &pins, &dirty, at(Instant::now())),
            limits(),
        );
        assert_eq!(plan, ReclaimPlan::default());
    }

    /// Past its budget the oldest segment is freed: small remainders are
    /// rewritten and a group holding much of it is reported lagging.
    #[test]
    fn small_remainders_are_rewritten_and_large_ones_reported() {
        let sealed = sealed(1..=8);
        let pins = [pin(1, 1, 4), pin(2, 1, 40), pin(3, 2, 30), pin(4, 7, 50)];
        let dirty = no_dirt();
        let plan = plan(shape(&sealed, 100, &pins, &dirty, AgeRule::Off), limits());
        assert!(plan.delete.is_empty());
        assert_eq!(plan.rewrite, rewrite(1, &[1], FreeCause::OverBudget));
        assert_eq!(plan.lagging.into_iter().collect::<Vec<_>>(), [2]);
        assert_eq!(
            plan.pinned_segments, 1,
            "segment 2 is still needed by group 3"
        );
    }

    #[test]
    fn the_budget_has_a_floor_of_four_segments() {
        let pins = [pin(1, 1, 1)];
        let dirty = no_dirt();
        let sealed = self::sealed(1..=3);
        let plan = super::plan(shape(&sealed, 1, &pins, &dirty, AgeRule::Off), limits());
        assert_eq!(plan.rewrite, None, "3 sealed segments fit the floor");
        let sealed = self::sealed(1..=4);
        let plan = super::plan(shape(&sealed, 1, &pins, &dirty, AgeRule::Off), limits());
        assert_eq!(plan.rewrite, rewrite(1, &[1], FreeCause::OverBudget));
    }

    /// #669: a kept segment that has held dead frames for the age frees the
    /// oldest kept segment though the journal is within its budget, and
    /// the next passes free the ones up to it.
    #[test]
    fn an_expired_dirty_segment_is_freed_within_budget() {
        let start = Instant::now();
        let sealed = sealed(1..=3);
        let pins = [pin(1, 1, 10), pin(2, 3, 10)];
        let dirty = dirty_at(2, start);
        let expired = later(start, MAX_AGE);
        let plan = plan(shape(&sealed, 1_000, &pins, &dirty, at(expired)), limits());
        assert_eq!(plan.rewrite, rewrite(1, &[1], FreeCause::DirtyAge));
        assert!(plan.lagging.is_empty());
        // Once group 1 moved on, segment 1 goes and segment 2 is next.
        let pins = [pin(1, 2, 10), pin(2, 3, 10)];
        let plan = super::plan(shape(&sealed, 1_000, &pins, &dirty, at(expired)), limits());
        assert_eq!(plan.delete, [(SegmentId(1), SEGMENT)]);
        assert_eq!(plan.rewrite, rewrite(2, &[1], FreeCause::DirtyAge));
    }

    /// A segment whose dead frames are younger than the age, or not yet on
    /// the driver's clock, or a journal before its first maintenance pass,
    /// is left alone, and so is one without dead frames: nothing rewrites
    /// a settled journal.
    #[test]
    fn a_clean_or_young_segment_is_left_alone() {
        let start = Instant::now();
        let sealed = sealed(1..=3);
        let pins = [pin(1, 1, 10)];
        let clean = no_dirt();
        let late = later(start, MAX_AGE.saturating_mul(10));
        assert_eq!(
            plan(shape(&sealed, 1_000, &pins, &clean, at(late)), limits()),
            ReclaimPlan::default()
        );
        let dirty = dirty_at(1, start);
        let young = earlier(later(start, MAX_AGE), Duration::from_millis(1));
        assert_eq!(
            plan(shape(&sealed, 1_000, &pins, &dirty, at(young)), limits()),
            ReclaimPlan::default()
        );
        assert_eq!(
            plan(shape(&sealed, 1_000, &pins, &dirty, AgeRule::Off), limits()),
            ReclaimPlan::default()
        );
        let unstamped = BTreeMap::from([(SegmentId(1), DirtySince::Unstamped {
            age: MAX_AGE.saturating_mul(2),
        })]);
        assert_eq!(
            plan(shape(&sealed, 1_000, &pins, &unstamped, at(late)), limits()),
            ReclaimPlan::default()
        );
    }

    /// Rotate and free before rewrite: a dirty active segment is sealed
    /// once its dead frames are old, and a sealed segment no group needs is
    /// deleted without copying anything, however dirty.
    #[test]
    fn rotate_and_free_is_preferred_to_a_rewrite() {
        let start = Instant::now();
        let since = Some(DirtySince::At(start));
        assert!(!rotates_active(
            at(later(start, Duration::from_secs(450))),
            since
        ));
        assert!(rotates_active(at(later(start, MAX_AGE)), since));
        assert!(!rotates_active(at(later(start, MAX_AGE)), None));
        assert!(!rotates_active(AgeRule::Off, since));
        let sealed = sealed(1..=2);
        let dirty = dirty_at(2, start);
        let plan = plan(
            shape(&sealed, 0, &[], &dirty, at(later(start, MAX_AGE))),
            limits(),
        );
        assert_eq!(plan.delete, sealed);
        assert_eq!(plan.rewrite, None);
    }

    /// The age rule rewrites no more than the journal's budget does: a pass
    /// copies at most an eighth of a segment, and a group holding more than
    /// a quarter of the segment is reported lagging for the snapshot driver
    /// instead of copied.
    #[test]
    fn the_age_rule_keeps_the_rewrite_budget() {
        let start = Instant::now();
        let sealed = sealed(1..=2);
        let pins = [pin(1, 1, 16), pin(2, 1, 17)];
        let dirty = dirty_at(1, start);
        let plan = plan(
            shape(&sealed, 1_000, &pins, &dirty, at(later(start, MAX_AGE))),
            limits(),
        );
        assert_eq!(plan.rewrite, rewrite(1, &[1], FreeCause::DirtyAge));
        assert_eq!(plan.lagging.into_iter().collect::<Vec<_>>(), [2]);
        assert_eq!(limits().pass_rewrite_bytes, 1024);
        assert_eq!(
            ReclaimLimits::for_segment_bytes(64 * 1024 * 1024).pass_rewrite_bytes,
            8 * 1024 * 1024
        );
    }

    /// An age learned at open counts on the clock of the first maintenance
    /// pass, capped at the age that frees the segment.
    #[test]
    fn an_unstamped_age_is_stamped_on_the_pass_clock() {
        let now = later(Instant::now(), MAX_AGE.saturating_mul(4));
        let young = DirtySince::Unstamped {
            age: Duration::from_secs(1),
        };
        assert_eq!(
            young.stamped(now, MAX_AGE),
            DirtySince::At(earlier(now, Duration::from_secs(1)))
        );
        let old = DirtySince::Unstamped {
            age: MAX_AGE.saturating_mul(3),
        };
        assert_eq!(
            old.stamped(now, MAX_AGE),
            DirtySince::At(earlier(now, MAX_AGE))
        );
        assert!(at(now).expired(Some(old.stamped(now, MAX_AGE))));
        let stamped = DirtySince::At(now);
        assert_eq!(stamped.stamped(later(now, MAX_AGE), MAX_AGE), stamped);
    }
}
