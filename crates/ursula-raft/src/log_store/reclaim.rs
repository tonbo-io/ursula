//! When a core journal gives disk space back, decided without I/O.
//!
//! The writer feeds [`plan`] the journal's segments and, for every group, the
//! oldest segment holding one of its live records and the bytes it holds
//! live there. The plan:
//!
//! - deletes every sealed segment older than the oldest one any group needs;
//! - once the journal holds more than twice its live bytes (and at least
//!   [`ReclaimLimits::min_journal_bytes`]), frees the oldest remaining
//!   segment: groups holding at most [`ReclaimLimits::group_rewrite_bytes`]
//!   live there have those records rewritten into the newest segment, and
//!   groups holding more are reported as lagging, so the snapshot driver
//!   snapshots them and their purge frees the segment.
//!
//! Rewriting only small remainders keeps rewrite traffic a fraction of what
//! the segment frees. A group holding much of an old segment is one whose
//! snapshots fell behind, and copying its log forward would only move the
//! problem.

use std::collections::BTreeSet;

use super::segment::SegmentId;

/// Thresholds of [`plan`], derived from the target segment size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReclaimLimits {
    /// The journal may always hold this much before an old segment is
    /// reclaimed.
    pub(crate) min_journal_bytes: u64,
    /// Live bytes a group may hold in the oldest segment and still be
    /// rewritten out of it.
    pub(crate) group_rewrite_bytes: u64,
}

impl ReclaimLimits {
    pub(crate) fn for_segment_bytes(segment_bytes: u64) -> Self {
        Self {
            min_journal_bytes: segment_bytes.saturating_mul(4),
            group_rewrite_bytes: segment_bytes / 4,
        }
    }
}

/// A group that holds live records in a sealed segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GroupPin {
    pub(crate) group_id: u32,
    /// The oldest segment holding one of the group's live records.
    pub(crate) oldest: SegmentId,
    /// The bytes the group holds live in `oldest`.
    pub(crate) live_in_oldest: u64,
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
}

/// What a reclaim pass does.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ReclaimPlan {
    /// Sealed segments no group needs, oldest first.
    pub(crate) delete: Vec<(SegmentId, u64)>,
    /// The oldest segment kept and the groups whose records in it are
    /// rewritten into the newest segment.
    pub(crate) rewrite: Option<(SegmentId, Vec<u32>)>,
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
    if journal_bytes <= budget {
        return plan;
    }
    let mut rewrite = Vec::new();
    for pin in shape.pins.iter().filter(|pin| pin.oldest == oldest) {
        if pin.live_in_oldest <= limits.group_rewrite_bytes {
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
        plan.rewrite = Some((oldest, rewrite));
    }
    plan
}

#[cfg(test)]
mod tests {
    use super::GroupPin;
    use super::JournalShape;
    use super::ReclaimLimits;
    use super::ReclaimPlan;
    use super::SegmentId;
    use super::plan;

    const SEGMENT: u64 = 64;

    fn limits() -> ReclaimLimits {
        ReclaimLimits::for_segment_bytes(SEGMENT)
    }

    fn sealed(ids: std::ops::RangeInclusive<u64>) -> Vec<(SegmentId, u64)> {
        ids.map(|id| (SegmentId(id), SEGMENT)).collect()
    }

    fn pin(group_id: u32, oldest: u64, live_in_oldest: u64) -> GroupPin {
        GroupPin {
            group_id,
            oldest: SegmentId(oldest),
            live_in_oldest,
        }
    }

    #[test]
    fn segments_older_than_every_group_needs_are_deleted() {
        let sealed = sealed(1..=5);
        let pins = [pin(1, 3, 10), pin(2, 4, 10)];
        let plan = plan(
            JournalShape {
                sealed: &sealed,
                active_bytes: 10,
                live_bytes: 1_000,
                pins: &pins,
            },
            limits(),
        );
        assert_eq!(plan, ReclaimPlan {
            delete: vec![(SegmentId(1), SEGMENT), (SegmentId(2), SEGMENT)],
            ..ReclaimPlan::default()
        });
    }

    #[test]
    fn a_journal_without_live_records_keeps_only_its_newest_segment() {
        let sealed = sealed(1..=3);
        let plan = plan(
            JournalShape {
                sealed: &sealed,
                active_bytes: 10,
                live_bytes: 0,
                pins: &[],
            },
            limits(),
        );
        assert_eq!(plan.delete, sealed);
        assert_eq!(plan.rewrite, None);
    }

    /// Within twice the live bytes nothing is rewritten, however old the
    /// oldest segment.
    #[test]
    fn a_journal_within_its_budget_rewrites_nothing() {
        let sealed = sealed(1..=6);
        let pins = [pin(1, 1, 1), pin(2, 6, 200)];
        let plan = plan(
            JournalShape {
                sealed: &sealed,
                active_bytes: 10,
                live_bytes: 300,
                pins: &pins,
            },
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
        let plan = plan(
            JournalShape {
                sealed: &sealed,
                active_bytes: 10,
                live_bytes: 100,
                pins: &pins,
            },
            limits(),
        );
        assert!(plan.delete.is_empty());
        assert_eq!(plan.rewrite, Some((SegmentId(1), vec![1])));
        assert_eq!(plan.lagging.into_iter().collect::<Vec<_>>(), [2]);
        assert_eq!(
            plan.pinned_segments, 1,
            "segment 2 is still needed by group 3"
        );
    }

    #[test]
    fn the_budget_has_a_floor_of_four_segments() {
        let sealed = sealed(1..=3);
        let pins = [pin(1, 1, 1)];
        let plan = plan(
            JournalShape {
                sealed: &sealed,
                active_bytes: 10,
                live_bytes: 1,
                pins: &pins,
            },
            limits(),
        );
        assert_eq!(plan.rewrite, None, "3 sealed segments fit the floor");
        let sealed = self::sealed(1..=4);
        let plan = super::plan(
            JournalShape {
                sealed: &sealed,
                active_bytes: 10,
                live_bytes: 1,
                pins: &pins,
            },
            limits(),
        );
        assert_eq!(plan.rewrite, Some((SegmentId(1), vec![1])));
    }
}
