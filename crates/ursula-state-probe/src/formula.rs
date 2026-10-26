//! The per-structure formula checks (`docs/architecture/bounded-stream-state.md`),
//! evaluated per stream with the stream's own U, K and P at a checkpoint.
//!
//! Each check records whether the code meets the design's target. The gate's
//! ratchet file records which targets are met, so a regression (a met target
//! lost) fails and an improvement is reported for tightening.

use crate::codec::SnapStats;
use crate::out::Measured;
use crate::out::Outcome;
use crate::out::ratio;
use crate::out::round3;

/// Producer ids per stream (F3).
pub const MAX_PRODUCERS: u64 = 4_096;
/// Shared pack references per stream (F2).
pub const MAX_SHARED_REFS: u64 = 64;
/// Staged external refs per stream (F5).
pub const MAX_STAGED_EXTERNAL: u64 = 16;

/// `Prod(s) <= 0.4 KiB * P(s) + 56 KiB` (I1).
pub fn producer_bound(producers: u64) -> u64 {
    producers.saturating_mul(410).saturating_add(56 * 1024)
}

/// Evaluate every per-stream structure check against one checkpoint.
/// `shared_refs_interval` is the refs one compaction-driver interval may add.
pub fn per_stream_checks(outcome: &mut Outcome, m: &Measured, shared_refs_interval: u64) {
    for s in &m.snap.streams {
        outcome.check(
            "f2_shared_refs_per_stream",
            "shared refs per stream <= 64 + refs added in one driver interval (F2)",
            s.shared_refs as f64,
            MAX_SHARED_REFS.saturating_add(shared_refs_interval) as f64,
        );
        outcome.check(
            "f3_producers_per_stream",
            "producers per stream <= 4,096 (F3)",
            s.producers as f64,
            MAX_PRODUCERS as f64,
        );
        outcome.check(
            "f3_producer_bytes_per_stream",
            "producer snapshot bytes per stream within Prod(s) = 0.4 KiB * P + 56 KiB (F3)",
            s.producer_bytes as f64,
            producer_bound(s.producers) as f64,
        );
        outcome.check(
            "f5_staged_external_refs_per_stream",
            "staged external refs per stream <= 16 (F5)",
            s.external_segments as f64,
            MAX_STAGED_EXTERNAL as f64,
        );
    }
    let g = &m.gauges;
    outcome.check(
        "f8_ttl_heap_entries",
        "TTL heap entries <= 2 x live TTL streams (F8)",
        g.ttl_heap_entries as f64,
        g.ttl_streams.saturating_mul(2) as f64,
    );
}

/// The group snapshot bytes one end record holds (F20, bounded-state I2):
/// the 122-byte id limit plus about 128 B of fields, rounded up.
pub const MAX_END_RECORD_BYTES: u64 = 256;

/// F20 before the sweep folds the removals' end records: how many the group
/// holds and what each adds to its snapshot, at most
/// [`MAX_END_RECORD_BYTES`].
pub fn end_records_held(outcome: &mut Outcome, snap: &SnapStats) {
    let per_removal = ratio(snap.end_record_bytes, snap.end_record_frames);
    outcome.metric_u64("end_records_before_retire", snap.end_record_frames);
    outcome.metric("end_record_bytes_per_removal", round3(per_removal));
    outcome.check(
        "f20_end_record_bytes_per_removal",
        "an end record adds at most 256 B to a group snapshot (F20)",
        per_removal,
        MAX_END_RECORD_BYTES as f64,
    );
}

/// F20 after the sweep folded through the latest usage seq: the group's
/// snapshot holds no end record.
pub fn end_records_retired(outcome: &mut Outcome, m: &Measured) {
    outcome.check(
        "f20_end_records_after_retire",
        "no end record is left once the sweep folded through the latest seq (F20)",
        m.snap.end_record_frames as f64,
        0.0,
    );
}

/// `capacity after flush or retention <= 2 x len + 64` (F7), aggregated over
/// the whole state machine: slack (requested minus deep-clone bytes) at most
/// the tight size plus 64 KiB.
pub fn capacity_check(outcome: &mut Outcome, m: &Measured) {
    outcome.check(
        "f7_capacity_slack",
        "heap slack after flush/retention <= tight size + 64 KiB (F7)",
        m.slack_bytes() as f64,
        (m.tight.bytes.saturating_add(64 * 1024)) as f64,
    );
}

/// Residual `state - H - Prod` for one stream, with state
/// measured as the stream's snapshot frame bytes.
pub fn residual(m: &Measured) -> i64 {
    m.snap
        .streams
        .iter()
        .map(|s| {
            let frame = i64::try_from(s.frame_bytes).unwrap_or(i64::MAX);
            let allowance = s.hot_bytes.saturating_add(s.producer_bytes);
            frame.saturating_sub(i64::try_from(allowance).unwrap_or(i64::MAX))
        })
        .sum()
}
