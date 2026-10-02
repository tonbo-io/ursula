//! The §7.2 per-structure formula checks (`docs/architecture/bounded-stream-state.md`),
//! evaluated per stream with the stream's own U, K and P at a checkpoint.
//!
//! Each check records whether today's code meets the design's target. Most do
//! not yet; the gate's ratchet file records which, so a regression (a met
//! target lost) fails and an improvement is reported for tightening.

use crate::out::Measured;
use crate::out::Outcome;

/// Receipt-item window per stream (F3).
pub const RECEIPT_WINDOW: u64 = 1_024;
/// Producer ids per stream (F3).
pub const MAX_PRODUCERS: u64 = 4_096;
/// Shared pack references per stream (F2).
pub const MAX_SHARED_REFS: u64 = 64;
/// Staged external refs per stream (F5).
pub const MAX_STAGED_EXTERNAL: u64 = 16;
/// Hot overhead per unflushed record with F6b (bytes).
pub const HOT_OVERHEAD_PER_RECORD: f64 = 24.0;
/// Hot overhead per unflushed record with F6b and F4b, from feature level 4
/// (bytes).
pub const HOT_OVERHEAD_PER_RECORD_LB4: f64 = 12.0;
/// Payload bytes per hot block (F6b).
pub const HOT_BLOCK_BYTES: u64 = 64 * 1024;
/// Header bytes allowed per hot block (F6b).
pub const HOT_BLOCK_HEADER_ALLOWANCE: u64 = 64;
/// Snapshot bytes of mark fields per cold MiB (F1).
pub const MARK_SNAPSHOT_BYTES_PER_COLD_MIB: u64 = 32;

/// `Prod(s) <= 0.4 KiB * P(s) + 56 KiB` (I1).
pub fn producer_bound(producers: u64) -> u64 {
    producers.saturating_mul(410).saturating_add(56 * 1024)
}

/// Evaluate every per-stream structure check against one checkpoint.
/// `shared_refs_interval` is the refs one compaction-driver interval may add.
pub fn per_stream_checks(outcome: &mut Outcome, m: &Measured, shared_refs_interval: u64) {
    for s in &m.snap.streams {
        let cold_mib = s.cold_mib_ceil();
        outcome.check(
            "f1_dense_entries_eq_unflushed",
            "dense record entries = unflushed records, plus the one straddling the seal point (F1)",
            s.dense_entries as f64,
            (s.unflushed_records + 1) as f64,
        );
        outcome.check(
            "f1_mark_snapshot_bytes_per_cold_mib",
            "snapshot bytes of the mark fields 17-19 <= 32 per cold MiB, and every record \
             below the seal point sealed into them (F1)",
            if s.dense_entries > s.unflushed_records + 1 {
                // Dense offsets below the seal point stand in for marks.
                (s.record_marks_bytes + s.record_offsets_bytes) as f64
            } else {
                s.record_marks_bytes as f64
            },
            (MARK_SNAPSHOT_BYTES_PER_COLD_MIB * cold_mib.max(1)) as f64,
        );
        outcome.check(
            "f2_shared_refs_per_stream",
            "shared refs per stream <= 64 + refs added in one driver interval (F2)",
            s.shared_refs as f64,
            (MAX_SHARED_REFS + shared_refs_interval) as f64,
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
            "f4_message_records_per_stream",
            "message records per stream <= unflushed records + 2 (F4a)",
            s.message_records as f64,
            (s.unflushed_records + 2) as f64,
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
        "f1_record_marks",
        "record marks <= ceil(cold MiB) + 2 (F1)",
        g.record_marks as f64,
        (m.snap
            .streams
            .iter()
            .map(|s| s.cold_mib_ceil() + 2)
            .sum::<u64>()) as f64,
    );
    outcome.check(
        "f3_receipt_items_per_stream",
        "receipt items per stream <= 1,024 (F3)",
        g.max_receipt_items_per_stream as f64,
        RECEIPT_WINDOW as f64,
    );
    outcome.check(
        "f8_ttl_heap_entries",
        "TTL heap entries <= 2 x live TTL streams (F8)",
        g.ttl_heap_entries as f64,
        (2 * g.ttl_streams) as f64,
    );
    let unflushed: u64 = m.snap.streams.iter().map(|s| s.unflushed_records).sum();
    if unflushed > 0 {
        // Hot overhead per unflushed record: hot-buffer headers and append
        // starts (8 B each, F4b) + message records (16 B) + dense offsets
        // (8 B) for records above the seal point. With F6b the hot buffer
        // holds one header per block of up to 64 KiB, which is per payload
        // byte rather than per record: the bound allows one header per
        // started block of each stream's hot bytes, so per-append headers
        // would still fail it. From feature level 4 message records are gone
        // and the target drops from 24 B to 12 B per record.
        let hot_message_records = m.snap.message_records_count.min(unflushed);
        let overhead = g.hot_overhead_bytes + 16 * hot_message_records + 8 * unflushed;
        let block_allowance: u64 = m
            .snap
            .streams
            .iter()
            .filter(|s| s.hot_bytes > 0)
            .map(|s| HOT_BLOCK_HEADER_ALLOWANCE * (s.hot_bytes.div_ceil(HOT_BLOCK_BYTES) + 1))
            .sum();
        let per_record = if g.feature_level >= ursula_stream::FEATURE_LEVEL_HOT_REPRESENTATION {
            HOT_OVERHEAD_PER_RECORD_LB4
        } else {
            HOT_OVERHEAD_PER_RECORD
        };
        outcome.check(
            "f6_hot_overhead_per_unflushed_record",
            "hot overhead per unflushed record <= 24 B (12 B from level 4, F4b) plus one header \
             per 64 KiB hot block (F6b)",
            overhead as f64 / unflushed as f64,
            per_record + block_allowance as f64 / unflushed as f64,
        );
    }
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

/// Residual `state - H - 8 U - 16 B K - Prod` for one stream, with state
/// measured as the stream's snapshot frame bytes.
pub fn residual(m: &Measured) -> i64 {
    m.snap
        .streams
        .iter()
        .map(|s| {
            let frame = i64::try_from(s.frame_bytes).unwrap_or(i64::MAX);
            let allowance =
                s.hot_bytes + 8 * s.unflushed_records + 16 * s.cold_mib_ceil() + s.producer_bytes;
            frame.saturating_sub(i64::try_from(allowance).unwrap_or(i64::MAX))
        })
        .sum()
}
