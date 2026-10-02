//! Leader-side cold flush planner (bounded-stream-state F10).
//!
//! The planner works from a derived index of the streams that hold hot bytes
//! instead of every stream in the group, reads each stream's hot size from an
//! O(1) counter before copying any payload, and sorts once per pass:
//!
//! - **Group drain** (group hot at or above `min_hot_bytes`, the group flush
//!   threshold): streams are flushed largest first until the group falls
//!   below half of `min_hot_bytes`, instead of flushing every stream that
//!   holds a byte. Below the threshold a group plans nothing.
//! - **Node pressure**: the group drains, largest first, its proportional
//!   share of the node's excess over the pressure target.
//!
//! Streams of equal hot size are ordered starting after a leader-local
//! rotation cursor (the last stream planned by the previous pass), so many
//! equally slow streams take turns. Every candidate gets at most the
//! remaining batch budget (a flush may end mid-chunk), so a stream that does
//! not fit never stops the pass. The index and cursor are not replicated and
//! are not part of snapshots; restore rebuilds the index from the restored
//! hot buffers.

use std::cmp::Ordering;
use std::collections::HashSet;

use super::BucketStreamId;
use super::ColdFlushCandidate;
use super::StreamErrorCode;
use super::StreamResponse;
use super::StreamStateMachine;

#[derive(Debug, Clone, Default)]
pub(super) struct FlushPlannerState {
    /// Streams whose hot buffer is non-empty. Maintained at apply on every
    /// hot-buffer change and stream removal, so it is bounded by live streams.
    hot_streams: HashSet<BucketStreamId>,
    /// The stream that produced the last candidate of the previous pass;
    /// among equally large streams the next pass starts after it.
    cursor: Option<BucketStreamId>,
}

impl FlushPlannerState {
    pub(super) fn mark_hot(&mut self, stream_id: &BucketStreamId) {
        if !self.hot_streams.contains(stream_id) {
            self.hot_streams.insert(stream_id.clone());
        }
    }

    pub(super) fn unmark_hot(&mut self, stream_id: &BucketStreamId) {
        self.hot_streams.remove(stream_id);
    }
}

/// Node-level flush pressure, shared by every group of the node in one pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColdFlushPressure {
    /// Hot bytes observed across the node when the pass started.
    pub node_hot_bytes: u64,
    /// Hot bytes the node should fall to (three quarters of the watermark).
    pub node_target_bytes: u64,
}

impl ColdFlushPressure {
    /// This group's proportional share of the node's excess hot bytes,
    /// rounded up so a group holding any hot bytes drains at least one.
    fn group_drain_bytes(self, group_hot_bytes: u64) -> u64 {
        let excess = self.node_hot_bytes.saturating_sub(self.node_target_bytes);
        if excess == 0 || self.node_hot_bytes == 0 {
            return 0;
        }
        let share = u128::from(group_hot_bytes) * u128::from(excess);
        let node = u128::from(self.node_hot_bytes);
        let drain = share.div_ceil(node);
        u64::try_from(drain)
            .unwrap_or(u64::MAX)
            .min(group_hot_bytes)
    }
}

/// One planning pass over a group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColdFlushPassRequest {
    /// Group hot size at which the group drains (the flush threshold).
    pub min_hot_bytes: usize,
    pub max_flush_bytes: usize,
    pub max_batch_bytes: usize,
    pub max_candidates: usize,
    pub pressure: Option<ColdFlushPressure>,
}

/// Deterministic planner work counters for one pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ColdFlushPlanStats {
    /// Index entries examined; bounded by the streams that hold hot bytes.
    pub streams_visited: usize,
    /// Sorts of the hot-stream list; one per pass.
    pub sorts: usize,
    /// Payload bytes copied into candidates (and hashed for their digest).
    pub bytes_copied: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdFlushPass {
    pub candidates: Vec<ColdFlushCandidate>,
    pub stats: ColdFlushPlanStats,
}

/// Planner-local total order: bucket, affinity, stream. `compare_stream_ids`
/// stays as it is because it also orders TTL expiry and snapshots.
fn planner_order(left: &BucketStreamId, right: &BucketStreamId) -> Ordering {
    left.bucket_id
        .cmp(&right.bucket_id)
        .then_with(|| left.affinity_key.cmp(&right.affinity_key))
        .then_with(|| left.stream_id.cmp(&right.stream_id))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PassMode {
    /// Stop once the group's unplanned hot bytes fall below half of
    /// `min_hot_bytes`.
    GroupDrain,
    /// Stop once this many bytes are planned.
    Pressure(u64),
}

/// Equal-size tie order: streams after the cursor first, then planner order.
fn rotated_order(
    left: &BucketStreamId,
    right: &BucketStreamId,
    cursor: Option<&BucketStreamId>,
) -> Ordering {
    let wrapped = |stream_id: &BucketStreamId| {
        cursor.is_some_and(|cursor| planner_order(stream_id, cursor) != Ordering::Greater)
    };
    wrapped(left)
        .cmp(&wrapped(right))
        .then_with(|| planner_order(left, right))
}

impl StreamStateMachine {
    /// Leader path: plans one pass and advances the rotation cursor.
    pub fn plan_cold_flush_pass(
        &mut self,
        request: ColdFlushPassRequest,
    ) -> Result<ColdFlushPass, StreamResponse> {
        let cursor = self.flush_planner.cursor.clone();
        let (pass, next_cursor) = self.plan_cold_flush_pass_from(request, cursor.as_ref())?;
        if next_cursor.is_some() {
            self.flush_planner.cursor = next_cursor;
        }
        Ok(pass)
    }

    /// Plans one pass without touching the cursor. The candidates are a
    /// deterministic function of the hot index, the request and `cursor`.
    pub fn plan_cold_flush_pass_from(
        &self,
        request: ColdFlushPassRequest,
        cursor: Option<&BucketStreamId>,
    ) -> Result<(ColdFlushPass, Option<BucketStreamId>), StreamResponse> {
        let mut stats = ColdFlushPlanStats::default();
        let mut candidates = Vec::new();
        if request.max_candidates == 0
            || request.max_flush_bytes == 0
            || request.max_batch_bytes == 0
        {
            return Ok((ColdFlushPass { candidates, stats }, None));
        }
        let group_hot_bytes = self.hot_payload_bytes;
        let min_hot_bytes = u64::try_from(request.min_hot_bytes).unwrap_or(u64::MAX);
        let mode = match request.pressure {
            Some(pressure) => PassMode::Pressure(pressure.group_drain_bytes(group_hot_bytes)),
            None if group_hot_bytes >= min_hot_bytes => PassMode::GroupDrain,
            // No stream can hold `min_hot_bytes` while its group holds less.
            None => return Ok((ColdFlushPass { candidates, stats }, None)),
        };

        let mut hot = Vec::with_capacity(self.flush_planner.hot_streams.len());
        for stream_id in &self.flush_planner.hot_streams {
            stats.streams_visited += 1;
            let Some(slot) = self.stream_slot(stream_id) else {
                continue;
            };
            let hot_len = slot.hot_buffer.len();
            if hot_len > 0 {
                hot.push((stream_id, hot_len));
            }
        }
        stats.sorts += 1;
        hot.sort_by(|left, right| {
            right
                .1
                .cmp(&left.1)
                .then_with(|| rotated_order(left.0, right.0, cursor))
        });

        let mut budget = request.max_batch_bytes;
        let mut planned_total = 0u64;
        let mut last_stream = None;
        let drained = |planned_total: u64| match mode {
            PassMode::GroupDrain => {
                group_hot_bytes
                    .saturating_sub(planned_total)
                    .saturating_mul(2)
                    < min_hot_bytes
            }
            PassMode::Pressure(target) => planned_total >= target,
        };
        'streams: for (stream_id, hot_len) in hot {
            let mut start = self.hot_start_offset(stream_id);
            let mut planned_for_stream = 0usize;
            loop {
                if candidates.len() >= request.max_candidates || budget == 0 {
                    break 'streams;
                }
                if drained(planned_total) {
                    break 'streams;
                }
                let remaining = hot_len.saturating_sub(planned_for_stream);
                if remaining == 0 {
                    break;
                }
                let cap = request.max_flush_bytes.min(budget);
                let candidate = match self.plan_cold_flush_with_start(stream_id, start, 1, cap) {
                    Ok(Some(candidate)) => candidate,
                    // A gap (externalized bytes) or a vanished stream ends
                    // this stream's run, never the pass.
                    Ok(None)
                    | Err(StreamResponse::Error {
                        code: StreamErrorCode::StreamGone | StreamErrorCode::StreamNotFound,
                        ..
                    }) => break,
                    Err(err) => return Err(err),
                };
                let len = candidate.payload.len();
                stats.bytes_copied += len;
                planned_for_stream = planned_for_stream.saturating_add(len);
                planned_total =
                    planned_total.saturating_add(u64::try_from(len).unwrap_or(u64::MAX));
                budget = budget.saturating_sub(len);
                start = candidate.end_offset;
                last_stream = Some(stream_id);
                candidates.push(candidate);
            }
        }
        Ok((ColdFlushPass { candidates, stats }, last_stream.cloned()))
    }
}

#[cfg(test)]
mod tests {
    use super::ColdFlushPressure;

    #[test]
    fn pressure_share_is_proportional_and_rounds_up() {
        let pressure = ColdFlushPressure {
            node_hot_bytes: 128,
            node_target_bytes: 96,
        };
        assert_eq!(pressure.group_drain_bytes(64), 16);
        assert_eq!(pressure.group_drain_bytes(1), 1);
        assert_eq!(pressure.group_drain_bytes(0), 0);
        let relieved = ColdFlushPressure {
            node_hot_bytes: 90,
            node_target_bytes: 96,
        };
        assert_eq!(relieved.group_drain_bytes(64), 0);
    }
}
