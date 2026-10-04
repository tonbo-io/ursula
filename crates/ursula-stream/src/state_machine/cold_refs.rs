//! Leader-side, read-only queries over a group's cold references.
//!
//! - Shared pack-reference compaction discovery (bounded-stream-state F2,
//!   §5.3): which streams the leader's driver should compact next, and the
//!   run of shared refs to compact for each.
//! - Orphan-sweep references (F14h, §5.15): every object path the group's
//!   replicated state still references, so the sweep never deletes one.
//!
//! Nothing here mutates replicated state. The idle tracker is leader-local
//! and is rebuilt from scratch after a restart or leader change.

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;

use ursula_shard::BucketStreamId;

use super::ColdChunkRef;
use super::ColdGcTarget;
use super::StreamStateMachine;

/// Shared refs per stream at which the driver compacts it (T in §5.3).
pub const SHARED_REF_COMPACTION_THRESHOLD: usize = 64;

/// How long a stream's tail must stay put before the driver compacts its
/// shared refs even below the threshold (§5.3: one hour).
pub const SHARED_REF_IDLE_MS: u64 = 60 * 60 * 1_000;

/// One discovery pass of the shared-ref compaction driver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedRefCompactionRequest {
    /// Streams with at least this many shared refs are candidates.
    pub min_refs: usize,
    /// Streams with at least one shared ref and a tail unchanged for this
    /// long are candidates too.
    pub idle_ms: u64,
    /// Leader wall clock for the idle tracker.
    pub now_ms: u64,
    /// Upper bound on the logical bytes of one planned run
    /// (`compaction_max_size`). A single slice larger than this is still
    /// planned alone, so every candidate makes progress.
    pub max_run_bytes: u64,
    /// Maximum candidates returned.
    pub limit: usize,
    /// The legacy-pack filter (#278): only shared refs into pre-erasure-domain
    /// packs ([`is_legacy_cross_bucket_pack`]) count, every stream holding one
    /// is a candidate regardless of `min_refs` and `idle_ms`, and the idle
    /// tracker is left untouched.
    pub legacy_packs_only: bool,
}

impl SharedRefCompactionRequest {
    /// The design's defaults (T = 64, idle after one hour) for `now_ms`.
    pub fn new(now_ms: u64, max_run_bytes: u64, limit: usize) -> Self {
        Self {
            min_refs: SHARED_REF_COMPACTION_THRESHOLD,
            idle_ms: SHARED_REF_IDLE_MS,
            now_ms,
            max_run_bytes,
            limit,
            legacy_packs_only: false,
        }
    }

    /// The legacy-pack migration's filter: every stream holding a shared ref
    /// into a pre-erasure-domain pack, with its oldest contiguous run of them.
    pub fn legacy_packs(max_run_bytes: u64, limit: usize) -> Self {
        Self {
            min_refs: 1,
            idle_ms: 0,
            now_ms: 0,
            max_run_bytes,
            limit,
            legacy_packs_only: true,
        }
    }
}

/// Whether `chunk` is a shared slice of a pack written before bucket erasure
/// domains, which may hold several tenants: its path is not under the
/// stream's own `{bucket}/_packs/` prefix.
pub fn is_legacy_cross_bucket_pack(stream_id: &BucketStreamId, chunk: &ColdChunkRef) -> bool {
    chunk.shared_object
        && !chunk
            .s3_path
            .strip_prefix(stream_id.bucket_id.as_str())
            .is_some_and(|rest| rest.starts_with("/_packs/"))
}

/// A stream the driver should compact, with the run it should compact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedRefCandidate {
    pub stream_id: BucketStreamId,
    /// Cold generation the replacement chunk is written under (F14g).
    pub cold_generation: u64,
    /// Shared refs the stream holds now.
    pub shared_refs: usize,
    /// Fewest live slices among the packs the planned run pins. Candidates
    /// are ordered by it, so nearly empty packs are released first.
    pub min_pack_live_slices: u64,
    /// The oldest contiguous run of shared refs (each start equals the
    /// previous end), at most `max_run_bytes` unless a single slice is larger.
    pub run: Vec<ColdChunkRef>,
}

impl SharedRefCandidate {
    /// Logical bytes covered by the run.
    pub fn run_bytes(&self) -> u64 {
        self.run.iter().fold(0_u64, |total, chunk| {
            total.saturating_add(chunk.end_offset.saturating_sub(chunk.start_offset))
        })
    }
}

/// Leader-local record of when each stream holding shared refs last moved
/// its tail. Not replicated; a new leader starts empty, which only delays
/// idle compaction by one idle period.
#[derive(Debug, Clone, Default)]
pub struct SharedRefIdleTracker {
    tails: HashMap<BucketStreamId, (u64, u64)>,
}

impl SharedRefIdleTracker {
    /// Records `tail_offset` for `stream_id` at `now_ms` and returns how long
    /// the tail has stayed put.
    fn observe(&mut self, stream_id: &BucketStreamId, tail_offset: u64, now_ms: u64) -> u64 {
        let entry = self
            .tails
            .entry(stream_id.clone())
            .or_insert((tail_offset, now_ms));
        if entry.0 != tail_offset {
            *entry = (tail_offset, now_ms);
        }
        now_ms.saturating_sub(entry.1)
    }

    /// Streams currently tracked.
    pub fn len(&self) -> usize {
        self.tails.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tails.is_empty()
    }
}

/// The oldest contiguous run of `refs` (sorted by start offset first): each
/// start equals the previous end, and the run's logical bytes stay within
/// `max_bytes`, except that the first slice is always taken.
pub fn plan_shared_ref_run(refs: &[ColdChunkRef], max_bytes: u64) -> Vec<ColdChunkRef> {
    let mut sorted = refs
        .iter()
        .filter(|chunk| chunk.shared_object && chunk.end_offset > chunk.start_offset)
        .collect::<Vec<_>>();
    sorted.sort_by_key(|chunk| (chunk.start_offset, chunk.end_offset));
    let mut run: Vec<ColdChunkRef> = Vec::new();
    let mut bytes = 0_u64;
    for chunk in sorted {
        let len = chunk.end_offset.saturating_sub(chunk.start_offset);
        if let Some(last) = run.last()
            && (chunk.start_offset != last.end_offset || bytes.saturating_add(len) > max_bytes)
        {
            break;
        }
        bytes = bytes.saturating_add(len);
        run.push(chunk.clone());
    }
    run
}

impl StreamStateMachine {
    /// F2 discovery (`shared_ref_candidates`): streams with at least
    /// `min_refs` shared refs, or with at least one and a tail that has not
    /// moved for `idle_ms` (tracked leader-locally in `tracker`), ordered by
    /// the occupancy of the packs their runs pin, fewest live slices first,
    /// then by stream id. No snapshot and no object-store listing.
    pub fn shared_ref_candidates(
        &self,
        request: &SharedRefCompactionRequest,
        tracker: &mut SharedRefIdleTracker,
    ) -> Vec<SharedRefCandidate> {
        let mut seen = HashSet::new();
        let mut candidates = Vec::new();
        for slot in self.registry.slots() {
            let stream_id = &slot.metadata.stream_id;
            let refs = slot.cold.cold_chunks();
            let run = if request.legacy_packs_only {
                let legacy = refs
                    .iter()
                    .filter(|chunk| is_legacy_cross_bucket_pack(stream_id, chunk))
                    .cloned()
                    .collect::<Vec<_>>();
                if legacy.is_empty() {
                    continue;
                }
                (
                    legacy.len(),
                    plan_shared_ref_run(&legacy, request.max_run_bytes),
                )
            } else {
                let shared_refs = refs.iter().filter(|chunk| chunk.shared_object).count();
                if shared_refs == 0 {
                    continue;
                }
                let idle_for =
                    tracker.observe(stream_id, slot.metadata.tail_offset, request.now_ms);
                seen.insert(stream_id.clone());
                if shared_refs < request.min_refs.max(1) && idle_for < request.idle_ms {
                    continue;
                }
                (
                    shared_refs,
                    plan_shared_ref_run(refs, request.max_run_bytes),
                )
            };
            let (shared_refs, run) = run;
            if run.is_empty() {
                continue;
            }
            let min_pack_live_slices = run
                .iter()
                .filter_map(|chunk| self.shared_cold_object_refs.get(&chunk.s3_path).copied())
                .min()
                .unwrap_or(0);
            candidates.push(SharedRefCandidate {
                stream_id: stream_id.clone(),
                cold_generation: slot.cold.cold_generation(),
                shared_refs,
                min_pack_live_slices,
                run,
            });
        }
        if !request.legacy_packs_only {
            tracker
                .tails
                .retain(|stream_id, _| seen.contains(stream_id));
        }
        candidates.sort_by(|left, right| {
            left.min_pack_live_slices
                .cmp(&right.min_pack_live_slices)
                .then_with(|| super::compare_stream_ids(&left.stream_id, &right.stream_id))
        });
        candidates.truncate(request.limit);
        candidates
    }

    /// Every bucket this group holds state for, sorted. The orphan sweep
    /// lists the group's pack prefix under each.
    pub fn bucket_ids(&self) -> Vec<String> {
        let mut buckets = self.buckets.iter().cloned().collect::<Vec<_>>();
        buckets.sort();
        buckets
    }

    /// Group-wide object paths that replicated state still references and
    /// that no stream's own refs name: every live shared object (pack) and
    /// every path a pending cold-GC entry will delete. The orphan sweep
    /// (F14h) never deletes them; GC entries keep their grace.
    pub fn group_referenced_cold_paths(&self) -> BTreeSet<String> {
        let mut paths = self
            .shared_cold_object_refs
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        for entry in self.cold_gc.entries() {
            if let ColdGcTarget::Paths(targets) = &entry.target {
                paths.extend(targets.iter().cloned());
            }
        }
        paths
    }

    /// Object paths one stream's replicated state references directly:
    /// shared and exclusive chunk refs, external payload refs and a cold
    /// snapshot body (F16). Pages hold the rest; the orphan sweep reads them
    /// separately.
    pub fn stream_referenced_cold_paths(&self, stream_id: &BucketStreamId) -> Vec<String> {
        let Some(slot) = self.stream_slot(stream_id) else {
            return Vec::new();
        };
        slot.cold
            .cold_chunks()
            .iter()
            .map(|chunk| chunk.s3_path.clone())
            .chain(
                slot.cold
                    .external_segments()
                    .iter()
                    .map(|object| object.s3_path.clone()),
            )
            .chain(
                slot.visible_snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.object.as_ref())
                    .map(|object| object.s3_path.clone()),
            )
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slice(start: u64, end: u64, path: &str) -> ColdChunkRef {
        ColdChunkRef {
            start_offset: start,
            end_offset: end,
            s3_path: path.to_owned(),
            object_size: 1_000,
            object_offset: 0,
            shared_object: true,
            payload_digest: String::new(),
        }
    }

    #[test]
    fn run_takes_the_oldest_contiguous_prefix_within_the_byte_cap() {
        let refs = vec![
            slice(10, 20, "p2"),
            slice(0, 10, "p1"),
            slice(20, 30, "p3"),
            slice(40, 50, "p4"),
        ];
        let run = plan_shared_ref_run(&refs, 1_000);
        assert_eq!(
            run.iter().map(|c| c.start_offset).collect::<Vec<_>>(),
            vec![0, 10, 20],
            "the run stops at the first gap"
        );
        let run = plan_shared_ref_run(&refs, 15);
        assert_eq!(run.len(), 1, "the byte cap ends the run");
        let run = plan_shared_ref_run(&refs, 5);
        assert_eq!(
            run.len(),
            1,
            "an oversized first slice is still planned alone"
        );
        assert!(plan_shared_ref_run(&[], 10).is_empty());
    }

    #[test]
    fn idle_tracker_restarts_when_the_tail_moves() {
        let mut tracker = SharedRefIdleTracker::default();
        let id = BucketStreamId::new("b", "s");
        assert_eq!(tracker.observe(&id, 10, 1_000), 0);
        assert_eq!(tracker.observe(&id, 10, 5_000), 4_000);
        assert_eq!(tracker.observe(&id, 11, 6_000), 0);
        assert_eq!(tracker.observe(&id, 11, 9_000), 3_000);
        assert_eq!(tracker.len(), 1);
    }
}
