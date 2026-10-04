//! Value types and pure helpers for the leader-side cold-reference drivers
//! (`docs/architecture/bounded-stream-state.md`):
//!
//! - the shared pack-reference compaction driver (F2, §5.3), whose discovery
//!   is the state query [`ursula_stream::StreamStateMachine::shared_ref_candidates`];
//! - the orphan sweep (F14h, §5.15), which deletes cold objects that
//!   ambiguous publishes left behind once they are older than a grace period
//!   and nothing references them;
//! - the external-locator offload (F5, §5.6), which writes
//!   cold-index page entries for committed state-held external refs and then
//!   removes them from state with `OffloadColdRefs`.
//!
//! The drivers themselves are `ShardRuntime` methods; the group engine only
//! answers the read-only planning queries below in its group actor.

use std::collections::BTreeSet;

use ursula_shard::BucketStreamId;

/// One step of the orphan sweep's cursor over a group's streams.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColdOrphanSweepRequest {
    /// Resume after this stream id; `None` starts a new cycle, which also
    /// sweeps the group's pack prefixes.
    pub after: Option<BucketStreamId>,
    pub max_streams: usize,
}

/// What a group's applied state references, for one orphan-sweep step.
/// Empty with `leader == false` on a follower.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColdOrphanSweepPlan {
    /// The answering replica is the group's local leader.
    pub leader: bool,
    /// Pack directories (`{bucket}/_packs/{group:08x}/`) to sweep. Only set
    /// at the start of a cycle.
    pub pack_dirs: Vec<String>,
    /// Group-wide referenced paths: live shared objects and pending cold-GC
    /// targets.
    pub group_referenced: BTreeSet<String>,
    /// The streams of this step.
    pub streams: Vec<ColdOrphanSweepStream>,
    /// Where the next step resumes; `None` ends the cycle.
    pub next_after: Option<BucketStreamId>,
}

/// One live stream in an orphan-sweep step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdOrphanSweepStream {
    pub stream_id: BucketStreamId,
    /// Cold generation of the live incarnation (F14g): the sweep lists its
    /// chunk directory and reads its pages.
    pub generation: u64,
    /// Paths the stream's replicated state references directly.
    pub referenced: Vec<String>,
    /// `[retained, hot start)`: the retained bytes no longer in the hot
    /// buffer, which state refs or cold-index pages must cover (RT6).
    pub cold_range: (u64, u64),
    /// Byte ranges of the objects the stream's state references directly.
    pub referenced_ranges: Vec<(u64, u64)>,
    /// `[retained, tail)`: every retained byte of the stream. The external
    /// guard (RT6) needs it covered by the hot buffer, state refs and pages
    /// before an unreferenced external payload of the stream may go.
    pub retained_range: (u64, u64),
    /// Byte ranges the hot buffer holds (it may hold bytes between cold
    /// ranges).
    pub hot_ranges: Vec<(u64, u64)>,
}

/// Outcome of one orphan-sweep step.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColdOrphanSweepReport {
    pub streams_scanned: u64,
    pub objects_scanned: u64,
    pub orphans_deleted: u64,
    pub orphan_bytes: u64,
    pub delete_errors: u64,
    /// RT6 alert: unreferenced exclusive chunks kept because no referenced
    /// object covers their part of the stream's cold range, so their page
    /// entry was lost (a page write by a deposed leader, or a rollback after
    /// an ambiguous redirect). Deleting them would lose committed data.
    /// Also counts unreferenced external payloads kept because the stream's
    /// retained bytes are not fully covered by the hot buffer, state refs
    /// and page entries (a lost external page entry leaves the payload as
    /// the only copy).
    pub uncovered_chunks_kept: u64,
    /// The step ran on the leader and reached the end of the group's streams.
    pub cycle_completed: bool,
}

impl ColdOrphanSweepReport {
    pub fn add(&mut self, other: &Self) {
        self.streams_scanned = self.streams_scanned.saturating_add(other.streams_scanned);
        self.objects_scanned = self.objects_scanned.saturating_add(other.objects_scanned);
        self.orphans_deleted = self.orphans_deleted.saturating_add(other.orphans_deleted);
        self.orphan_bytes = self.orphan_bytes.saturating_add(other.orphan_bytes);
        self.delete_errors = self.delete_errors.saturating_add(other.delete_errors);
        self.uncovered_chunks_kept = self
            .uncovered_chunks_kept
            .saturating_add(other.uncovered_chunks_kept);
    }
}

/// Whether `ranges` cover every byte of `[start, end)`.
pub fn ranges_cover(ranges: &[(u64, u64)], start: u64, end: u64) -> bool {
    let mut sorted = ranges
        .iter()
        .copied()
        .filter(|(range_start, range_end)| range_start < range_end)
        .collect::<Vec<_>>();
    sorted.sort_unstable();
    let mut cursor = start;
    for (range_start, range_end) in sorted {
        if cursor >= end {
            break;
        }
        if range_start > cursor {
            return false;
        }
        cursor = cursor.max(range_end);
    }
    cursor >= end
}

/// Settings of the shared pack-reference compaction driver (F2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedRefCompactionConfig {
    /// Shared refs at which a stream is compacted (T = 64).
    pub min_refs: usize,
    /// Tail idle time after which a stream with any shared ref is compacted.
    pub idle_ms: u64,
    /// Largest run compacted into one chunk (`compaction_max_size`).
    pub max_run_bytes: u64,
    /// Streams compacted per group per pass
    /// (`compaction_max_streams_per_pass`).
    pub max_streams: usize,
    /// Delay before released packs may be deleted (`compaction_gc_grace`).
    pub gc_grace_ms: u64,
}

impl SharedRefCompactionConfig {
    /// The design's discovery defaults with the given limits.
    pub fn new(max_run_bytes: u64, max_streams: usize, gc_grace_ms: u64) -> Self {
        Self {
            min_refs: ursula_stream::SHARED_REF_COMPACTION_THRESHOLD,
            idle_ms: ursula_stream::SHARED_REF_IDLE_MS,
            max_run_bytes,
            max_streams,
            gc_grace_ms,
        }
    }
}

/// Outcome of one shared-ref compaction pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SharedRefCompactionReport {
    pub candidates: u64,
    pub compacted_streams: u64,
    pub compacted_slices: u64,
    pub compacted_bytes: u64,
    /// Compactions rejected definitely; their replacement was deleted.
    pub rejected: u64,
    /// Compactions with an ambiguous outcome; their replacement was kept for
    /// the orphan sweep.
    pub ambiguous: u64,
    /// Streams skipped after a read, digest or write error.
    pub errors: u64,
}

impl SharedRefCompactionReport {
    pub fn add(&mut self, other: &Self) {
        self.candidates = self.candidates.saturating_add(other.candidates);
        self.compacted_streams = self
            .compacted_streams
            .saturating_add(other.compacted_streams);
        self.compacted_slices = self.compacted_slices.saturating_add(other.compacted_slices);
        self.compacted_bytes = self.compacted_bytes.saturating_add(other.compacted_bytes);
        self.rejected = self.rejected.saturating_add(other.rejected);
        self.ambiguous = self.ambiguous.saturating_add(other.ambiguous);
        self.errors = self.errors.saturating_add(other.errors);
    }
}

/// Write time in Unix milliseconds that Ursula encodes in the names of the
/// objects it writes, or `None` for any other name:
///
/// - exclusive chunks: `{start:016x}-{end:016x}-{nanos:032x}-{seq:016x}.bin`;
/// - packs and staged external payloads: `{nanos:032x}-{seq:016x}.bin`.
///
/// A zero timestamp (the simulator's clock) also yields `None`, so the
/// orphan sweep never deletes an object whose age it cannot tell.
pub fn cold_object_written_unix_ms(file_name: &str) -> Option<u64> {
    let stem = file_name.strip_suffix(".bin")?;
    let fields = stem.split('-').collect::<Vec<_>>();
    let widths = fields.iter().map(|field| field.len()).collect::<Vec<_>>();
    let nanos_hex = match widths.as_slice() {
        [16, 16, 32, 16] => fields.get(2)?,
        [32, 16] => fields.first()?,
        _ => return None,
    };
    if !fields
        .iter()
        .all(|field| field.bytes().all(|byte| byte.is_ascii_hexdigit()))
    {
        return None;
    }
    let nanos = u128::from_str_radix(nanos_hex, 16).ok()?;
    if nanos == 0 {
        return None;
    }
    u64::try_from(nanos / 1_000_000).ok()
}

/// One leader-side external-locator offload pass over a group (F5): the
/// engine offloads every stream holding more than `max_staged_refs` state
/// external refs, or one written at least `min_age_ms` before `now_ms`, at
/// most `max_streams` streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffloadColdRefsRequest {
    pub now_ms: u64,
    pub min_age_ms: u64,
    pub max_staged_refs: usize,
    pub max_streams: usize,
}

impl OffloadColdRefsRequest {
    /// The design's triggers (T_ext = 16 refs, or a ref older than 10 s).
    pub fn new(now_ms: u64, max_streams: usize) -> Self {
        Self {
            now_ms,
            min_age_ms: ursula_stream::STAGED_EXTERNAL_REF_MAX_AGE_MS,
            max_staged_refs: ursula_stream::MAX_STAGED_EXTERNAL_REFS,
            max_streams,
        }
    }

    /// Whether `object` is due for offload: written at least `min_age_ms`
    /// ago, or of unknown age (a name Ursula did not time-stamp, or the
    /// simulator's zero clock). Offloading a committed ref early is always
    /// safe; it only costs a page write sooner.
    pub fn is_due(&self, object: &ursula_stream::ObjectPayloadRef) -> bool {
        let file_name = object
            .s3_path
            .rsplit('/')
            .next()
            .unwrap_or(object.s3_path.as_str());
        cold_object_written_unix_ms(file_name)
            .is_none_or(|written_ms| self.now_ms.saturating_sub(written_ms) >= self.min_age_ms)
    }
}

/// Outcome of one external-locator offload pass.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OffloadColdRefsResponse {
    /// Streams whose `OffloadColdRefs` committed.
    pub streams: u64,
    /// Refs removed from state.
    pub refs_offloaded: u64,
    /// Page entries the proven writes clipped (F19).
    pub page_entries_clipped: u64,
    /// Streams whose `OffloadColdRefs` was definitely rejected (deleted in
    /// between, for example). Their page entries stay: the refs were
    /// committed, so the entries are correct.
    pub rejected: u64,
}

impl OffloadColdRefsResponse {
    pub fn add(&mut self, other: &Self) {
        self.streams = self.streams.saturating_add(other.streams);
        self.refs_offloaded = self.refs_offloaded.saturating_add(other.refs_offloaded);
        self.page_entries_clipped = self
            .page_entries_clipped
            .saturating_add(other.page_entries_clipped);
        self.rejected = self.rejected.saturating_add(other.rejected);
    }
}

/// Result of one replicated `OffloadColdRefs` command.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OffloadStreamColdRefsResponse {
    pub placement: ursula_shard::ShardPlacement,
    /// Listed refs that were still in state and were removed.
    pub removed: u64,
    /// State-held external refs the stream keeps.
    pub remaining: u64,
    pub group_commit_index: u64,
}

#[cfg(test)]
mod tests {
    use super::OffloadColdRefsRequest;
    use super::cold_object_written_unix_ms;

    #[test]
    fn offload_is_due_by_name_age_or_unknown_age() {
        let request = OffloadColdRefsRequest::new(1_700_000_020_000, 8);
        let object = |name: String| ursula_stream::ObjectPayloadRef {
            start_offset: 0,
            end_offset: 1,
            s3_path: format!("b/s/external/{name}"),
            object_size: 1,
            object_offset: 0,
        };
        let named =
            |written_ms: u128| object(format!("{:032x}-{:016x}.bin", written_ms * 1_000_000, 1));
        assert!(
            request.is_due(&named(1_700_000_010_000)),
            "exactly 10 s old"
        );
        assert!(!request.is_due(&named(1_700_000_015_000)), "5 s old");
        assert!(request.is_due(&object("initial.bin".to_owned())));
        assert!(request.is_due(&named(0)), "simulator zero clock");
    }

    #[test]
    fn written_time_comes_from_ursula_object_names_only() {
        let nanos: u128 = 1_700_000_000_123 * 1_000_000;
        let chunk = format!("{:016x}-{:016x}-{nanos:032x}-{:016x}.bin", 0, 8, 7);
        let pack = format!("{nanos:032x}-{:016x}.bin", 9);
        assert_eq!(cold_object_written_unix_ms(&chunk), Some(1_700_000_000_123));
        assert_eq!(cold_object_written_unix_ms(&pack), Some(1_700_000_000_123));
        assert_eq!(
            cold_object_written_unix_ms(&format!("{:032x}-{:016x}.bin", 0, 1)),
            None,
            "the simulator's zero clock never ages"
        );
        assert_eq!(cold_object_written_unix_ms("replacement.bin"), None);
        assert_eq!(
            cold_object_written_unix_ms("00000000000000000000.idx"),
            None
        );
        assert_eq!(
            cold_object_written_unix_ms(&format!("{nanos:032x}-zzzzzzzzzzzzzzzz.bin")),
            None
        );
    }
}
