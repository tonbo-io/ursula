//! The cold orphan sweep (bounded-stream-state F14h, D5).
//!
//! Publishes with ambiguous outcomes keep their objects on purpose: a chunk,
//! a pack or a staged external payload may have been committed although the
//! caller saw an error, so deleting it eagerly could corrupt acknowledged
//! data. Nothing used to reclaim the ones that were not committed, and packs
//! live outside every stream prefix.
//!
//! A per-group leader job walks the group's streams with a cursor, a bounded
//! number per step. At the start of each cycle it lists the group's pack
//! directory in every bucket; for each stream it lists the live
//! incarnation's chunk directory and the stream's external directory. It
//! deletes an object only when all of these hold:
//!
//! - its name is one Ursula writes there, carrying its write time;
//! - it is older than the grace (a day by default), so no publish of it can
//!   still be in flight;
//! - no state ref, cold-index page of the live incarnation, live shared
//!   object or pending cold-GC entry names it (pending GC entries keep their
//!   own grace for reads planned before a release).
//!
//! Objects are listed before the references are read, and only objects older
//! than the grace are candidates, so a reference added after the plan can
//! only name a newer object.
//!
//! RT6 guard: an unreferenced exclusive chunk is still kept when part of its
//! range lies in the stream's cold range (retained bytes below the hot
//! buffer) and no referenced object covers that part. Its page entry was
//! lost (a page read-modify-write by a deposed leader, or a rollback after an
//! ambiguous redirect), so it may hold the only copy of committed bytes. The
//! sweep logs an error and counts `cold_orphan_uncovered_chunks_kept` for an
//! operator to repair the stream's cold index.
//!
//! External payloads carry no range in their name, so the same guard works
//! per stream: an unreferenced external payload of a stream is deleted only
//! when the stream's retained bytes `[retained, tail)` are fully covered by
//! its hot buffer, state refs and page entries. The offload pass moves
//! external refs into pages (F5), so a lost page entry would otherwise make
//! the sole payload look like an orphan. While coverage has a
//! gap, every unreferenced external payload of the stream is kept, counted
//! and logged as an error.

use std::collections::BTreeSet;

use ursula_shard::RaftGroupId;

use super::ShardRuntime;
use super::list_cold_index_page_ids;
use super::unix_time_ms;
use crate::cold_index::ColdIndexPageKey;
use crate::cold_index::ColdIndexPageStore;
use crate::cold_index::ColdStoreColdIndexPageStore;
use crate::cold_refs::ColdOrphanSweepReport;
use crate::cold_refs::ColdOrphanSweepRequest;
use crate::cold_refs::cold_object_written_unix_ms;
use crate::cold_refs::ranges_cover;
use crate::cold_store::ColdStoreHandle;
use crate::cold_store::cold_chunk_dir;
use crate::cold_store::cold_chunk_file_range;
use crate::cold_store::cold_external_dir;
use crate::cold_store::is_cold_chunk_file_name;
use crate::cold_store::is_external_payload_file_name;
use crate::error::RuntimeError;

/// A live chunk directory, its stream's cold range and the byte ranges its
/// referenced objects cover (RT6).
type ChunkGuard = (String, (u64, u64), Vec<(u64, u64)>);

/// A stream's external directory and, when its retained bytes are not fully
/// covered, the first uncovered range (RT6).
type ExternalGuard = (String, Option<(u64, u64)>);

/// Default age below which the sweep never deletes an object.
pub const COLD_ORPHAN_SWEEP_GRACE_MS: u64 = 24 * 60 * 60 * 1_000;

fn cold_io(err: std::io::Error) -> RuntimeError {
    RuntimeError::ColdStoreIo {
        message: err.to_string(),
    }
}

impl ShardRuntime {
    /// One orphan-sweep step for one group: sweeps up to `max_streams`
    /// streams after the group's cursor (and the group's pack directories at
    /// the start of a cycle), deleting unreferenced objects written more than
    /// `grace_ms` ago. On a follower it deletes nothing and restarts the
    /// cursor.
    pub async fn sweep_cold_orphans_group_once(
        &self,
        raft_group_id: RaftGroupId,
        max_streams: usize,
        grace_ms: u64,
    ) -> Result<ColdOrphanSweepReport, RuntimeError> {
        self.run_group_work(raft_group_id, move |runtime| async move {
            runtime
                .sweep_cold_orphans_group_once_admitted(raft_group_id, max_streams, grace_ms)
                .await
        })
        .await?
    }

    async fn sweep_cold_orphans_group_once_admitted(
        &self,
        raft_group_id: RaftGroupId,
        max_streams: usize,
        grace_ms: u64,
    ) -> Result<ColdOrphanSweepReport, RuntimeError> {
        let Some(cold_store) = self.cold_store.as_ref() else {
            return Ok(ColdOrphanSweepReport::default());
        };
        let after = self
            .cold_orphan_sweep
            .lock()
            .map_err(|_| RuntimeError::ColdStoreConfig {
                message: "cold orphan sweep cursor lock poisoned".to_owned(),
            })?
            .get(&raft_group_id)
            .cloned()
            .flatten();

        // List first: every candidate object exists before the references
        // are read below.
        let request_after = after.clone();
        let listing_plan = self
            .plan_cold_orphan_sweep(raft_group_id, ColdOrphanSweepRequest {
                after: request_after,
                max_streams: max_streams.max(1),
            })
            .await?;
        if !listing_plan.leader {
            self.set_cold_orphan_sweep_cursor(raft_group_id, None)?;
            return Ok(ColdOrphanSweepReport::default());
        }
        let cutoff_ms = unix_time_ms().saturating_sub(grace_ms);
        let mut aged = Vec::new();
        for dir in &listing_plan.pack_dirs {
            for name in cold_store.list_file_names(dir).await.map_err(cold_io)? {
                if is_external_payload_file_name(&name) {
                    aged.push((format!("{dir}{name}"), name));
                }
            }
        }
        for stream in &listing_plan.streams {
            let chunk_dir = cold_chunk_dir(&stream.stream_id, stream.generation);
            for name in cold_store
                .list_file_names(&chunk_dir)
                .await
                .map_err(cold_io)?
            {
                if is_cold_chunk_file_name(&name) {
                    aged.push((format!("{chunk_dir}{name}"), name));
                }
            }
            let external_dir = cold_external_dir(&stream.stream_id);
            for name in cold_store
                .list_file_names(&external_dir)
                .await
                .map_err(cold_io)?
            {
                if is_external_payload_file_name(&name) {
                    aged.push((format!("{external_dir}{name}"), name));
                }
            }
        }
        let objects_scanned = u64::try_from(aged.len()).unwrap_or(u64::MAX);
        aged.retain(|(_, name)| {
            cold_object_written_unix_ms(name).is_some_and(|written_ms| written_ms < cutoff_ms)
        });

        // Then read what state and pages reference now.
        let mut referenced = BTreeSet::new();
        // Per live chunk directory: the stream's cold range and the ranges
        // its referenced objects cover (RT6).
        let mut chunk_guards: Vec<ChunkGuard> = Vec::new();
        let mut external_guards: Vec<ExternalGuard> = Vec::new();
        if !aged.is_empty() {
            let plan = self
                .plan_cold_orphan_sweep(raft_group_id, ColdOrphanSweepRequest {
                    after,
                    max_streams: max_streams.max(1),
                })
                .await?;
            if !plan.leader {
                self.set_cold_orphan_sweep_cursor(raft_group_id, None)?;
                return Ok(ColdOrphanSweepReport::default());
            }
            referenced.extend(plan.group_referenced);
            for stream in &listing_plan.streams {
                // A stream that left the plan in between is skipped: its
                // objects now belong to its stream GC entry.
                let Some(current) = plan
                    .streams
                    .iter()
                    .find(|current| current.stream_id == stream.stream_id)
                else {
                    let chunk_dir = cold_chunk_dir(&stream.stream_id, stream.generation);
                    let external_dir = cold_external_dir(&stream.stream_id);
                    aged.retain(|(path, _)| {
                        !path.starts_with(&chunk_dir) && !path.starts_with(&external_dir)
                    });
                    continue;
                };
                referenced.extend(current.referenced.iter().cloned());
                let mut covered = current.referenced_ranges.clone();
                for generation in [stream.generation, current.generation] {
                    let (paths, ranges) =
                        page_references(cold_store, &stream.stream_id, generation).await?;
                    referenced.extend(paths);
                    covered.extend(ranges);
                }
                let mut retained_covered = covered.clone();
                retained_covered.extend(current.hot_ranges.iter().copied());
                let (retained_start, retained_end) = current.retained_range;
                external_guards.push((
                    cold_external_dir(&stream.stream_id),
                    first_gap(&retained_covered, retained_start, retained_end),
                ));
                chunk_guards.push((
                    cold_chunk_dir(&stream.stream_id, stream.generation),
                    current.cold_range,
                    covered,
                ));
            }
        }

        let mut report = ColdOrphanSweepReport {
            streams_scanned: u64::try_from(listing_plan.streams.len()).unwrap_or(u64::MAX),
            objects_scanned,
            ..ColdOrphanSweepReport::default()
        };
        for (path, name) in aged {
            if referenced.contains(&path) {
                continue;
            }
            if let Some((chunk_start, chunk_end)) = cold_chunk_file_range(&name)
                && let Some((_, (cold_start, cold_end), covered)) = chunk_guards
                    .iter()
                    .find(|(dir, _, _)| path.strip_prefix(dir.as_str()) == Some(name.as_str()))
            {
                let start = chunk_start.max(*cold_start);
                let end = chunk_end.min(*cold_end);
                if start < end && !ranges_cover(covered, start, end) {
                    self.metrics.record_cold_orphan_uncovered_chunk_kept();
                    report.uncovered_chunks_kept = report.uncovered_chunks_kept.saturating_add(1);
                    tracing::error!(
                        path = %path,
                        uncovered_start = start,
                        uncovered_end = end,
                        "unreferenced cold chunk holds retained bytes no index entry covers; \
                         keeping it (lost cold-index page entry, repair the stream)"
                    );
                    continue;
                }
            }
            if is_external_payload_file_name(&name)
                && let Some((_, Some((gap_start, gap_end)))) = external_guards
                    .iter()
                    .find(|(dir, _)| path.strip_prefix(dir.as_str()) == Some(name.as_str()))
            {
                self.metrics.record_cold_orphan_uncovered_chunk_kept();
                report.uncovered_chunks_kept = report.uncovered_chunks_kept.saturating_add(1);
                tracing::error!(
                    path = %path,
                    uncovered_start = gap_start,
                    uncovered_end = gap_end,
                    "unreferenced external payload of a stream whose retained bytes no index \
                     entry covers; keeping it (lost cold-index page entry, repair the stream)"
                );
                continue;
            }
            let bytes = cold_store.object_size(&path).await.unwrap_or(0);
            match cold_store.delete_chunk(&path).await {
                Ok(()) => {
                    self.metrics.record_cold_orphan_cleanup(bytes, false);
                    report.orphans_deleted = report.orphans_deleted.saturating_add(1);
                    report.orphan_bytes = report.orphan_bytes.saturating_add(bytes);
                    tracing::info!(path = %path, bytes, "deleted unreferenced cold object");
                }
                Err(err) => {
                    self.metrics.record_cold_orphan_cleanup(0, true);
                    report.delete_errors = report.delete_errors.saturating_add(1);
                    tracing::warn!(
                        path = %path,
                        error = %err,
                        "failed to delete unreferenced cold object"
                    );
                }
            }
        }
        report.cycle_completed = listing_plan.next_after.is_none();
        self.set_cold_orphan_sweep_cursor(raft_group_id, listing_plan.next_after)?;
        Ok(report)
    }

    /// One orphan-sweep step in every group. A failing group is logged and
    /// skipped, so it cannot stall the others.
    pub async fn sweep_cold_orphans_all_groups_once(
        &self,
        max_streams_per_group: usize,
        grace_ms: u64,
    ) -> ColdOrphanSweepReport {
        let mut report = ColdOrphanSweepReport::default();
        if self.cold_store.is_none() {
            return report;
        }
        for group_id in 0..self.shard_map.raft_group_count() {
            match self
                .sweep_cold_orphans_group_once(
                    RaftGroupId(group_id),
                    max_streams_per_group,
                    grace_ms,
                )
                .await
            {
                Ok(step) => report.add(&step),
                Err(err) => tracing::warn!(
                    raft_group_id = group_id,
                    error = %err,
                    "cold orphan sweep step failed; continuing with remaining groups"
                ),
            }
        }
        report
    }

    fn set_cold_orphan_sweep_cursor(
        &self,
        raft_group_id: RaftGroupId,
        after: Option<ursula_shard::BucketStreamId>,
    ) -> Result<(), RuntimeError> {
        self.cold_orphan_sweep
            .lock()
            .map_err(|_| RuntimeError::ColdStoreConfig {
                message: "cold orphan sweep cursor lock poisoned".to_owned(),
            })?
            .insert(raft_group_id, after);
        Ok(())
    }
}

/// The first part of `[start, end)` that `ranges` leave uncovered, if any.
fn first_gap(ranges: &[(u64, u64)], start: u64, end: u64) -> Option<(u64, u64)> {
    if start >= end || ranges_cover(ranges, start, end) {
        return None;
    }
    let mut sorted = ranges
        .iter()
        .copied()
        .filter(|(range_start, range_end)| range_start < range_end)
        .collect::<Vec<_>>();
    sorted.sort_unstable();
    let mut cursor = start;
    for (range_start, range_end) in sorted {
        if range_start > cursor {
            return Some((cursor, range_start.min(end)));
        }
        cursor = cursor.max(range_end);
        if cursor >= end {
            return None;
        }
    }
    Some((cursor, end))
}

/// Every object path the pages of one stream generation reference, and the
/// byte ranges those entries cover.
async fn page_references(
    cold_store: &ColdStoreHandle,
    stream_id: &ursula_shard::BucketStreamId,
    generation: u64,
) -> Result<(BTreeSet<String>, Vec<(u64, u64)>), RuntimeError> {
    let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
    let mut paths = BTreeSet::new();
    let mut ranges = Vec::new();
    for page_id in list_cold_index_page_ids(cold_store, stream_id, generation)
        .await
        .map_err(cold_io)?
    {
        let key = ColdIndexPageKey {
            stream_id: stream_id.clone(),
            generation,
            page_id,
        };
        let Some(page) = store.get_page(&key).await.map_err(cold_io)? else {
            continue;
        };
        for chunk in &page.cold_chunks {
            paths.insert(chunk.s3_path.clone());
            ranges.push((chunk.start_offset, chunk.end_offset));
        }
        for object in &page.external_segments {
            paths.insert(object.s3_path.clone());
            ranges.push((object.start_offset, object.end_offset));
        }
    }
    Ok((paths, ranges))
}
