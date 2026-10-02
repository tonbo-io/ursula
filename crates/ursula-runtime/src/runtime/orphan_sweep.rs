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
use crate::cold_store::ColdStoreHandle;
use crate::cold_store::cold_chunk_dir;
use crate::cold_store::cold_external_dir;
use crate::cold_store::is_cold_chunk_file_name;
use crate::cold_store::is_external_payload_file_name;
use crate::error::RuntimeError;

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
                for generation in [stream.generation, current.generation] {
                    referenced.extend(
                        page_referenced_paths(cold_store, &stream.stream_id, generation).await?,
                    );
                }
            }
        }

        let mut report = ColdOrphanSweepReport {
            streams_scanned: u64::try_from(listing_plan.streams.len()).unwrap_or(u64::MAX),
            objects_scanned,
            ..ColdOrphanSweepReport::default()
        };
        for (path, _) in aged {
            if referenced.contains(&path) {
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

/// Every object path the pages of one stream generation reference.
async fn page_referenced_paths(
    cold_store: &ColdStoreHandle,
    stream_id: &ursula_shard::BucketStreamId,
    generation: u64,
) -> Result<BTreeSet<String>, RuntimeError> {
    let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
    let mut paths = BTreeSet::new();
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
        paths.extend(page.cold_chunks.iter().map(|chunk| chunk.s3_path.clone()));
        paths.extend(
            page.external_segments
                .iter()
                .map(|object| object.s3_path.clone()),
        );
    }
    Ok(paths)
}
