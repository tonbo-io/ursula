//! The shared pack-reference compaction driver (bounded-stream-state F2,
//! §5.3, step 2).
//!
//! Every packed flush adds one shared ref per participating stream, and only
//! retention or delete used to remove them. The driver runs on each group's
//! leader every compaction interval:
//!
//! 1. **Discovery** is the state query `shared_ref_candidates`: streams with
//!    at least T = 64 shared refs, or with one and a tail idle for an hour,
//!    fewest live pack slices first. No group snapshot, no object listing.
//! 2. **Repair** the stream's cold-index pages (F19), because turning refs
//!    into a page entry uncovers whatever the refs hid.
//! 3. **Execute** the planned run (the oldest contiguous shared refs, at most
//!    `compaction_max_size`): range-read each slice from its pack without the
//!    read cache, write one exclusive chunk, and publish `CompactCold` with
//!    `gc_not_before = now + compaction_gc_grace`. Releasing a pack's last
//!    ref queues it for GC after the grace.
//! 4. **Failures.** A definite rejection (typed stream error or a redirect
//!    before proposal) rolls the page entry back in the engine, and the
//!    driver deletes the replacement. An ambiguous outcome keeps both; the
//!    orphan sweep (F14h) reclaims the replacement if nothing references it,
//!    and a retried compaction's page write clips the earlier entry.
//!
//! The legacy-pack migration that bucket purge runs (#278) is the same
//! driver with the legacy-pack filter
//! ([`SharedRefCompactionRequest::legacy_packs`]): it compacts runs of shared
//! slices of pre-erasure-domain packs instead of cloning every group's state.

use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;
use ursula_stream::ColdChunkRef;
use ursula_stream::SharedRefCompactionRequest;

use super::ShardRuntime;
use super::unix_time_ms;
use crate::cold_index::RepairColdIndexRequest;
use crate::cold_refs::SharedRefCompactionConfig;
use crate::cold_refs::SharedRefCompactionReport;
use crate::cold_store::ColdStoreHandle;
use crate::cold_store::new_cold_chunk_path_in_generation;
use crate::error::RuntimeError;
use crate::request::CompactColdRequest;
use crate::runtime::LegacySharedMigrationReport;

/// Upper bound on the logical bytes of one legacy-pack migration run (the
/// default `compaction_max_size`).
const LEGACY_MIGRATION_MAX_RUN_BYTES: u64 = 16 * 1024 * 1024;

/// What happened to one planned run. Rejections and ambiguous outcomes carry
/// the publish error, which the legacy migration returns to its caller.
enum SharedRunOutcome {
    Compacted { slices: u64, bytes: u64 },
    Rejected(RuntimeError),
    Ambiguous(RuntimeError),
}

impl ShardRuntime {
    /// One F2 driver pass over one group. Does nothing unless this node leads
    /// the group. A stream whose compaction fails is logged and skipped; an
    /// ambiguous outcome ends the group's pass, since it usually means the
    /// leadership moved.
    pub async fn compact_shared_refs_group_once(
        &self,
        raft_group_id: RaftGroupId,
        config: &SharedRefCompactionConfig,
    ) -> Result<SharedRefCompactionReport, RuntimeError> {
        let mut report = SharedRefCompactionReport::default();
        let Some(cold_store) = self.cold_store.as_ref() else {
            return Ok(report);
        };
        let candidates = self
            .plan_shared_ref_compaction(raft_group_id, SharedRefCompactionRequest {
                min_refs: config.min_refs,
                idle_ms: config.idle_ms,
                now_ms: unix_time_ms(),
                max_run_bytes: config.max_run_bytes,
                limit: config.max_streams.max(1),
                legacy_packs_only: false,
            })
            .await?;
        report.candidates = u64::try_from(candidates.len()).unwrap_or(u64::MAX);
        for candidate in candidates {
            self.repair_cold_index(raft_group_id, RepairColdIndexRequest {
                after: None,
                max_streams: 1,
                stream: Some(candidate.stream_id.clone()),
            })
            .await?;
            let outcome = self
                .compact_shared_run(
                    cold_store,
                    &candidate.stream_id,
                    candidate.cold_generation,
                    candidate.run,
                    config.gc_grace_ms,
                )
                .await;
            match outcome {
                Ok(SharedRunOutcome::Compacted { slices, bytes }) => {
                    report.compacted_streams = report.compacted_streams.saturating_add(1);
                    report.compacted_slices = report.compacted_slices.saturating_add(slices);
                    report.compacted_bytes = report.compacted_bytes.saturating_add(bytes);
                }
                Ok(SharedRunOutcome::Rejected(_)) => {
                    report.rejected = report.rejected.saturating_add(1);
                }
                Ok(SharedRunOutcome::Ambiguous(_)) => {
                    report.ambiguous = report.ambiguous.saturating_add(1);
                    break;
                }
                Err(err) => {
                    report.errors = report.errors.saturating_add(1);
                    tracing::warn!(
                        stream = %candidate.stream_id,
                        error = %err,
                        "shared-ref compaction failed; continuing with remaining streams"
                    );
                }
            }
        }
        Ok(report)
    }

    /// One F2 driver pass in every group. A failing group is logged and
    /// skipped, so it cannot stall the others.
    pub async fn compact_shared_refs_all_groups_once(
        &self,
        config: &SharedRefCompactionConfig,
    ) -> SharedRefCompactionReport {
        let mut report = SharedRefCompactionReport::default();
        if self.cold_store.is_none() {
            return report;
        }
        for group_id in 0..self.shard_map.raft_group_count() {
            match self
                .compact_shared_refs_group_once(RaftGroupId(group_id), config)
                .await
            {
                Ok(group_report) => report.add(&group_report),
                Err(err) => tracing::warn!(
                    raft_group_id = group_id,
                    error = %err,
                    "shared-ref compaction pass failed; continuing with remaining groups"
                ),
            }
        }
        report
    }

    /// Rewrites a bounded number of pre-erasure-domain shared pack slices as
    /// stream-exclusive objects (#278), through the F2 driver with the
    /// legacy-pack filter: discovery is the `shared_ref_candidates` state
    /// query (read on followers too, so the global debt is counted on every
    /// node), and each stream's oldest contiguous run of legacy slices becomes
    /// one exclusive chunk published with `CompactCold`. At most `max_chunks`
    /// slices are rewritten per call. A failed publish is returned (a
    /// redirect lets bucket purge forward to the leader); a definite
    /// rejection deletes the replacement and an ambiguous outcome keeps it for
    /// the orphan sweep.
    pub async fn migrate_legacy_shared_cold_once(
        &self,
        max_chunks: usize,
        gc_grace_ms: u64,
    ) -> Result<LegacySharedMigrationReport, RuntimeError> {
        let Some(cold_store) = self.cold_store.as_ref() else {
            return Ok(LegacySharedMigrationReport::default());
        };
        let mut candidates = Vec::new();
        for group_id in 0..self.shard_map.raft_group_count() {
            candidates.extend(
                self.plan_shared_ref_compaction(
                    RaftGroupId(group_id),
                    SharedRefCompactionRequest::legacy_packs(
                        LEGACY_MIGRATION_MAX_RUN_BYTES,
                        usize::MAX,
                    ),
                )
                .await?,
            );
        }
        let observed_chunks = candidates.iter().fold(0_usize, |total, candidate| {
            total.saturating_add(candidate.shared_refs)
        });
        let mut migrated_chunks = 0_usize;
        for mut candidate in candidates {
            let budget = max_chunks.saturating_sub(migrated_chunks);
            if budget == 0 {
                break;
            }
            candidate.run.truncate(budget);
            let slices = candidate.run.len();
            let raft_group_id = self.locate(&candidate.stream_id).raft_group_id;
            self.repair_cold_index(raft_group_id, RepairColdIndexRequest {
                after: None,
                max_streams: 1,
                stream: Some(candidate.stream_id.clone()),
            })
            .await?;
            match self
                .compact_shared_run(
                    cold_store,
                    &candidate.stream_id,
                    candidate.cold_generation,
                    candidate.run,
                    gc_grace_ms,
                )
                .await?
            {
                SharedRunOutcome::Compacted { .. } => {
                    migrated_chunks = migrated_chunks.saturating_add(slices);
                }
                SharedRunOutcome::Rejected(err) | SharedRunOutcome::Ambiguous(err) => {
                    return Err(err);
                }
            }
        }
        Ok(LegacySharedMigrationReport {
            observed_chunks,
            migrated_chunks,
            pending_chunks: observed_chunks.saturating_sub(migrated_chunks),
        })
    }

    /// Rewrites one contiguous run of a stream's shared slices into one
    /// exclusive chunk and publishes it with `CompactCold`. A read or write
    /// error before the publish leaves state untouched.
    async fn compact_shared_run(
        &self,
        cold_store: &ColdStoreHandle,
        stream_id: &BucketStreamId,
        generation: u64,
        run: Vec<ColdChunkRef>,
        gc_grace_ms: u64,
    ) -> Result<SharedRunOutcome, RuntimeError> {
        let (Some(first), Some(last)) = (run.first(), run.last()) else {
            return Ok(SharedRunOutcome::Rejected(RuntimeError::ColdStoreIo {
                message: "shared-ref compaction run is empty".to_owned(),
            }));
        };
        let (start_offset, end_offset) = (first.start_offset, last.end_offset);
        let total_bytes = end_offset.saturating_sub(start_offset);
        let capacity = usize::try_from(total_bytes).map_err(|_| RuntimeError::ColdStoreIo {
            message: "shared-ref compaction run exceeds addressable memory".to_owned(),
        })?;
        let mut payload = Vec::with_capacity(capacity);
        for slice in &run {
            let len = usize::try_from(slice.end_offset.saturating_sub(slice.start_offset))
                .map_err(|_| RuntimeError::ColdStoreIo {
                    message: "shared slice exceeds addressable memory".to_owned(),
                })?;
            let bytes = cold_store
                .read_chunk_range_uncached(slice, slice.start_offset, len)
                .await
                .map_err(|err| RuntimeError::ColdStoreIo {
                    message: err.to_string(),
                })?;
            if !slice.payload_digest.is_empty()
                && blake3::hash(&bytes).to_hex().as_str() != slice.payload_digest
            {
                return Err(RuntimeError::ColdStoreIo {
                    message: format!(
                        "shared slice [{}..{}) of '{}' does not match its digest",
                        slice.start_offset, slice.end_offset, slice.s3_path
                    ),
                });
            }
            payload.extend_from_slice(&bytes);
        }
        let path =
            new_cold_chunk_path_in_generation(stream_id, generation, start_offset, end_offset);
        let object_size = cold_store
            .write_chunk(&path, &payload)
            .await
            .map_err(|err| RuntimeError::ColdStoreIo {
                message: err.to_string(),
            })?;
        let slices = u64::try_from(run.len()).unwrap_or(u64::MAX);
        let replacement = ColdChunkRef {
            start_offset,
            end_offset,
            s3_path: path.clone(),
            object_size,
            object_offset: 0,
            shared_object: false,
            payload_digest: blake3::hash(&payload).to_hex().to_string(),
        };
        let result = self
            .compact_cold(CompactColdRequest {
                stream_id: stream_id.clone(),
                old_chunks: run,
                replacement,
                gc_not_before_ms: unix_time_ms().saturating_add(gc_grace_ms),
            })
            .await;
        match result {
            Ok(_) => Ok(SharedRunOutcome::Compacted {
                slices,
                bytes: total_bytes,
            }),
            Err(err) if err.stream_error_code().is_some() || err.leader_hint().is_some() => {
                // Definitely not committed: the engine rolled the page entry
                // back, so nothing references the replacement.
                if let Err(cleanup_err) = cold_store.delete_chunk(&path).await {
                    tracing::warn!(
                        stream = %stream_id,
                        path = %path,
                        error = %cleanup_err,
                        "failed to remove a rejected shared-ref compaction replacement"
                    );
                }
                tracing::info!(
                    stream = %stream_id,
                    error = %err,
                    "shared-ref compaction rejected"
                );
                Ok(SharedRunOutcome::Rejected(err))
            }
            Err(err) => {
                tracing::warn!(
                    stream = %stream_id,
                    path = %path,
                    error = %err,
                    "shared-ref compaction outcome is ambiguous; keeping its replacement"
                );
                Ok(SharedRunOutcome::Ambiguous(err))
            }
        }
    }
}
