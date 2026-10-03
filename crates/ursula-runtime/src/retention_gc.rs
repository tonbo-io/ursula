//! Retention GC (bounded-stream-state F14f).
//!
//! From feature level 1 a stream's exclusive cold chunks and its offloaded
//! external payloads are referenced only by its cold-index pages in S3, not
//! by replicated state. `AdvanceRetention` therefore could release only the
//! shared pack slices state holds; every exclusive object below the retained
//! offset stayed in S3, still named by its page entry, so not even the
//! orphan sweep (F14h) reclaimed it.
//!
//! The leader's cold-index repair cursor now collects them. For each stream
//! it visits, [`RetentionGcTracker`] notes the retained offset and when this
//! leader first saw it; once that observation is older than the retention
//! grace ([`ursula_stream::RETENTION_COLD_GC_GRACE_MS`], the grace apply
//! gives the pack slices that retention drops), a read planned before the
//! retention can no longer be in flight, and
//! [`collect_retained_cold_objects`] removes every page entry that lies
//! wholly below that offset, deletes pages left with nothing at or above
//! it, and then deletes the entries' objects. Pages go first, so a crash
//! in between leaves unreferenced objects for the orphan sweep rather than
//! entries naming deleted objects.
//!
//! The tracker is leader-local and not replicated: a new leader starts it
//! empty and waits a full grace again, which only delays collection. Page
//! writes run inside the group actor, like the repair pass and flush page
//! writes, so they never interleave with another page read-modify-write.

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::io;

use ursula_shard::BucketStreamId;

use crate::cold_index::ColdIndexPageCache;
use crate::cold_index::ColdIndexPageKey;
use crate::cold_index::ColdIndexPageStore;
use crate::cold_index::ColdIndexRepairInput;
use crate::cold_index::ColdStoreColdIndexPageStore;
use crate::cold_index::cold_index_generation_dir;
use crate::cold_index::parse_cold_index_page_file_name;
use crate::cold_store::ColdStoreHandle;

/// One stream incarnation's cold objects to collect below `collect_below`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionGcTarget {
    pub stream_id: BucketStreamId,
    pub generation: u64,
    pub created_at_ms: u64,
    /// Entries ending at or below this offset are collected. It is a
    /// retained offset this leader observed at least a grace ago.
    pub collect_below: u64,
    /// Objects state still references (shared pack slices, direct external
    /// refs): never deleted here.
    pub keep_paths: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetentionGcReport {
    pub pages_deleted: u64,
    pub pages_rewritten: u64,
    pub objects_deleted: u64,
}

type IncarnationKey = (BucketStreamId, u64, u64);

#[derive(Debug, Clone, Copy, Default)]
struct RetentionMark {
    /// Retained offset whose grace has elapsed.
    eligible: u64,
    /// Offset up to which collection has completed.
    collected: u64,
    /// Newer retained offset and when this leader first saw it.
    pending: Option<(u64, u64)>,
}

/// Leader-local grace clock of retention GC. Not replicated.
#[derive(Debug, Clone, Default)]
pub struct RetentionGcTracker {
    marks: HashMap<IncarnationKey, RetentionMark>,
}

impl RetentionGcTracker {
    /// Records `input`'s retained offset as seen at `now_ms` and returns the
    /// collection now due for it, if any.
    pub fn observe(
        &mut self,
        input: &ColdIndexRepairInput,
        now_ms: u64,
        grace_ms: u64,
    ) -> Option<RetentionGcTarget> {
        if input.retained_offset == 0 {
            return None;
        }
        let key = (
            input.stream_id.clone(),
            input.generation,
            input.created_at_ms,
        );
        let mark = self.marks.entry(key).or_default();
        if input.retained_offset < mark.eligible {
            // Retention never moves back within an incarnation; start over.
            *mark = RetentionMark::default();
        }
        if let Some((offset, seen_ms)) = mark.pending
            && now_ms >= seen_ms.saturating_add(grace_ms)
        {
            mark.eligible = mark.eligible.max(offset);
            mark.pending = None;
        }
        if mark.pending.is_none() && input.retained_offset > mark.eligible {
            mark.pending = Some((input.retained_offset, now_ms));
        }
        (mark.eligible > mark.collected).then(|| RetentionGcTarget {
            stream_id: input.stream_id.clone(),
            generation: input.generation,
            created_at_ms: input.created_at_ms,
            collect_below: mark.eligible,
            keep_paths: input
                .state_refs
                .iter()
                .map(|object| object.s3_path.clone())
                .collect(),
        })
    }

    /// Records that `target` was collected.
    pub fn collected(&mut self, target: &RetentionGcTarget) {
        let key = (
            target.stream_id.clone(),
            target.generation,
            target.created_at_ms,
        );
        if let Some(mark) = self.marks.get_mut(&key) {
            mark.collected = mark.collected.max(target.collect_below);
        }
    }

    /// Drops the marks of incarnations `live` no longer accepts.
    pub fn retain(&mut self, mut live: impl FnMut(&BucketStreamId, u64) -> bool) {
        self.marks
            .retain(|(stream_id, _, created_at_ms), _| live(stream_id, *created_at_ms));
    }

    pub fn len(&self) -> usize {
        self.marks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.marks.is_empty()
    }
}

/// Collects each target's cold objects below its `collect_below`: rewrites
/// or deletes its pages first, then deletes the objects only those entries
/// named. Returns the targets that completed.
pub async fn collect_retained_cold_objects(
    cold_store: &ColdStoreHandle,
    cache: Option<&ColdIndexPageCache<ColdStoreColdIndexPageStore>>,
    targets: &[RetentionGcTarget],
) -> (RetentionGcReport, Vec<RetentionGcTarget>) {
    let mut report = RetentionGcReport::default();
    let mut completed = Vec::with_capacity(targets.len());
    for target in targets {
        match collect_stream(cold_store, target, &mut report).await {
            Ok(changed) => {
                if changed && let Some(cache) = cache {
                    cache.invalidate_stream(&target.stream_id);
                }
                completed.push(target.clone());
            }
            Err(err) => {
                if let Some(cache) = cache {
                    cache.invalidate_stream(&target.stream_id);
                }
                tracing::warn!(
                    stream = %target.stream_id,
                    collect_below = target.collect_below,
                    error = %err,
                    "retention gc failed; retrying on a later pass"
                );
            }
        }
    }
    if report.objects_deleted > 0 || report.pages_deleted > 0 {
        tracing::info!(
            objects_deleted = report.objects_deleted,
            pages_deleted = report.pages_deleted,
            pages_rewritten = report.pages_rewritten,
            "retention gc reclaimed cold objects below retained offsets"
        );
    }
    (report, completed)
}

async fn collect_stream(
    cold_store: &ColdStoreHandle,
    target: &RetentionGcTarget,
    report: &mut RetentionGcReport,
) -> io::Result<bool> {
    let span = ursula_stream::COLD_INDEX_PAGE_SPAN_BYTES;
    let below = target.collect_below;
    let dir = cold_index_generation_dir(&target.stream_id, target.generation);
    let mut page_ids = cold_store
        .list_file_names(&dir)
        .await?
        .iter()
        .filter_map(|name| parse_cold_index_page_file_name(name))
        .filter(|page_id| page_id.saturating_mul(span) < below)
        .collect::<Vec<_>>();
    page_ids.sort_unstable();
    let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
    let mut doomed = BTreeSet::new();
    let mut kept = target.keep_paths.iter().cloned().collect::<BTreeSet<_>>();
    let mut changed = false;
    for page_id in page_ids {
        let key = ColdIndexPageKey {
            stream_id: target.stream_id.clone(),
            generation: target.generation,
            page_id,
        };
        let Some(mut page) = store.get_page(&key).await? else {
            continue;
        };
        let before = page.cold_chunks.len() + page.external_segments.len();
        page.cold_chunks.retain(|chunk| {
            let keep = chunk.shared_object || chunk.end_offset > below;
            if keep {
                kept.insert(chunk.s3_path.clone());
            } else {
                doomed.insert(chunk.s3_path.clone());
            }
            keep
        });
        page.external_segments.retain(|object| {
            let keep = object.end_offset > below;
            if keep {
                kept.insert(object.s3_path.clone());
            } else {
                doomed.insert(object.s3_path.clone());
            }
            keep
        });
        let after = page.cold_chunks.len() + page.external_segments.len();
        if after == 0 && page_id.saturating_add(1).saturating_mul(span) <= below {
            cold_store.delete_chunk(&key.path()).await?;
            report.pages_deleted = report.pages_deleted.saturating_add(1);
            changed = true;
        } else if after < before {
            store.put_page(&key, &page).await?;
            report.pages_rewritten = report.pages_rewritten.saturating_add(1);
            changed = true;
        }
    }
    for path in doomed.difference(&kept) {
        cold_store.delete_chunk(path).await?;
        report.objects_deleted = report.objects_deleted.saturating_add(1);
    }
    Ok(changed)
}
