//! Retention GC (bounded-stream-state F14f).
//!
//! A stream's exclusive cold chunks and its offloaded
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
//! [`collect_retained_cold_objects`] deletes every page that lies wholly
//! below that offset and holds only entries wholly below it, then the
//! objects those entries name. Pages go first, so a crash in between leaves
//! unreferenced objects for the orphan sweep rather than entries naming
//! deleted objects.
//!
//! It never writes the boundary page (the one holding the offset) and never
//! deletes an object that page, or any page it keeps, names. Page writes are
//! unconditional PUTs and `is_leader` is checked only when a step starts, so
//! a deposed leader rewriting the boundary page could overwrite an entry a
//! new leader just flushed into it, and that entry is the chunk's only
//! reference. Pages wholly below the offset take no new entries. The cost is
//! at most about one page span (64 MiB) of objects below the offset per
//! stream, left until retention moves past that page. Conditional page PUTs
//! for repair, flush and compaction would remove the hazard generally; that
//! is a follow-up.
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
        match collect_stream(
            cold_store,
            target,
            ursula_stream::COLD_INDEX_PAGE_SPAN_BYTES,
            &mut report,
        )
        .await
        {
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
            "retention gc reclaimed cold objects below retained offsets"
        );
    }
    (report, completed)
}

async fn collect_stream(
    cold_store: &ColdStoreHandle,
    target: &RetentionGcTarget,
    span: u64,
    report: &mut RetentionGcReport,
) -> io::Result<bool> {
    let below = target.collect_below;
    let dir = cold_index_generation_dir(&target.stream_id, target.generation);
    // Pages that start below the offset: the ones wholly below it, plus the
    // boundary page, which is only read.
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
    let mut deletable_pages = Vec::new();
    for page_id in page_ids {
        let key = ColdIndexPageKey {
            stream_id: target.stream_id.clone(),
            generation: target.generation,
            page_id,
        };
        let Some(page) = store.get_page(&key).await? else {
            continue;
        };
        let paths = page
            .cold_chunks
            .iter()
            .map(|chunk| (chunk.s3_path.clone(), chunk.end_offset, chunk.shared_object))
            .chain(
                page.external_segments
                    .iter()
                    .map(|object| (object.s3_path.clone(), object.end_offset, false)),
            )
            .collect::<Vec<_>>();
        let wholly_below = page_id.saturating_add(1).saturating_mul(span) <= below;
        let all_below = paths
            .iter()
            .all(|(_, end_offset, shared)| !shared && *end_offset <= below);
        if wholly_below && all_below {
            doomed.extend(paths.into_iter().map(|(path, _, _)| path));
            deletable_pages.push(key);
        } else {
            // The boundary page (a leader may be flushing into it) and any
            // page an entry at or above the offset still needs stay as they
            // are, and so do the objects they name.
            kept.extend(paths.into_iter().map(|(path, _, _)| path));
        }
    }
    let changed = !deletable_pages.is_empty();
    for key in deletable_pages {
        cold_store.delete_chunk(&key.path()).await?;
        report.pages_deleted = report.pages_deleted.saturating_add(1);
    }
    for path in doomed.difference(&kept) {
        cold_store.delete_chunk(path).await?;
        report.objects_deleted = report.objects_deleted.saturating_add(1);
    }
    Ok(changed)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ursula_shard::BucketStreamId;
    use ursula_stream::ColdChunkRef;
    use ursula_stream::ObjectPayloadRef;

    use super::RetentionGcReport;
    use super::RetentionGcTarget;
    use super::RetentionGcTracker;
    use super::collect_stream;
    use crate::ColdIndexPage;
    use crate::ColdStore;
    use crate::cold_index::ColdIndexPageKey;
    use crate::cold_index::ColdIndexPageStore;
    use crate::cold_index::ColdIndexRepairInput;
    use crate::cold_index::ColdStoreColdIndexPageStore;

    fn chunk(start_offset: u64, end_offset: u64, s3_path: &str) -> ColdChunkRef {
        ColdChunkRef {
            start_offset,
            end_offset,
            s3_path: s3_path.to_owned(),
            object_size: end_offset.checked_sub(start_offset).unwrap(),
            ..Default::default()
        }
    }

    async fn exists(cold_store: &ColdStore, path: &str) -> bool {
        let (dir, name) = path.rsplit_once('/').expect("object path has a directory");
        cold_store
            .list_file_names(&format!("{dir}/"))
            .await
            .expect("list objects")
            .iter()
            .any(|listed| listed == name)
    }

    /// With an 8-byte page span and the retained offset at 10: page 0 lies
    /// wholly below it and goes with the objects only it names. Page 1 holds
    /// the offset, so it is never rewritten, and the objects it names stay,
    /// including the chunk wholly below the offset and the one that spans
    /// both pages.
    #[tokio::test]
    async fn collects_whole_pages_below_the_offset_and_never_the_boundary_page() {
        let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
        let stream = BucketStreamId::new("benchcmp", "retention-pages");
        let path = |name: &str| format!("benchcmp/retention-pages/chunks/{name}.bin");
        let (a, x, e, c, d) = (path("a"), path("x"), path("e"), path("c"), path("d"));
        for object in [&a, &x, &e, &c, &d] {
            cold_store.write_chunk(object, b"..").await.expect("stage");
        }
        let store = ColdStoreColdIndexPageStore::new(cold_store.clone());
        let key = |page_id| ColdIndexPageKey {
            stream_id: stream.clone(),
            generation: 7,
            page_id,
        };
        let page0 = ColdIndexPage {
            start_offset: 0,
            end_offset: 8,
            cold_chunks: vec![chunk(0, 4, &a), chunk(6, 9, &e)],
            external_segments: vec![ObjectPayloadRef {
                start_offset: 4,
                end_offset: 6,
                s3_path: x.clone(),
                object_size: 2,
                object_offset: 0,
            }],
        };
        let page1 = ColdIndexPage {
            start_offset: 8,
            end_offset: 16,
            cold_chunks: vec![chunk(6, 9, &e), chunk(9, 10, &c), chunk(10, 12, &d)],
            external_segments: Vec::new(),
        };
        store.put_page(&key(0), &page0).await.expect("page 0");
        store.put_page(&key(1), &page1).await.expect("page 1");

        let target = RetentionGcTarget {
            stream_id: stream.clone(),
            generation: 7,
            created_at_ms: 1,
            collect_below: 10,
            keep_paths: Vec::new(),
        };
        let mut report = RetentionGcReport::default();
        let changed = collect_stream(&cold_store, &target, 8, &mut report)
            .await
            .expect("collect");
        assert!(changed);
        assert_eq!(report.pages_deleted, 1);
        assert_eq!(report.objects_deleted, 2);
        assert!(store.get_page(&key(0)).await.expect("read").is_none());
        assert_eq!(
            store.get_page(&key(1)).await.expect("read"),
            Some(page1),
            "boundary page untouched"
        );
        assert!(!exists(&cold_store, &a).await);
        assert!(!exists(&cold_store, &x).await);
        for kept in [&e, &c, &d] {
            assert!(exists(&cold_store, kept).await, "{kept} deleted");
        }
    }

    #[test]
    fn tracker_releases_an_offset_only_after_the_grace() {
        let input = ColdIndexRepairInput {
            stream_id: BucketStreamId::new("benchcmp", "grace"),
            generation: 0,
            retained_offset: 10,
            tail_offset: 20,
            created_at_ms: 1,
            hot_ranges: Vec::new(),
            state_refs: Vec::new(),
        };
        let mut tracker = RetentionGcTracker::default();
        assert_eq!(tracker.observe(&input, 100, 50), None);
        assert_eq!(tracker.observe(&input, 149, 50), None);
        let target = tracker.observe(&input, 150, 50).expect("due after grace");
        assert_eq!(target.collect_below, 10);
        tracker.collected(&target);
        assert_eq!(tracker.observe(&input, 500, 50), None);
    }
}
