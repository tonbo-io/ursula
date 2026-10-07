//! The cold objects a stream snapshot references, and whether a cold store
//! holds them.
//!
//! A backup carries hot bytes and references to cold objects, not the cold
//! objects themselves. Before a restore imports anything, the target checks
//! that its cold store holds every object a read of the restored streams can
//! reach: the cold-index pages covering each stream's cold ranges, the chunks
//! and external payloads those pages and the stream state name, and the
//! objects holding published snapshot bodies. The ranges come from the
//! production read planner over each stream's whole retained range, so the
//! check follows the same rules a read does.

use std::collections::BTreeSet;
use std::io;
use std::sync::Arc;

use futures_util::StreamExt;
use futures_util::TryStreamExt;
use ursula_shard::BucketStreamId;
use ursula_stream::StreamReadSegment;
use ursula_stream::StreamResponse;
use ursula_stream::StreamSnapshot;
use ursula_stream::StreamSnapshotError;
use ursula_stream::StreamStateMachine;

use crate::cold_index::ColdIndexPageCache;
use crate::cold_index::ColdIndexPageKey;
use crate::cold_index::ColdStoreColdIndexPageStore;
use crate::cold_store::ColdStoreHandle;

/// How many missing object keys a check reports by name.
pub const MISSING_COLD_OBJECT_SAMPLE: usize = 10;
/// Object stats a check keeps in flight at once.
const COLD_REFERENCE_CHECK_CONCURRENCY: usize = 32;
/// Pages the check keeps cached while it resolves one stream's ranges.
const COLD_REFERENCE_PAGE_CACHE_PAGES: usize = 64;

/// What a check of one stream snapshot against a cold store found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColdReferenceReport {
    /// Distinct cold objects the snapshot references, index pages included.
    pub referenced: u64,
    /// How many of them the cold store does not hold.
    pub missing: u64,
    /// Missing keys relative to the cold root, at most
    /// [`MISSING_COLD_OBJECT_SAMPLE`]: index pages first, then other objects
    /// in key order.
    pub missing_sample: Vec<String>,
}

impl ColdReferenceReport {
    fn record_missing(&mut self, key: String) {
        self.missing = self.missing.saturating_add(1);
        if self.missing_sample.len() < MISSING_COLD_OBJECT_SAMPLE {
            self.missing_sample.push(key);
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ColdReferenceError {
    #[error("stream snapshot failed validation")]
    InvalidSnapshot(#[from] StreamSnapshotError),
    #[error(
        "stream '{stream_id}' has no read plan over its retained range: {}",
        plan_refusal(.response)
    )]
    Unplannable {
        stream_id: BucketStreamId,
        response: Box<StreamResponse>,
    },
    #[error("cold store could not answer for '{key}'")]
    ColdStore {
        key: String,
        #[source]
        source: io::Error,
    },
}

fn plan_refusal(response: &StreamResponse) -> String {
    match response {
        StreamResponse::Error { code, message, .. } => format!("{code:?}: {message}"),
        other => format!("unexpected planner answer {other:?}"),
    }
}

/// Checks that `cold_store` holds every cold object `snapshot` references.
/// Without a cold store, every reference is missing.
pub async fn check_cold_references(
    cold_store: Option<&ColdStoreHandle>,
    snapshot: StreamSnapshot,
) -> Result<ColdReferenceReport, ColdReferenceError> {
    let mut objects = BTreeSet::new();
    let mut stream_ids = Vec::with_capacity(snapshot.streams.len());
    for entry in &snapshot.streams {
        stream_ids.push(entry.metadata.stream_id.clone());
        if let Some(object) = entry
            .visible_snapshot
            .as_ref()
            .and_then(|visible| visible.object.as_ref())
        {
            objects.insert(object.s3_path.clone());
        }
    }
    let machine = StreamStateMachine::restore(snapshot)?;

    let pages = cold_store.map(|cold_store| {
        ColdIndexPageCache::new(
            Arc::new(ColdStoreColdIndexPageStore::new(Arc::clone(cold_store))),
            COLD_REFERENCE_PAGE_CACHE_PAGES,
        )
    });
    let mut report = ColdReferenceReport::default();
    let mut page_keys = BTreeSet::new();
    for stream_id in &stream_ids {
        let Some(tail) = machine.head(stream_id).map(|stream| stream.tail_offset) else {
            continue;
        };
        let retained = machine.retained_offset(stream_id);
        let Some(len) = tail.checked_sub(retained).filter(|len| *len > 0) else {
            continue;
        };
        let plan = machine
            .read_plan(
                stream_id,
                retained,
                usize::try_from(len).unwrap_or(usize::MAX),
            )
            .map_err(|response| ColdReferenceError::Unplannable {
                stream_id: stream_id.clone(),
                response: Box::new(response),
            })?;
        for segment in plan.segments {
            match segment {
                StreamReadSegment::Hot(_) => {}
                StreamReadSegment::Object(segment) => {
                    objects.insert(segment.object.s3_path);
                }
                StreamReadSegment::ColdIndex(segment) => {
                    let key = ColdIndexPageKey {
                        stream_id: stream_id.clone(),
                        generation: segment.generation,
                        page_id: segment.page_id,
                    };
                    let path = key.path();
                    let new_page = page_keys.insert(path.clone());
                    let Some(pages) = &pages else {
                        if new_page {
                            report.record_missing(path);
                        }
                        continue;
                    };
                    match pages.object_segments_for_read(stream_id, &segment).await {
                        Ok(page_objects) => {
                            objects.extend(page_objects.into_iter().map(|object| object.s3_path));
                        }
                        Err(err) if err.kind() == io::ErrorKind::NotFound => {
                            if new_page {
                                report.record_missing(path);
                            }
                        }
                        Err(source) => {
                            return Err(ColdReferenceError::ColdStore { key: path, source });
                        }
                    }
                }
            }
        }
    }
    report.referenced =
        u64::try_from(page_keys.len().saturating_add(objects.len())).unwrap_or(u64::MAX);

    let Some(cold_store) = cold_store else {
        for key in objects {
            report.record_missing(key);
        }
        return Ok(report);
    };
    let found = futures_util::stream::iter(objects)
        .map(|key| async move {
            match cold_store.object_size(&key).await {
                Ok(_) => Ok((key, true)),
                Err(err) if err.kind() == io::ErrorKind::NotFound => Ok((key, false)),
                Err(source) => Err(ColdReferenceError::ColdStore { key, source }),
            }
        })
        .buffered(COLD_REFERENCE_CHECK_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
    for (key, present) in found {
        if !present {
            report.record_missing(key);
        }
    }
    Ok(report)
}
