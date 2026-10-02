//! External payload locators, committed first and indexed after
//! (bounded-stream-state F5, feature level 3).
//!
//! From level 3 an `AppendExternal` keeps its [`ObjectPayloadRef`] in the
//! stream's replicated cold state at apply, so the locator exists exactly
//! when the append committed. The engine no longer writes a cold-index page
//! entry before proposing. A leader-side offload pass later writes page
//! entries for the committed refs (clipping whatever overlapped them, F19)
//! and proposes [`StreamCommand::OffloadColdRefs`], whose apply removes those
//! refs from state. State is the staging area; pages are the durable index.
//!
//! - [`StreamStateMachine::staged_external_ref_candidates`]: the offload
//!   pass's read-only query, streams whose staged refs are due.
//! - `offload_cold_refs`: apply of [`StreamCommand::OffloadColdRefs`].
//!
//! [`StreamCommand::OffloadColdRefs`]: crate::StreamCommand::OffloadColdRefs

use std::cmp::Ordering;

use ursula_shard::BucketStreamId;

use super::ObjectPayloadRef;
use super::StreamErrorCode;
use super::StreamResponse;
use super::StreamStateMachine;

/// Staged external refs per stream above which the offload pass moves them
/// into pages at once (T_ext in §5.6).
pub const MAX_STAGED_EXTERNAL_REFS: usize = 16;

/// Age after which a staged external ref is offloaded even below
/// [`MAX_STAGED_EXTERNAL_REFS`] (§5.6: 10 s).
pub const STAGED_EXTERNAL_REF_MAX_AGE_MS: u64 = 10_000;

/// A stream whose state-held external refs the offload pass should move into
/// cold-index pages, with every ref it holds now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedExternalRefCandidate {
    pub stream_id: BucketStreamId,
    /// Cold-index page generation of the stream's incarnation (F14g); the
    /// page entries are written under it.
    pub cold_generation: u64,
    /// The stream's state-held external refs, in offset order.
    pub refs: Vec<ObjectPayloadRef>,
}

fn stream_order(left: &BucketStreamId, right: &BucketStreamId) -> Ordering {
    (
        left.bucket_id.as_str(),
        left.affinity_key.as_deref(),
        left.stream_id.as_str(),
    )
        .cmp(&(
            right.bucket_id.as_str(),
            right.affinity_key.as_deref(),
            right.stream_id.as_str(),
        ))
}

impl StreamStateMachine {
    /// Whether `AppendExternal` keeps its locator in state (F5, level 3).
    pub(super) fn external_locators_in_state(&self) -> bool {
        self.feature_level >= crate::feature::FEATURE_LEVEL_EXTERNAL_LOCATORS
    }

    /// Offload discovery (F5): streams holding more than `max_staged` state
    /// external refs, or at least one ref for which `is_due` holds (the
    /// caller decides age, typically from the object's write time), most refs
    /// first, then by stream id, at most `limit`. Empty below level 3, where
    /// `OffloadColdRefs` is refused. Read-only.
    pub fn staged_external_ref_candidates(
        &self,
        max_staged: usize,
        is_due: &dyn Fn(&ObjectPayloadRef) -> bool,
        limit: usize,
    ) -> Vec<StagedExternalRefCandidate> {
        if !self.external_locators_in_state() {
            return Vec::new();
        }
        let mut candidates = self
            .registry
            .slots()
            .filter_map(|slot| {
                let refs = slot.cold.external_segments();
                if refs.is_empty() || (refs.len() <= max_staged && !refs.iter().any(is_due)) {
                    return None;
                }
                let mut refs = refs.to_vec();
                refs.sort_by_key(|object| (object.start_offset, object.end_offset));
                Some(StagedExternalRefCandidate {
                    stream_id: slot.metadata.stream_id.clone(),
                    cold_generation: slot.cold.cold_generation(),
                    refs,
                })
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            right
                .refs
                .len()
                .cmp(&left.refs.len())
                .then_with(|| stream_order(&left.stream_id, &right.stream_id))
        });
        candidates.truncate(limit);
        candidates
    }

    /// Applies [`crate::StreamCommand::OffloadColdRefs`]: removes exactly the
    /// listed refs that the stream still holds in state. The leader wrote
    /// their page entries before proposing, so nothing is queued for GC; the
    /// pages reference the objects now. A ref's range is never hot (an
    /// external append's bytes never enter the hot buffer), so the derived
    /// cold coverage (F18) still covers it after removal.
    pub(super) fn offload_cold_refs(
        &mut self,
        stream_id: &BucketStreamId,
        refs: &[ObjectPayloadRef],
    ) -> StreamResponse {
        if let Err(response) = self.require_feature_level(
            crate::feature::FEATURE_LEVEL_EXTERNAL_LOCATORS,
            "external locator offload",
        ) {
            return response;
        }
        let Some(slot) = self.stream_slot_mut(stream_id) else {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        };
        let removed = slot.cold.remove_external_segments(refs);
        let remaining = u64::try_from(slot.cold.external_segments().len()).unwrap_or(u64::MAX);
        StreamResponse::ColdRefsOffloaded { removed, remaining }
    }
}
