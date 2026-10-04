//! Cold-tier reference state: flushed chunks and external segments.
//!
//! Cold coverage is derived from the hot buffer (F18 step 2): every retained
//! byte the hot buffer does not hold is cold.

use super::ColdChunkRef;
use super::ObjectPayloadRef;
use super::hot_buffer::shrink_vec_if_slack;

#[derive(Debug, Clone, Default)]
pub(super) struct StreamColdState {
    cold_chunks: Vec<ColdChunkRef>,
    external_segments: Vec<ObjectPayloadRef>,
    /// Cold-index page generation of this stream incarnation (F14g): the
    /// stream's unique `created_at_ms`.
    cold_generation: u64,
}

impl StreamColdState {
    pub(super) fn cold_chunks(&self) -> &[ColdChunkRef] {
        &self.cold_chunks
    }

    pub(super) fn external_segments(&self) -> &[ObjectPayloadRef] {
        &self.external_segments
    }

    pub(super) fn cold_generation(&self) -> u64 {
        self.cold_generation
    }

    /// Cold state of a new incarnation whose objects are scoped to
    /// `cold_generation` (F14g step 2).
    pub(super) fn with_generation(cold_generation: u64) -> Self {
        Self {
            cold_generation,
            ..Self::default()
        }
    }

    /// Keeps an external payload as a direct state reference instead of a
    /// cold-index page entry. A create's initial payload is staged before
    /// apply assigns the incarnation, so no page generation exists for it
    /// yet (F14g step 2).
    pub(super) fn push_direct_external_segment(&mut self, object: ObjectPayloadRef) {
        self.external_segments.push(object);
    }

    /// Removes exactly the listed external refs that state still holds
    /// (F5 `OffloadColdRefs`) and returns how many it removed.
    pub(super) fn remove_external_segments(&mut self, refs: &[ObjectPayloadRef]) -> u64 {
        let before = self.external_segments.len();
        self.external_segments
            .retain(|object| !refs.iter().any(|offloaded| offloaded == object));
        if self.external_segments.len() < before {
            self.external_segments.shrink_to_fit();
        }
        u64::try_from(before.saturating_sub(self.external_segments.len())).unwrap_or(u64::MAX)
    }

    pub(super) fn push_cold_chunk(&mut self, chunk: ColdChunkRef) {
        if chunk.shared_object {
            self.cold_chunks.push(chunk);
        }
    }

    pub(super) fn restore(
        cold_index_generation: u64,
        cold_chunks: Vec<ColdChunkRef>,
        external_segments: Vec<ObjectPayloadRef>,
    ) -> Self {
        Self {
            cold_chunks,
            external_segments,
            cold_generation: cold_index_generation,
        }
    }

    pub(super) fn has_state_refs(&self) -> bool {
        !self.cold_chunks.is_empty() || !self.external_segments.is_empty()
    }

    pub(super) fn compact_before(&mut self, retained_offset: u64) -> Vec<String> {
        let mut dropped_cold_paths = Vec::new();
        self.cold_chunks.retain(|chunk| {
            let retain = chunk.end_offset > retained_offset;
            if !retain {
                dropped_cold_paths.push(chunk.s3_path.clone());
            }
            retain
        });
        self.external_segments
            .retain(|object| object.end_offset > retained_offset);
        // F7: return the capacity retention freed.
        shrink_vec_if_slack(&mut self.cold_chunks);
        shrink_vec_if_slack(&mut self.external_segments);
        dropped_cold_paths
    }

    pub(super) fn shared_object_paths(&self) -> impl Iterator<Item = &str> {
        self.cold_chunks
            .iter()
            .filter(|chunk| chunk.shared_object)
            .map(|chunk| chunk.s3_path.as_str())
    }

    pub(super) fn remove_shared_chunks(&mut self, old_chunks: &[ColdChunkRef]) -> bool {
        let before = self.cold_chunks.len();
        self.cold_chunks
            .retain(|chunk| !old_chunks.iter().any(|old| old == chunk));
        // F7: compaction removes up to a whole run of refs at once.
        shrink_vec_if_slack(&mut self.cold_chunks);
        before.saturating_sub(self.cold_chunks.len()) == old_chunks.len()
    }

    #[cfg(test)]
    pub(super) fn ref_capacities(&self) -> (usize, usize) {
        (
            self.cold_chunks.capacity(),
            self.external_segments.capacity(),
        )
    }
}
