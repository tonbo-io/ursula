//! Cold-tier reference state: flushed chunks, external segments, and the cold frontier.

use super::ColdChunkRef;
use super::ObjectPayloadRef;

#[derive(Debug, Clone, Default)]
pub(super) struct StreamColdState {
    cold_chunks: Vec<ColdChunkRef>,
    external_segments: Vec<ObjectPayloadRef>,
    cold_frontier: u64,
}

impl StreamColdState {
    pub(super) fn cold_chunks(&self) -> &[ColdChunkRef] {
        &self.cold_chunks
    }

    pub(super) fn external_segments(&self) -> &[ObjectPayloadRef] {
        &self.external_segments
    }

    pub(super) fn cold_generation(&self) -> u64 {
        0
    }

    pub(super) fn push_cold_chunk(&mut self, chunk: ColdChunkRef) {
        self.cold_frontier = chunk.end_offset;
    }

    pub(super) fn push_external_segment(&mut self, object: ObjectPayloadRef) {
        let frontier = self.cold_frontier.max(object.end_offset);
        self.cold_frontier = frontier;
    }

    pub(super) fn restore(
        cold_frontier_offset: u64,
        _cold_index_generation: u64,
        cold_chunks: Vec<ColdChunkRef>,
        external_segments: Vec<ObjectPayloadRef>,
    ) -> Self {
        Self {
            cold_chunks,
            external_segments,
            cold_frontier: cold_frontier_offset,
        }
    }

    pub(super) fn has_cold_objects(&self) -> bool {
        self.cold_frontier > 0 || !self.cold_chunks.is_empty() || !self.external_segments.is_empty()
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
        dropped_cold_paths
    }

    pub(super) fn cold_frontier_offset(&self, retained_offset: u64) -> u64 {
        let external_segments = self.external_segments();
        let cold_frontier = self.cold_frontier.max(retained_offset);
        let mut ranges = Vec::with_capacity(1 + external_segments.len());
        if cold_frontier > retained_offset {
            ranges.push((retained_offset, cold_frontier));
        }
        ranges.extend(
            external_segments
                .iter()
                .map(|object| (object.start_offset, object.end_offset)),
        );
        ranges.sort_unstable();

        let mut frontier = retained_offset;
        for (start_offset, end_offset) in ranges {
            if end_offset <= frontier {
                continue;
            }
            if start_offset > frontier {
                break;
            }
            frontier = end_offset;
        }
        frontier
    }
}
