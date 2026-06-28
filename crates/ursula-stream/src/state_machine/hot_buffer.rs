//! Hot in-memory payload buffer split into append-ordered chunks.

use super::HotPayloadSegment;
use super::StreamReadSegment;
use super::VecDeque;

#[derive(Debug, Clone, Default)]
pub(super) struct HotBuffer {
    chunks: VecDeque<HotChunk>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HotChunk {
    start_offset: u64,
    end_offset: u64,
    bytes: Vec<u8>,
}

impl HotBuffer {
    pub(super) fn from_payload(start_offset: u64, payload: Vec<u8>) -> Self {
        if payload.is_empty() {
            return Self::default();
        }
        let end_offset = start_offset
            .saturating_add(u64::try_from(payload.len()).expect("payload len fits u64"));
        let mut chunks = VecDeque::new();
        chunks.push_back(HotChunk {
            start_offset,
            end_offset,
            bytes: payload,
        });
        Self { chunks }
    }

    pub(super) fn from_snapshot(payload: Vec<u8>, segments: &[HotPayloadSegment]) -> Self {
        let mut chunks = VecDeque::with_capacity(segments.len());
        for segment in segments {
            chunks.push_back(HotChunk {
                start_offset: segment.start_offset,
                end_offset: segment.end_offset,
                bytes: payload[segment.payload_start..segment.payload_end].to_vec(),
            });
        }
        Self { chunks }
    }

    pub(super) fn len(&self) -> usize {
        self.chunks.iter().map(|chunk| chunk.bytes.len()).sum()
    }

    pub(super) fn hot_start_offset(&self) -> u64 {
        self.chunks
            .front()
            .map(|chunk| chunk.start_offset)
            .unwrap_or(0)
    }

    pub(super) fn first_start_offset(&self) -> Option<u64> {
        self.chunks.front().map(|chunk| chunk.start_offset)
    }

    pub(super) fn payload(&self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(self.len());
        for chunk in &self.chunks {
            payload.extend_from_slice(&chunk.bytes);
        }
        payload
    }

    pub(super) fn hot_segments(&self) -> Vec<HotPayloadSegment> {
        let mut payload_start = 0usize;
        self.chunks
            .iter()
            .map(|chunk| {
                let payload_end = payload_start + chunk.bytes.len();
                let segment = HotPayloadSegment {
                    start_offset: chunk.start_offset,
                    end_offset: chunk.end_offset,
                    payload_start,
                    payload_end,
                };
                payload_start = payload_end;
                segment
            })
            .collect()
    }

    pub(super) fn push(&mut self, start_offset: u64, end_offset: u64, payload: &[u8]) {
        if payload.is_empty() {
            return;
        }
        self.chunks.push_back(HotChunk {
            start_offset,
            end_offset,
            bytes: payload.to_vec(),
        });
    }

    pub(super) fn plan_cold_flush_from(
        &self,
        from_offset: u64,
        min_hot_bytes: usize,
        max_flush_bytes: usize,
    ) -> Option<(u64, u64, Vec<u8>)> {
        let mut payload = Vec::new();
        for chunk in &self.chunks {
            if chunk.end_offset <= from_offset {
                continue;
            }
            if chunk.start_offset
                > from_offset + u64::try_from(payload.len()).expect("payload len fits u64")
            {
                break;
            }
            if payload.len() >= max_flush_bytes {
                break;
            }
            let skip = if chunk.start_offset < from_offset {
                usize::try_from(from_offset - chunk.start_offset).expect("skip fits usize")
            } else {
                0
            };
            let remaining = max_flush_bytes - payload.len();
            let take = (chunk.bytes.len() - skip).min(remaining);
            payload.extend_from_slice(&chunk.bytes[skip..skip + take]);
            if take < chunk.bytes.len() - skip {
                break;
            }
        }
        if payload.len() < min_hot_bytes {
            return None;
        }
        let end_offset = from_offset + u64::try_from(payload.len()).expect("payload len fits u64");
        Some((from_offset, end_offset, payload))
    }

    pub(super) fn remaining_len_from(&self, from_offset: u64) -> usize {
        self.chunks
            .iter()
            .filter(|chunk| chunk.end_offset > from_offset)
            .map(|chunk| {
                let start = chunk.start_offset.max(from_offset);
                usize::try_from(chunk.end_offset - start).expect("remaining len fits usize")
            })
            .sum()
    }

    pub(super) fn read_segments(
        &self,
        offset: u64,
        next_offset: u64,
    ) -> Vec<(u64, StreamReadSegment)> {
        let mut segments = Vec::new();
        for chunk in &self.chunks {
            let start = offset.max(chunk.start_offset);
            let end = next_offset.min(chunk.end_offset);
            if start < end {
                let payload_start =
                    usize::try_from(start - chunk.start_offset).expect("hot start fits usize");
                let payload_end =
                    usize::try_from(end - chunk.start_offset).expect("hot end fits usize");
                segments.push((
                    start,
                    StreamReadSegment::Hot(chunk.bytes[payload_start..payload_end].to_vec()),
                ));
            }
        }
        segments
    }

    pub(super) fn covers_prefix(&self, start_offset: u64, end_offset: u64) -> bool {
        let Some(first) = self.chunks.front() else {
            return false;
        };
        if first.start_offset != start_offset {
            return false;
        }
        let mut covered_offset = start_offset;
        for chunk in &self.chunks {
            if chunk.start_offset != covered_offset {
                return false;
            }
            if chunk.end_offset >= end_offset {
                return true;
            }
            covered_offset = chunk.end_offset;
        }
        false
    }

    pub(super) fn flush_prefix(&mut self, end_offset: u64) {
        while self
            .chunks
            .front()
            .is_some_and(|chunk| chunk.end_offset <= end_offset)
        {
            self.chunks.pop_front();
        }
        if let Some(front) = self.chunks.front_mut()
            && front.start_offset < end_offset
        {
            let drain_len =
                usize::try_from(end_offset - front.start_offset).expect("drain len fits usize");
            front.bytes.drain(..drain_len);
            front.start_offset = end_offset;
        }
    }

    pub(super) fn discard_before(&mut self, retained_offset: u64) {
        self.flush_prefix(retained_offset);
    }
}
