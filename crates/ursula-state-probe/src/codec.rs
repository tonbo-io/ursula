//! Exact group-snapshot sizes from the production codec
//! (`ursula_raft::group_snapshot_frames`), with a per-field and per-stream
//! byte breakdown obtained by decoding each frame with prost.

use std::time::Instant;

use anyhow::Context;
use anyhow::Result;
use bytes::Bytes;
use prost::Message;
use serde::Serialize;
use ursula_proto as proto;
use ursula_runtime::GroupSnapshot;
use ursula_runtime::StreamAppendCount;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;
use ursula_stream::StreamSnapshot;

pub const MIB: u64 = 1 << 20;

pub fn placement0() -> ShardPlacement {
    ShardPlacement {
        core_id: CoreId(0),
        shard_id: ShardId(0),
        raft_group_id: RaftGroupId(0),
    }
}

pub fn group_snapshot(
    stream_snapshot: StreamSnapshot,
    stream_append_counts: Vec<StreamAppendCount>,
    group_commit_index: u64,
) -> GroupSnapshot {
    GroupSnapshot {
        placement: placement0(),
        group_commit_index,
        stream_snapshot,
        stream_append_counts,
    }
}

/// Encode with the production frame iterator.
pub fn encode_frames(snapshot: GroupSnapshot) -> Result<Vec<Bytes>> {
    ursula_raft::group_snapshot_frames(std::sync::Arc::new(snapshot))
        .map(|frame| frame.context("encode snapshot frame"))
        .collect()
}

/// Concatenated snapshot bytes exactly as the inline snapshot store keeps them.
pub fn encode_bytes(snapshot: GroupSnapshot) -> Result<Vec<u8>> {
    let mut all = Vec::new();
    for frame in encode_frames(snapshot)? {
        all.extend_from_slice(&frame);
    }
    Ok(all)
}

pub fn decode(bytes: &[u8]) -> Result<GroupSnapshot> {
    ursula_raft::decode_group_snapshot(bytes).context("decode group snapshot")
}

/// Per-stream figures used by the §7.2 formula checks.
#[derive(Debug, Default, Clone, Serialize)]
pub struct StreamFrameStats {
    pub name: String,
    /// Bytes of this stream's frame.
    pub frame_bytes: u64,
    /// Unflushed payload bytes, `H(s)`.
    pub hot_bytes: u64,
    /// Records starting at or above the seal point, `U(s)`.
    pub unflushed_records: u64,
    /// Retained log below the seal point, in bytes (`K(s)` is this / MiB).
    pub cold_bytes: u64,
    pub dense_entries: u64,
    pub record_offsets_bytes: u64,
    pub message_records: u64,
    pub shared_refs: u64,
    pub external_segments: u64,
    pub producers: u64,
    pub receipts: u64,
    pub producer_bytes: u64,
}

impl StreamFrameStats {
    /// `K(s)` in MiB, rounded up.
    pub fn cold_mib_ceil(&self) -> u64 {
        self.cold_bytes.div_ceil(MIB)
    }
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct SnapStats {
    pub total_bytes: u64,
    pub zstd3_bytes: u64,
    pub frames: u64,
    pub header_bytes: u64,
    pub header_shared_owner_bytes: u64,
    pub header_shared_owner_count: u64,
    pub header_bucket_usage_bytes: u64,
    pub header_bucket_usage_count: u64,
    pub header_erased_bucket_bytes: u64,
    pub header_erased_bucket_count: u64,
    pub stream_frames: u64,
    pub stream_bytes: u64,
    pub record_offsets_count: u64,
    pub record_offsets_bytes: u64,
    pub message_records_count: u64,
    pub message_records_bytes: u64,
    pub cold_chunks_count: u64,
    pub cold_chunks_bytes: u64,
    pub external_segments_count: u64,
    pub external_segments_bytes: u64,
    pub hot_payload_bytes: u64,
    pub hot_segments_count: u64,
    pub hot_segments_bytes: u64,
    pub producer_count: u64,
    pub receipt_count: u64,
    pub producer_bytes: u64,
    pub visible_snapshot_bytes: u64,
    pub stream_fixed_bytes: u64,
    pub append_count_frames: u64,
    pub append_count_bytes: u64,
    pub cold_gc_frames: u64,
    pub cold_gc_bytes: u64,
    pub encode_ms: f64,
    pub zstd_ms: f64,
    #[serde(skip)]
    pub streams: Vec<StreamFrameStats>,
}

fn n(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn delta<F: FnOnce(&mut proto::StreamSnapshotEntryV1)>(
    entry: &proto::StreamSnapshotEntryV1,
    full: usize,
    clear: F,
) -> u64 {
    let mut stripped = entry.clone();
    clear(&mut stripped);
    n(full.saturating_sub(stripped.encoded_len()))
}

fn header_delta<F: FnOnce(&mut proto::SnapshotHeaderV1)>(
    header: &proto::SnapshotHeaderV1,
    full: usize,
    clear: F,
) -> u64 {
    let mut stripped = header.clone();
    clear(&mut stripped);
    n(full.saturating_sub(stripped.encoded_len()))
}

fn stream_name(entry: &proto::StreamSnapshotEntryV1) -> String {
    entry
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.stream_id.as_ref())
        .map(|id| format!("{id:?}"))
        .unwrap_or_default()
}

fn add_stream(st: &mut SnapStats, entry: proto::StreamSnapshotEntryV1, frame_bytes: u64) {
    st.stream_frames += 1;
    st.stream_bytes += frame_bytes;
    let full = entry.encoded_len();
    let ro = delta(&entry, full, |x| x.record_offsets.clear());
    let mr = delta(&entry, full, |x| x.message_records.clear());
    let cc = delta(&entry, full, |x| x.cold_chunks.clear());
    let es = delta(&entry, full, |x| x.external_segments.clear());
    let hp = delta(&entry, full, |x| x.payload = Bytes::new());
    let hs = delta(&entry, full, |x| x.hot_segments.clear());
    let pr = delta(&entry, full, |x| x.producer_states.clear());
    let vs = delta(&entry, full, |x| x.visible_snapshot = None);
    let receipts: u64 = entry
        .producer_states
        .iter()
        .map(|p| n(p.receipts.len()))
        .sum();
    st.record_offsets_bytes += ro;
    st.record_offsets_count += n(entry.record_offsets.len());
    st.message_records_bytes += mr;
    st.message_records_count += n(entry.message_records.len());
    st.cold_chunks_bytes += cc;
    st.cold_chunks_count += n(entry.cold_chunks.len());
    st.external_segments_bytes += es;
    st.external_segments_count += n(entry.external_segments.len());
    st.hot_payload_bytes += hp;
    st.hot_segments_bytes += hs;
    st.hot_segments_count += n(entry.hot_segments.len());
    st.producer_bytes += pr;
    st.producer_count += n(entry.producer_states.len());
    st.receipt_count += receipts;
    st.visible_snapshot_bytes += vs;
    st.stream_fixed_bytes += frame_bytes.saturating_sub(ro + mr + cc + es + hp + hs + pr + vs);

    let tail = entry
        .metadata
        .as_ref()
        .map_or(0, |metadata| metadata.tail_offset);
    let hot_bytes = n(entry.payload.len());
    let seal_point = if hot_bytes == 0 {
        tail
    } else {
        entry.hot_start_offset
    };
    let retained = entry.retained_offset.unwrap_or(0);
    st.streams.push(StreamFrameStats {
        name: stream_name(&entry),
        frame_bytes,
        hot_bytes,
        unflushed_records: n(entry
            .record_offsets
            .iter()
            .filter(|offset| **offset >= seal_point)
            .count()),
        cold_bytes: seal_point.saturating_sub(retained),
        dense_entries: n(entry.record_offsets.len()),
        record_offsets_bytes: ro,
        message_records: n(entry.message_records.len()),
        shared_refs: n(entry.cold_chunks.iter().filter(|c| c.shared_object).count()),
        external_segments: n(entry.external_segments.len()),
        producers: n(entry.producer_states.len()),
        receipts,
        producer_bytes: pr,
    });
}

/// Encode `snapshot` with the real codec, then decode each frame to attribute
/// bytes to fields and streams. `with_zstd` mirrors the S3 snapshot store
/// (level 3).
pub fn measure(snapshot: GroupSnapshot, with_zstd: bool) -> Result<SnapStats> {
    let started = Instant::now();
    let frames = encode_frames(snapshot)?;
    let mut st = SnapStats {
        encode_ms: started.elapsed().as_secs_f64() * 1e3,
        frames: n(frames.len()),
        ..SnapStats::default()
    };
    for frame_bytes in &frames {
        let len = n(frame_bytes.len());
        st.total_bytes += len;
        let mut cursor = std::io::Cursor::new(frame_bytes.as_ref());
        let frame = proto::SnapshotFrameV1::decode_length_delimited(&mut cursor)
            .context("decode snapshot frame")?;
        match frame.frame.context("empty snapshot frame")? {
            proto::snapshot_frame_v1::Frame::Header(h) => {
                st.header_bytes += len;
                let full = h.encoded_len();
                st.header_shared_owner_bytes +=
                    header_delta(&h, full, |x| x.shared_cold_object_owners.clear());
                st.header_bucket_usage_bytes += header_delta(&h, full, |x| x.bucket_usage.clear());
                st.header_erased_bucket_bytes +=
                    header_delta(&h, full, |x| x.erased_buckets.clear());
                st.header_shared_owner_count += n(h.shared_cold_object_owners.len());
                st.header_bucket_usage_count += n(h.bucket_usage.len());
                st.header_erased_bucket_count += n(h.erased_buckets.len());
            }
            proto::snapshot_frame_v1::Frame::Stream(entry) => add_stream(&mut st, *entry, len),
            proto::snapshot_frame_v1::Frame::AppendCount(_) => {
                st.append_count_frames += 1;
                st.append_count_bytes += len;
            }
            proto::snapshot_frame_v1::Frame::ColdGc(_) => {
                st.cold_gc_frames += 1;
                st.cold_gc_bytes += len;
            }
            proto::snapshot_frame_v1::Frame::Footer(_) => {}
        }
    }
    if with_zstd {
        let mut all = Vec::with_capacity(usize::try_from(st.total_bytes).unwrap_or(0));
        for frame in &frames {
            all.extend_from_slice(frame);
        }
        let started = Instant::now();
        st.zstd3_bytes = n(zstd::bulk::compress(&all, 3).context("zstd")?.len());
        st.zstd_ms = started.elapsed().as_secs_f64() * 1e3;
    }
    Ok(st)
}
