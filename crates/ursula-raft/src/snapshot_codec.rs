#![expect(
    clippy::arithmetic_side_effects,
    reason = "pre-existing arithmetic debt; see Known debt in AGENTS.md"
)]
use std::io::Cursor;
use std::sync::Arc;

use bytes::Buf;
use bytes::Bytes;
use prost::Message;
use ursula_proto as proto;
use ursula_runtime::GroupSnapshot;
use ursula_runtime::SnapshotBytesIterator;
use ursula_runtime::SnapshotStoreError;
use ursula_runtime::StreamAppendCount;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;
use ursula_stream::ColdGcEntry;
use ursula_stream::ColdGcTarget;
use ursula_stream::HotPayloadSegment;
use ursula_stream::ObjectPayloadRef;
use ursula_stream::ProducerReceipt;
use ursula_stream::ProducerSnapshot;
use ursula_stream::StreamMetadata;
use ursula_stream::StreamSnapshot;
use ursula_stream::StreamSnapshotEntry;
use ursula_stream::StreamStatus;
use ursula_stream::StreamVisibleSnapshot;

fn placement_to_proto(placement: ShardPlacement) -> proto::ShardPlacementV1 {
    proto::ShardPlacementV1 {
        core_id: u32::from(placement.core_id.0),
        shard_id: placement.shard_id.0,
        raft_group_id: placement.raft_group_id.0,
    }
}

/// Encodes a group snapshot as the length-delimited `SnapshotFrameV1` frames
/// every snapshot store persists. Public so measurement tools
/// (`ursula-state-probe`) size snapshots with the production codec.
pub fn group_snapshot_frames(snapshot: Arc<GroupSnapshot>) -> SnapshotBytesIterator {
    Box::new(GroupSnapshotFrameIter::new(snapshot))
}

#[cfg(test)]
thread_local! {
    static DECODE_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Number of [`decode_group_snapshot`] calls on the current thread, so tests
/// can assert that an install decodes a snapshot exactly once.
#[cfg(test)]
pub(crate) fn decode_calls_on_this_thread() -> usize {
    DECODE_CALLS.with(std::cell::Cell::get)
}

/// Decodes the concatenated frames written by [`group_snapshot_frames`].
pub fn decode_group_snapshot(bytes: &[u8]) -> Result<GroupSnapshot, SnapshotStoreError> {
    #[cfg(test)]
    DECODE_CALLS.with(|calls| calls.set(calls.get() + 1));
    let mut cursor = Cursor::new(bytes);
    let mut header = None;
    let mut streams = Vec::new();
    let mut stream_append_counts = Vec::new();
    let mut pending_cold_gc = Vec::new();
    let mut footer_seen = false;

    // E5: the format-epoch frame comes first and is checked before any other
    // frame is decoded, so a 0.5.x snapshot fails here rather than on a
    // stream frame it would misread.
    // A first frame that does not decode is corruption, not an old
    // snapshot; only a frame that decodes but is not `FormatEpoch` gets E5.
    let first = if cursor.has_remaining() {
        proto::SnapshotFrameV1::decode_length_delimited(&mut cursor)
            .map_err(|err| SnapshotStoreError::Deserialize(format!("snapshot frame: {err}")))?
            .frame
    } else {
        None
    };
    match first {
        Some(proto::snapshot_frame_v1::Frame::FormatEpoch(value)) => {
            if value.epoch != ursula_stream::FORMAT_EPOCH {
                return Err(SnapshotStoreError::Deserialize(
                    ursula_stream::format_epoch_refusal(
                        "snapshot",
                        &format!("is format epoch {}", value.epoch),
                    ),
                ));
            }
        }
        _ => {
            return Err(SnapshotStoreError::Deserialize(
                ursula_stream::format_epoch_refusal(
                    "snapshot",
                    "has no leading format-epoch frame (Ursula 0.5.x or earlier, format epoch 1)",
                ),
            ));
        }
    }

    while cursor.has_remaining() {
        let frame = proto::SnapshotFrameV1::decode_length_delimited(&mut cursor)
            .map_err(|err| SnapshotStoreError::Deserialize(format!("snapshot frame: {err}")))?;
        let frame = required(frame.frame, "snapshot frame")?;
        match frame {
            proto::snapshot_frame_v1::Frame::FormatEpoch(_) => {
                return Err(SnapshotStoreError::Deserialize(
                    "snapshot format-epoch frame is not the first frame".to_owned(),
                ));
            }
            proto::snapshot_frame_v1::Frame::Header(value) => {
                if header.replace(value).is_some() {
                    return Err(SnapshotStoreError::Deserialize(
                        "snapshot contains duplicate header".to_owned(),
                    ));
                }
            }
            proto::snapshot_frame_v1::Frame::Stream(value) => {
                streams.push(stream_from_proto(*value)?);
            }
            proto::snapshot_frame_v1::Frame::AppendCount(value) => {
                stream_append_counts.push(append_count_from_proto(value)?);
            }
            proto::snapshot_frame_v1::Frame::ColdGc(value) => {
                pending_cold_gc.push(cold_gc_from_proto(value)?);
            }
            proto::snapshot_frame_v1::Frame::Footer(_) => {
                footer_seen = true;
            }
        }
    }

    let header = header.ok_or_else(|| {
        SnapshotStoreError::Deserialize("snapshot missing header frame".to_owned())
    })?;
    if !footer_seen {
        return Err(SnapshotStoreError::Deserialize(
            "snapshot missing footer frame".to_owned(),
        ));
    }
    // Header field 7 stays `optional` on the wire; every writer sets it, so
    // an absent value is corruption.
    let snapshot_write_unit = required(
        header.committed_write_unit_bytes,
        "snapshot header committed_write_unit_bytes",
    )?;
    if snapshot_write_unit != ursula_stream::COMMITTED_WRITE_UNIT_BYTES {
        return Err(SnapshotStoreError::Deserialize(format!(
            "snapshot committed write unit is {snapshot_write_unit} bytes; this build uses {}",
            ursula_stream::COMMITTED_WRITE_UNIT_BYTES
        )));
    }

    Ok(GroupSnapshot {
        placement: placement_from_proto(required(header.placement, "snapshot header placement")?),
        group_commit_index: header.group_commit_index,
        stream_snapshot: StreamSnapshot {
            format_epoch: ursula_stream::FORMAT_EPOCH,
            buckets: header.buckets,
            erased_buckets: header.erased_buckets,
            streams,
            pending_cold_gc,
            next_cold_gc_seq: header.next_cold_gc_seq,
            bucket_usage: header
                .bucket_usage
                .into_iter()
                .map(bucket_usage_from_proto)
                .collect(),
            last_created_at_ms: header.last_created_at_ms,
        },
        stream_append_counts,
    })
}

fn bucket_usage_from_proto(value: proto::BucketUsageV1) -> ursula_stream::BucketUsageSnapshot {
    ursula_stream::BucketUsageSnapshot {
        bucket_id: value.bucket_id,
        usage: ursula_stream::BucketUsage {
            committed_append_bytes: value.committed_append_bytes,
            committed_records: value.committed_records,
            committed_write_units: value.committed_write_units,
            retained_bytes: value.retained_bytes,
            stream_count: value.stream_count,
        },
    }
}

fn bucket_usage_to_proto(value: ursula_stream::BucketUsageSnapshot) -> proto::BucketUsageV1 {
    proto::BucketUsageV1 {
        bucket_id: value.bucket_id,
        committed_append_bytes: value.usage.committed_append_bytes,
        committed_records: value.usage.committed_records,
        committed_write_units: value.usage.committed_write_units,
        retained_bytes: value.usage.retained_bytes,
        stream_count: value.usage.stream_count,
    }
}

/// Encodes a group snapshot frame by frame without copying the whole group
/// first (bounded-stream-state F12c). The iterator shares the builder's
/// snapshot through an `Arc`, so a fallback re-encode does not deep-clone it;
/// only the entry being encoded is copied, one frame at a time.
struct GroupSnapshotFrameIter {
    snapshot: Arc<GroupSnapshot>,
    format_epoch: bool,
    header: bool,
    next_stream: usize,
    next_append_count: usize,
    next_cold_gc: usize,
    footer: bool,
}

impl GroupSnapshotFrameIter {
    fn new(snapshot: Arc<GroupSnapshot>) -> Self {
        Self {
            snapshot,
            format_epoch: true,
            header: true,
            next_stream: 0,
            next_append_count: 0,
            next_cold_gc: 0,
            footer: true,
        }
    }

    fn header_frame(snapshot: &GroupSnapshot) -> proto::SnapshotHeaderV1 {
        let stream_snapshot = &snapshot.stream_snapshot;
        proto::SnapshotHeaderV1 {
            placement: Some(placement_to_proto(snapshot.placement)),
            group_commit_index: snapshot.group_commit_index,
            buckets: stream_snapshot.buckets.clone(),
            erased_buckets: stream_snapshot.erased_buckets.clone(),
            next_cold_gc_seq: stream_snapshot.next_cold_gc_seq,
            bucket_usage: stream_snapshot
                .bucket_usage
                .iter()
                .cloned()
                .map(bucket_usage_to_proto)
                .collect(),
            committed_write_unit_bytes: Some(ursula_stream::COMMITTED_WRITE_UNIT_BYTES),
            last_created_at_ms: stream_snapshot.last_created_at_ms,
        }
    }
}

impl Iterator for GroupSnapshotFrameIter {
    type Item = Result<Bytes, SnapshotStoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        let snapshot = &*self.snapshot;
        let frame = if self.format_epoch {
            self.format_epoch = false;
            proto::snapshot_frame_v1::Frame::FormatEpoch(proto::FormatEpochV1 {
                epoch: ursula_stream::FORMAT_EPOCH,
            })
        } else if self.header {
            self.header = false;
            proto::snapshot_frame_v1::Frame::Header(Self::header_frame(snapshot))
        } else if let Some(stream) = snapshot.stream_snapshot.streams.get(self.next_stream) {
            self.next_stream += 1;
            match stream_to_proto(stream.clone()) {
                Ok(stream) => proto::snapshot_frame_v1::Frame::Stream(Box::new(stream)),
                Err(err) => return Some(Err(err)),
            }
        } else if let Some(append_count) = snapshot.stream_append_counts.get(self.next_append_count)
        {
            self.next_append_count += 1;
            proto::snapshot_frame_v1::Frame::AppendCount(append_count_to_proto(
                append_count.clone(),
            ))
        } else if let Some(cold_gc) = snapshot
            .stream_snapshot
            .pending_cold_gc
            .get(self.next_cold_gc)
        {
            self.next_cold_gc += 1;
            proto::snapshot_frame_v1::Frame::ColdGc(cold_gc_to_proto(cold_gc.clone()))
        } else if self.footer {
            self.footer = false;
            proto::snapshot_frame_v1::Frame::Footer(proto::SnapshotFooterV1 {})
        } else {
            return None;
        };

        Some(encode_frame(proto::SnapshotFrameV1 { frame: Some(frame) }))
    }
}

fn encode_frame(frame: proto::SnapshotFrameV1) -> Result<Bytes, SnapshotStoreError> {
    let mut bytes = Vec::with_capacity(frame.encoded_len());
    frame
        .encode_length_delimited(&mut bytes)
        .map_err(|err| SnapshotStoreError::Serialize(format!("snapshot frame: {err}")))?;
    Ok(Bytes::from(bytes))
}

fn stream_to_proto(
    entry: StreamSnapshotEntry,
) -> Result<proto::StreamSnapshotEntryV1, SnapshotStoreError> {
    Ok(proto::StreamSnapshotEntryV1 {
        metadata: Some(metadata_to_proto(entry.metadata)),
        hot_start_offset: entry.hot_start_offset,
        payload: entry.payload.into(),
        hot_segments: entry
            .hot_segments
            .into_iter()
            .map(hot_segment_to_proto)
            .collect(),
        cold_index_generation: entry.cold_index_generation,
        cold_chunks: entry.cold_chunks,
        external_segments: entry
            .external_segments
            .into_iter()
            .map(object_ref_to_proto)
            .collect(),
        visible_snapshot: entry.visible_snapshot.map(visible_snapshot_to_proto),
        producer_states: entry
            .producer_states
            .into_iter()
            .map(producer_to_proto)
            .collect(),
        retained_offset: Some(entry.retained_offset),
    })
}

/// Entry field 16 `retained_offset` stays `optional` on the wire; every
/// writer sets it, so an absent value is corruption. (Dropping `optional`
/// would make writers omit a zero.)
fn stream_from_proto(
    entry: proto::StreamSnapshotEntryV1,
) -> Result<StreamSnapshotEntry, SnapshotStoreError> {
    Ok(StreamSnapshotEntry {
        metadata: metadata_from_proto(required(entry.metadata, "snapshot stream metadata")?)?,
        hot_start_offset: entry.hot_start_offset,
        payload: entry.payload.to_vec(),
        hot_segments: entry
            .hot_segments
            .into_iter()
            .map(hot_segment_from_proto)
            .collect::<Result<Vec<_>, _>>()?,
        cold_index_generation: entry.cold_index_generation,
        cold_chunks: entry.cold_chunks,
        external_segments: entry
            .external_segments
            .into_iter()
            .map(object_ref_from_proto)
            .collect(),
        retained_offset: required(entry.retained_offset, "snapshot stream retained_offset")?,
        visible_snapshot: entry.visible_snapshot.map(visible_snapshot_from_proto),
        producer_states: entry
            .producer_states
            .into_iter()
            .map(producer_from_proto)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn metadata_to_proto(metadata: StreamMetadata) -> proto::StreamMetadataV1 {
    proto::StreamMetadataV1 {
        stream_id: Some(metadata.stream_id.into()),
        content_type: metadata.content_type,
        status: status_to_proto(metadata.status) as i32,
        tail_offset: metadata.tail_offset,
        last_stream_seq: metadata.last_stream_seq,
        stream_ttl_seconds: metadata.stream_ttl_seconds,
        stream_expires_at_ms: metadata.stream_expires_at_ms,
        created_at_ms: metadata.created_at_ms,
        last_ttl_touch_at_ms: metadata.last_ttl_touch_at_ms,
    }
}

fn metadata_from_proto(
    metadata: proto::StreamMetadataV1,
) -> Result<StreamMetadata, SnapshotStoreError> {
    Ok(StreamMetadata {
        stream_id: required(metadata.stream_id, "snapshot stream id")?.into(),
        content_type: metadata.content_type,
        status: status_from_proto(metadata.status)?,
        tail_offset: metadata.tail_offset,
        last_stream_seq: metadata.last_stream_seq,
        stream_ttl_seconds: metadata.stream_ttl_seconds,
        stream_expires_at_ms: metadata.stream_expires_at_ms,
        created_at_ms: metadata.created_at_ms,
        last_ttl_touch_at_ms: metadata.last_ttl_touch_at_ms,
    })
}

fn status_to_proto(status: StreamStatus) -> proto::StreamStatusV1 {
    match status {
        StreamStatus::Open => proto::StreamStatusV1::StreamStatusOpen,
        StreamStatus::Closed => proto::StreamStatusV1::StreamStatusClosed,
    }
}

fn status_from_proto(status: i32) -> Result<StreamStatus, SnapshotStoreError> {
    match proto::StreamStatusV1::try_from(status).map_err(|_unknown| {
        SnapshotStoreError::Deserialize(format!("invalid stream status value {status}"))
    })? {
        proto::StreamStatusV1::StreamStatusOpen => Ok(StreamStatus::Open),
        proto::StreamStatusV1::StreamStatusClosed => Ok(StreamStatus::Closed),
    }
}

fn hot_segment_to_proto(segment: HotPayloadSegment) -> proto::HotPayloadSegmentV1 {
    proto::HotPayloadSegmentV1 {
        start_offset: segment.start_offset,
        end_offset: segment.end_offset,
        payload_start: segment.payload_start as u64,
        payload_end: segment.payload_end as u64,
    }
}

fn hot_segment_from_proto(
    segment: proto::HotPayloadSegmentV1,
) -> Result<HotPayloadSegment, SnapshotStoreError> {
    Ok(HotPayloadSegment {
        start_offset: segment.start_offset,
        end_offset: segment.end_offset,
        payload_start: usize::try_from(segment.payload_start).map_err(|_overflow| {
            SnapshotStoreError::Deserialize(format!(
                "hot segment payload_start {} does not fit usize",
                segment.payload_start
            ))
        })?,
        payload_end: usize::try_from(segment.payload_end).map_err(|_overflow| {
            SnapshotStoreError::Deserialize(format!(
                "hot segment payload_end {} does not fit usize",
                segment.payload_end
            ))
        })?,
    })
}

fn object_ref_to_proto(object: ObjectPayloadRef) -> proto::ObjectPayloadRefV1 {
    proto::ObjectPayloadRefV1 {
        start_offset: object.start_offset,
        end_offset: object.end_offset,
        s3_path: object.s3_path,
        object_size: object.object_size,
    }
}

fn object_ref_from_proto(object: proto::ObjectPayloadRefV1) -> ObjectPayloadRef {
    ObjectPayloadRef {
        start_offset: object.start_offset,
        end_offset: object.end_offset,
        s3_path: object.s3_path,
        object_size: object.object_size,
        object_offset: 0,
    }
}

fn visible_snapshot_to_proto(snapshot: StreamVisibleSnapshot) -> proto::StreamVisibleSnapshotV1 {
    proto::StreamVisibleSnapshotV1 {
        offset: snapshot.offset,
        content_type: snapshot.content_type,
        payload: snapshot.payload.into(),
        digest: snapshot.digest,
        object: snapshot.object,
    }
}

fn visible_snapshot_from_proto(snapshot: proto::StreamVisibleSnapshotV1) -> StreamVisibleSnapshot {
    StreamVisibleSnapshot {
        offset: snapshot.offset,
        content_type: snapshot.content_type,
        payload: snapshot.payload.to_vec(),
        digest: snapshot.digest,
        object: snapshot.object,
    }
}

fn producer_to_proto(producer: ProducerSnapshot) -> proto::ProducerSnapshotV1 {
    proto::ProducerSnapshotV1 {
        producer_id: producer.producer_id,
        producer_epoch: producer.producer_epoch,
        producer_seq: producer.producer_seq,
        last_start_offset: producer.last_start_offset,
        last_next_offset: producer.last_next_offset,
        last_closed: producer.last_closed,
        receipts: producer
            .receipts
            .into_iter()
            .map(producer_receipt_to_proto)
            .collect(),
        last_seen_ms: Some(producer.last_seen_ms),
    }
}

/// Field 9 `last_seen_ms` stays `optional` on the wire, but every writer sets
/// it (bounded-state F3), so an absent value is corruption.
fn producer_from_proto(
    producer: proto::ProducerSnapshotV1,
) -> Result<ProducerSnapshot, SnapshotStoreError> {
    let last_seen_ms = producer.last_seen_ms.ok_or_else(|| {
        SnapshotStoreError::Deserialize(format!(
            "snapshot producer '{}' has no last_seen_ms",
            producer.producer_id
        ))
    })?;
    Ok(ProducerSnapshot {
        producer_id: producer.producer_id,
        producer_epoch: producer.producer_epoch,
        producer_seq: producer.producer_seq,
        last_start_offset: producer.last_start_offset,
        last_next_offset: producer.last_next_offset,
        last_closed: producer.last_closed,
        receipts: producer
            .receipts
            .into_iter()
            .map(producer_receipt_from_proto)
            .collect(),
        last_seen_ms,
    })
}

fn producer_receipt_to_proto(receipt: ProducerReceipt) -> proto::ProducerReceiptV1 {
    proto::ProducerReceiptV1 {
        producer_seq: receipt.producer_seq,
        start_offset: receipt.start_offset,
        next_offset: receipt.next_offset,
        closed: receipt.closed,
    }
}

fn producer_receipt_from_proto(receipt: proto::ProducerReceiptV1) -> ProducerReceipt {
    ProducerReceipt {
        producer_seq: receipt.producer_seq,
        start_offset: receipt.start_offset,
        next_offset: receipt.next_offset,
        closed: receipt.closed,
    }
}

fn append_count_to_proto(count: StreamAppendCount) -> proto::StreamAppendCountV1 {
    proto::StreamAppendCountV1 {
        stream_id: Some(count.stream_id.into()),
        append_count: count.append_count,
    }
}

fn append_count_from_proto(
    count: proto::StreamAppendCountV1,
) -> Result<StreamAppendCount, SnapshotStoreError> {
    Ok(StreamAppendCount {
        stream_id: required(count.stream_id, "snapshot append count stream id")?.into(),
        append_count: count.append_count,
    })
}

fn cold_gc_to_proto(entry: ColdGcEntry) -> proto::ColdGcEntryV1 {
    proto::ColdGcEntryV1 {
        seq: entry.seq,
        bucket_id: entry.bucket_id,
        not_before_ms: entry.not_before_ms,
        cold_generation: entry.cold_generation,
        defer_attempts: entry.defer_attempts,
        target: Some(match entry.target {
            ColdGcTarget::Stream(stream_id) => {
                proto::cold_gc_entry_v1::Target::Stream(stream_id.into())
            }
            ColdGcTarget::Paths(paths) => {
                proto::cold_gc_entry_v1::Target::Paths(proto::ColdGcPathsV1 { paths })
            }
        }),
    }
}

fn cold_gc_from_proto(entry: proto::ColdGcEntryV1) -> Result<ColdGcEntry, SnapshotStoreError> {
    Ok(ColdGcEntry {
        seq: entry.seq,
        bucket_id: entry.bucket_id,
        not_before_ms: entry.not_before_ms,
        cold_generation: entry.cold_generation,
        defer_attempts: entry.defer_attempts,
        target: match required(entry.target, "snapshot cold gc target")? {
            proto::cold_gc_entry_v1::Target::Stream(stream_id) => {
                ColdGcTarget::Stream(stream_id.into())
            }
            proto::cold_gc_entry_v1::Target::Paths(paths) => ColdGcTarget::Paths(paths.paths),
        },
    })
}

fn placement_from_proto(placement: proto::ShardPlacementV1) -> ShardPlacement {
    ShardPlacement {
        core_id: CoreId(
            u16::try_from(placement.core_id).expect("snapshot core_id fits configured u16 core id"),
        ),
        shard_id: ShardId(placement.shard_id),
        raft_group_id: RaftGroupId(placement.raft_group_id),
    }
}

fn required<T>(value: Option<T>, field: &str) -> Result<T, SnapshotStoreError> {
    value.ok_or_else(|| SnapshotStoreError::Deserialize(format!("missing {field}")))
}

#[cfg(test)]
mod tests {
    use ursula_shard::BucketStreamId;

    use super::*;

    #[test]
    fn empty_group_snapshot_round_trips() {
        let snapshot = GroupSnapshot {
            placement: ShardPlacement {
                core_id: CoreId(0),
                shard_id: ShardId(0),
                raft_group_id: RaftGroupId(7),
            },
            group_commit_index: 42,
            stream_snapshot: StreamSnapshot {
                format_epoch: ursula_stream::FORMAT_EPOCH,
                buckets: vec!["bucket".to_owned()],
                erased_buckets: vec!["erased-bucket".to_owned()],
                streams: Vec::new(),
                pending_cold_gc: vec![
                    ColdGcEntry {
                        seq: 7,
                        bucket_id: "bucket".to_owned(),
                        not_before_ms: 0,
                        target: ColdGcTarget::Stream(BucketStreamId::new("bucket", "deleted")),
                        cold_generation: Some(1_233),
                        defer_attempts: 0,
                    },
                    ColdGcEntry {
                        seq: 8,
                        bucket_id: "bucket".to_owned(),
                        not_before_ms: 0,
                        target: ColdGcTarget::Stream(BucketStreamId::new("bucket", "scoped")),
                        cold_generation: Some(1_234),
                        defer_attempts: 3,
                    },
                ],
                next_cold_gc_seq: 9,
                bucket_usage: vec![ursula_stream::BucketUsageSnapshot {
                    bucket_id: "bucket".to_owned(),
                    usage: ursula_stream::BucketUsage {
                        committed_append_bytes: 100,
                        committed_records: 7,
                        committed_write_units: 3,
                        retained_bytes: 60,
                        stream_count: 2,
                    },
                }],
                last_created_at_ms: 1_234,
            },
            stream_append_counts: vec![StreamAppendCount {
                stream_id: BucketStreamId {
                    bucket_id: "bucket".to_owned(),
                    stream_id: "stream".to_owned(),
                },
                append_count: 3,
            }],
        };
        let bytes = group_snapshot_frames(Arc::new(snapshot.clone()))
            .collect::<Result<Vec<_>, _>>()
            .expect("encode frames")
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        let decoded = decode_group_snapshot(&bytes).expect("decode frames");

        assert_eq!(decoded, snapshot);
    }

    /// Bounded-state F3: the producer idle clock and the receipt window
    /// survive the group snapshot codec, so an installed replica holds
    /// exactly the live replica's producer state.
    #[test]
    fn producer_last_seen_and_receipt_window_round_trip() {
        let mut machine = ursula_stream::StreamStateMachine::new();
        machine.apply(ursula_stream::StreamCommand::CreateBucket {
            bucket_id: "bucket".to_owned(),
        });
        let stream_id = BucketStreamId::new("bucket", "producers");
        machine.apply(ursula_stream::StreamCommand::CreateStream {
            stream_id: stream_id.clone(),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: bytes::Bytes::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            now_ms: 1,
        });
        for seq in 0..1_100u64 {
            for (producer_id, now_ms) in [("a", 10 + seq), ("b", 20 + seq)] {
                if producer_id == "b" && seq > 3 {
                    continue;
                }
                let response = machine.apply(ursula_stream::StreamCommand::Append {
                    stream_id: stream_id.clone(),
                    content_type: Some("application/octet-stream".to_owned()),
                    payload: bytes::Bytes::from_static(b"xy"),
                    close_after: false,
                    stream_seq: None,
                    producer: Some(ursula_stream::ProducerRequest {
                        producer_id: producer_id.to_owned(),
                        producer_epoch: 1,
                        producer_seq: seq,
                    }),
                    now_ms,
                });
                assert!(
                    matches!(response, ursula_stream::StreamResponse::Appended { .. }),
                    "{response:?}"
                );
            }
        }
        let stream_snapshot = machine.snapshot();
        let producers = &stream_snapshot.streams[0].producer_states;
        assert_eq!(producers[0].last_seen_ms, 10 + 1_099);
        assert_eq!(producers[1].last_seen_ms, 23);
        // Field 9 stays optional on the wire; an absent value is refused.
        let mut entry = stream_to_proto(stream_snapshot.streams[0].clone()).expect("encode entry");
        entry.producer_states[0].last_seen_ms = None;
        let error = stream_from_proto(entry).expect_err("absent last_seen_ms");
        assert!(error.to_string().contains("last_seen_ms"), "{error}");
        // So does entry field 16.
        let mut entry = stream_to_proto(stream_snapshot.streams[0].clone()).expect("encode entry");
        entry.retained_offset = None;
        let error = stream_from_proto(entry).expect_err("absent retained_offset");
        assert!(error.to_string().contains("retained_offset"), "{error}");
        let snapshot = GroupSnapshot {
            placement: ShardPlacement {
                core_id: CoreId(0),
                shard_id: ShardId(0),
                raft_group_id: RaftGroupId(0),
            },
            group_commit_index: 7,
            stream_snapshot,
            stream_append_counts: Vec::new(),
        };
        let bytes = group_snapshot_frames(Arc::new(snapshot.clone()))
            .collect::<Result<Vec<_>, _>>()
            .expect("encode frames")
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let decoded = decode_group_snapshot(&bytes).expect("decode frames");
        assert_eq!(decoded, snapshot);
        let restored = ursula_stream::StreamStateMachine::restore(decoded.stream_snapshot)
            .expect("restore decoded snapshot");
        assert_eq!(restored.snapshot(), machine.snapshot());
        assert_eq!(restored.state_gauges(), machine.state_gauges());
    }

    fn epoch_frame(epoch: u32) -> Bytes {
        encode_frame(proto::SnapshotFrameV1 {
            frame: Some(proto::snapshot_frame_v1::Frame::FormatEpoch(
                proto::FormatEpochV1 { epoch },
            )),
        })
        .expect("encode epoch frame")
    }

    fn header_v1() -> proto::SnapshotHeaderV1 {
        proto::SnapshotHeaderV1 {
            placement: Some(placement_to_proto(ShardPlacement {
                core_id: CoreId(0),
                shard_id: ShardId(0),
                raft_group_id: RaftGroupId(0),
            })),
            group_commit_index: 0,
            buckets: Vec::new(),
            erased_buckets: Vec::new(),
            next_cold_gc_seq: 0,
            bucket_usage: Vec::new(),
            committed_write_unit_bytes: Some(ursula_stream::COMMITTED_WRITE_UNIT_BYTES),
            last_created_at_ms: 0,
        }
    }

    fn header_frame() -> Bytes {
        encode_frame(proto::SnapshotFrameV1 {
            frame: Some(proto::snapshot_frame_v1::Frame::Header(header_v1())),
        })
        .expect("encode header")
    }

    fn footer_frame() -> Bytes {
        encode_frame(proto::SnapshotFrameV1 {
            frame: Some(proto::snapshot_frame_v1::Frame::Footer(
                proto::SnapshotFooterV1 {},
            )),
        })
        .expect("encode footer")
    }

    /// Format epoch 2 (E5): the epoch frame comes first and is checked before
    /// any other frame.
    #[test]
    fn decode_requires_a_leading_epoch_frame_of_this_epoch() {
        let epoch = ursula_stream::FORMAT_EPOCH;
        let valid = [epoch_frame(epoch), header_frame(), footer_frame()].concat();
        decode_group_snapshot(&valid).expect("an epoch-2 snapshot decodes");

        let cases = [
            (
                [header_frame(), footer_frame()].concat(),
                "no leading format-epoch frame",
            ),
            (
                [epoch_frame(epoch - 1), header_frame(), footer_frame()].concat(),
                "is format epoch 1",
            ),
            (
                [header_frame(), epoch_frame(epoch), footer_frame()].concat(),
                "no leading format-epoch frame",
            ),
            (
                [
                    epoch_frame(epoch),
                    header_frame(),
                    epoch_frame(epoch),
                    footer_frame(),
                ]
                .concat(),
                "not the first frame",
            ),
        ];
        for (bytes, expected) in cases {
            let error = decode_group_snapshot(&bytes).expect_err("refused");
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[test]
    fn rejects_missing_footer() {
        let header = encode_frame(proto::SnapshotFrameV1 {
            frame: Some(proto::snapshot_frame_v1::Frame::Header(
                proto::SnapshotHeaderV1 {
                    placement: Some(placement_to_proto(ShardPlacement {
                        core_id: CoreId(0),
                        shard_id: ShardId(0),
                        raft_group_id: RaftGroupId(0),
                    })),
                    group_commit_index: 0,
                    buckets: Vec::new(),
                    erased_buckets: Vec::new(),
                    next_cold_gc_seq: 0,
                    bucket_usage: Vec::new(),
                    committed_write_unit_bytes: Some(ursula_stream::COMMITTED_WRITE_UNIT_BYTES),
                    last_created_at_ms: 0,
                },
            )),
        })
        .expect("encode header");
        let header = [epoch_frame(ursula_stream::FORMAT_EPOCH), header].concat();

        assert!(matches!(
            decode_group_snapshot(&header),
            Err(SnapshotStoreError::Deserialize(_))
        ));
    }

    #[test]
    fn rejects_a_snapshot_with_a_different_or_absent_write_unit() {
        for (unit, expected) in [(Some(4096), "4096"), (None, "committed_write_unit_bytes")] {
            let error = decode_group_snapshot(&write_unit_snapshot(unit))
                .expect_err("unit mismatch must fail restore");
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    fn write_unit_snapshot(committed_write_unit_bytes: Option<u64>) -> Vec<u8> {
        let header = proto::SnapshotHeaderV1 {
            placement: Some(placement_to_proto(ShardPlacement {
                core_id: CoreId(0),
                shard_id: ShardId(0),
                raft_group_id: RaftGroupId(0),
            })),
            group_commit_index: 0,
            buckets: Vec::new(),
            erased_buckets: Vec::new(),
            next_cold_gc_seq: 0,
            bucket_usage: Vec::new(),
            committed_write_unit_bytes,
            last_created_at_ms: 0,
        };
        [
            epoch_frame(ursula_stream::FORMAT_EPOCH),
            encode_frame(proto::SnapshotFrameV1 {
                frame: Some(proto::snapshot_frame_v1::Frame::Header(header)),
            })
            .expect("encode header"),
            footer_frame(),
        ]
        .concat()
    }

    /// Bounded-state F16: a cold snapshot body travels through the codec as
    /// its object reference, never as inline bytes.
    #[test]
    fn cold_snapshot_reference_round_trips() {
        let mut machine = ursula_stream::StreamStateMachine::new();
        machine.apply(ursula_stream::StreamCommand::CreateBucket {
            bucket_id: "bucket".to_owned(),
        });
        let stream_id = BucketStreamId::new("bucket", "s");
        machine.apply(ursula_stream::StreamCommand::CreateStream {
            stream_id: stream_id.clone(),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: bytes::Bytes::from_static(b"ab"),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            now_ms: 1,
        });
        let object = ursula_stream::ExternalPayloadRef {
            s3_path: "bucket/s/external/a.bin".to_owned(),
            payload_len: 1 << 30,
            object_size: 1 << 30,
        };
        machine.apply(ursula_stream::StreamCommand::PublishSnapshotExternal {
            stream_id,
            snapshot_offset: 2,
            content_type: "application/octet-stream".to_owned(),
            object: object.clone(),
            digest: "d".to_owned(),
            now_ms: 2,
            expected_incarnation: None,
        });
        let entry = machine.snapshot().streams.remove(0);
        let encoded = stream_to_proto(entry.clone()).expect("encode entry");
        let visible = encoded.visible_snapshot.as_ref().expect("visible snapshot");
        assert!(visible.payload.is_empty());
        assert_eq!(visible.object.as_ref(), Some(&object));
        assert_eq!(
            stream_from_proto(encoded)
                .expect("decode entry")
                .visible_snapshot,
            entry.visible_snapshot
        );
    }
}
