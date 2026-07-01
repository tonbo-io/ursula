use std::io::Cursor;

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
use ursula_stream::ProducerAppendRecord;
use ursula_stream::ProducerSnapshot;
use ursula_stream::StreamAttrs;
use ursula_stream::StreamIntegritySnapshot;
use ursula_stream::StreamMessageRecord;
use ursula_stream::StreamMetadata;
use ursula_stream::StreamSnapshot;
use ursula_stream::StreamSnapshotEntry;
use ursula_stream::StreamStatus;
use ursula_stream::StreamVisibleSnapshot;

use crate::codec::placement_to_proto;

pub(crate) fn group_snapshot_frames(snapshot: GroupSnapshot) -> SnapshotBytesIterator {
    Box::new(GroupSnapshotFrameIter::new(snapshot))
}

pub(crate) fn decode_group_snapshot(bytes: &[u8]) -> Result<GroupSnapshot, SnapshotStoreError> {
    let mut cursor = Cursor::new(bytes);
    let mut header = None;
    let mut streams = Vec::new();
    let mut stream_append_counts = Vec::new();
    let mut pending_cold_gc = Vec::new();
    let mut footer_seen = false;

    while cursor.has_remaining() {
        let frame = proto::SnapshotFrameV1::decode_length_delimited(&mut cursor)
            .map_err(|err| SnapshotStoreError::Deserialize(format!("snapshot frame: {err}")))?;
        let frame = required(frame.frame, "snapshot frame")?;
        match frame {
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

    Ok(GroupSnapshot {
        placement: placement_from_proto(required(header.placement, "snapshot header placement")?),
        group_commit_index: header.group_commit_index,
        stream_snapshot: StreamSnapshot {
            buckets: header.buckets,
            streams,
            pending_cold_gc,
            next_cold_gc_seq: header.next_cold_gc_seq,
        },
        stream_append_counts,
    })
}

struct GroupSnapshotFrameIter {
    header: Option<proto::SnapshotHeaderV1>,
    streams: std::vec::IntoIter<StreamSnapshotEntry>,
    append_counts: std::vec::IntoIter<StreamAppendCount>,
    cold_gc: std::vec::IntoIter<ColdGcEntry>,
    footer: bool,
}

impl GroupSnapshotFrameIter {
    fn new(snapshot: GroupSnapshot) -> Self {
        let GroupSnapshot {
            placement,
            group_commit_index,
            stream_snapshot,
            stream_append_counts,
        } = snapshot;
        let StreamSnapshot {
            buckets,
            streams,
            pending_cold_gc,
            next_cold_gc_seq,
        } = stream_snapshot;
        Self {
            header: Some(proto::SnapshotHeaderV1 {
                placement: Some(placement_to_proto(placement)),
                group_commit_index,
                buckets,
                next_cold_gc_seq,
            }),
            streams: streams.into_iter(),
            append_counts: stream_append_counts.into_iter(),
            cold_gc: pending_cold_gc.into_iter(),
            footer: true,
        }
    }
}

impl Iterator for GroupSnapshotFrameIter {
    type Item = Result<Bytes, SnapshotStoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        let frame = if let Some(header) = self.header.take() {
            proto::snapshot_frame_v1::Frame::Header(header)
        } else if let Some(stream) = self.streams.next() {
            match stream_to_proto(stream) {
                Ok(stream) => proto::snapshot_frame_v1::Frame::Stream(Box::new(stream)),
                Err(err) => return Some(Err(err)),
            }
        } else if let Some(append_count) = self.append_counts.next() {
            proto::snapshot_frame_v1::Frame::AppendCount(append_count_to_proto(append_count))
        } else if let Some(cold_gc) = self.cold_gc.next() {
            proto::snapshot_frame_v1::Frame::ColdGc(cold_gc_to_proto(cold_gc))
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
        attrs_json: entry
            .attrs
            .map(|attrs| serde_json::to_vec(&attrs))
            .transpose()
            .map_err(|err| SnapshotStoreError::Serialize(format!("stream attrs: {err}")))?,
        hot_start_offset: entry.hot_start_offset,
        payload: entry.payload,
        hot_segments: entry
            .hot_segments
            .into_iter()
            .map(hot_segment_to_proto)
            .collect(),
        cold_frontier_offset: entry.cold_frontier_offset,
        cold_index_generation: entry.cold_index_generation,
        cold_chunks: entry.cold_chunks,
        external_segments: entry
            .external_segments
            .into_iter()
            .map(object_ref_to_proto)
            .collect(),
        message_records: entry
            .message_records
            .into_iter()
            .map(message_record_to_proto)
            .collect(),
        integrity: Some(integrity_to_proto(entry.integrity)),
        visible_snapshot: entry.visible_snapshot.map(visible_snapshot_to_proto),
        producer_states: entry
            .producer_states
            .into_iter()
            .map(producer_to_proto)
            .collect(),
    })
}

fn stream_from_proto(
    entry: proto::StreamSnapshotEntryV1,
) -> Result<StreamSnapshotEntry, SnapshotStoreError> {
    Ok(StreamSnapshotEntry {
        metadata: metadata_from_proto(required(entry.metadata, "snapshot stream metadata")?)?,
        attrs: entry
            .attrs_json
            .map(|bytes| serde_json::from_slice::<StreamAttrs>(&bytes))
            .transpose()
            .map_err(|err| SnapshotStoreError::Deserialize(format!("stream attrs: {err}")))?,
        hot_start_offset: entry.hot_start_offset,
        payload: entry.payload,
        hot_segments: entry
            .hot_segments
            .into_iter()
            .map(hot_segment_from_proto)
            .collect::<Result<Vec<_>, _>>()?,
        cold_frontier_offset: entry.cold_frontier_offset,
        cold_index_generation: entry.cold_index_generation,
        cold_chunks: entry.cold_chunks,
        external_segments: entry
            .external_segments
            .into_iter()
            .map(object_ref_from_proto)
            .collect(),
        message_records: entry
            .message_records
            .into_iter()
            .map(message_record_from_proto)
            .collect(),
        integrity: integrity_from_proto(required(entry.integrity, "snapshot stream integrity")?),
        visible_snapshot: entry.visible_snapshot.map(visible_snapshot_from_proto),
        producer_states: entry
            .producer_states
            .into_iter()
            .map(producer_from_proto)
            .collect(),
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
        forked_from: metadata.forked_from.map(Into::into),
        fork_offset: metadata.fork_offset,
        fork_ref_count: metadata.fork_ref_count,
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
        forked_from: metadata.forked_from.map(Into::into),
        fork_offset: metadata.fork_offset,
        fork_ref_count: metadata.fork_ref_count,
    })
}

fn status_to_proto(status: StreamStatus) -> proto::StreamStatusV1 {
    match status {
        StreamStatus::Open => proto::StreamStatusV1::StreamStatusOpen,
        StreamStatus::Closed => proto::StreamStatusV1::StreamStatusClosed,
        StreamStatus::SoftDeleted => proto::StreamStatusV1::StreamStatusSoftDeleted,
    }
}

fn status_from_proto(status: i32) -> Result<StreamStatus, SnapshotStoreError> {
    match proto::StreamStatusV1::try_from(status).map_err(|_| {
        SnapshotStoreError::Deserialize(format!("invalid stream status value {status}"))
    })? {
        proto::StreamStatusV1::StreamStatusOpen => Ok(StreamStatus::Open),
        proto::StreamStatusV1::StreamStatusClosed => Ok(StreamStatus::Closed),
        proto::StreamStatusV1::StreamStatusSoftDeleted => Ok(StreamStatus::SoftDeleted),
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
        payload_start: usize::try_from(segment.payload_start).map_err(|_| {
            SnapshotStoreError::Deserialize(format!(
                "hot segment payload_start {} does not fit usize",
                segment.payload_start
            ))
        })?,
        payload_end: usize::try_from(segment.payload_end).map_err(|_| {
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
    }
}

fn message_record_to_proto(record: StreamMessageRecord) -> proto::StreamMessageRecordV1 {
    proto::StreamMessageRecordV1 {
        start_offset: record.start_offset,
        end_offset: record.end_offset,
    }
}

fn message_record_from_proto(record: proto::StreamMessageRecordV1) -> StreamMessageRecord {
    StreamMessageRecord {
        start_offset: record.start_offset,
        end_offset: record.end_offset,
    }
}

fn integrity_to_proto(integrity: StreamIntegritySnapshot) -> proto::StreamIntegritySnapshotV1 {
    proto::StreamIntegritySnapshotV1 {
        live_setsum: integrity.live_setsum,
        evicted_setsum: integrity.evicted_setsum,
        total_setsum: integrity.total_setsum,
        live_start_offset: integrity.live_start_offset,
        tail_offset: integrity.tail_offset,
        live_records: integrity.live_records,
        evicted_records: integrity.evicted_records,
        total_records: integrity.total_records,
    }
}

fn integrity_from_proto(integrity: proto::StreamIntegritySnapshotV1) -> StreamIntegritySnapshot {
    StreamIntegritySnapshot {
        live_setsum: integrity.live_setsum,
        evicted_setsum: integrity.evicted_setsum,
        total_setsum: integrity.total_setsum,
        live_start_offset: integrity.live_start_offset,
        tail_offset: integrity.tail_offset,
        live_records: integrity.live_records,
        evicted_records: integrity.evicted_records,
        total_records: integrity.total_records,
    }
}

fn visible_snapshot_to_proto(snapshot: StreamVisibleSnapshot) -> proto::StreamVisibleSnapshotV1 {
    proto::StreamVisibleSnapshotV1 {
        offset: snapshot.offset,
        content_type: snapshot.content_type,
        payload: snapshot.payload,
    }
}

fn visible_snapshot_from_proto(snapshot: proto::StreamVisibleSnapshotV1) -> StreamVisibleSnapshot {
    StreamVisibleSnapshot {
        offset: snapshot.offset,
        content_type: snapshot.content_type,
        payload: snapshot.payload,
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
        last_items: producer
            .last_items
            .into_iter()
            .map(producer_append_record_to_proto)
            .collect(),
    }
}

fn producer_from_proto(producer: proto::ProducerSnapshotV1) -> ProducerSnapshot {
    ProducerSnapshot {
        producer_id: producer.producer_id,
        producer_epoch: producer.producer_epoch,
        producer_seq: producer.producer_seq,
        last_start_offset: producer.last_start_offset,
        last_next_offset: producer.last_next_offset,
        last_closed: producer.last_closed,
        last_items: producer
            .last_items
            .into_iter()
            .map(producer_append_record_from_proto)
            .collect(),
    }
}

fn producer_append_record_to_proto(record: ProducerAppendRecord) -> proto::ProducerAppendRecordV1 {
    proto::ProducerAppendRecordV1 {
        start_offset: record.start_offset,
        next_offset: record.next_offset,
        closed: record.closed,
    }
}

fn producer_append_record_from_proto(
    record: proto::ProducerAppendRecordV1,
) -> ProducerAppendRecord {
    ProducerAppendRecord {
        start_offset: record.start_offset,
        next_offset: record.next_offset,
        closed: record.closed,
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
                buckets: vec!["bucket".to_owned()],
                streams: Vec::new(),
                pending_cold_gc: Vec::new(),
                next_cold_gc_seq: 9,
            },
            stream_append_counts: vec![StreamAppendCount {
                stream_id: BucketStreamId {
                    bucket_id: "bucket".to_owned(),
                    stream_id: "stream".to_owned(),
                },
                append_count: 3,
            }],
        };
        let bytes = group_snapshot_frames(snapshot.clone())
            .collect::<Result<Vec<_>, _>>()
            .expect("encode frames")
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        let decoded = decode_group_snapshot(&bytes).expect("decode frames");

        assert_eq!(decoded, snapshot);
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
                    next_cold_gc_seq: 0,
                },
            )),
        })
        .expect("encode header");

        assert!(matches!(
            decode_group_snapshot(&header),
            Err(SnapshotStoreError::Deserialize(_))
        ));
    }
}
