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
use ursula_stream::ProducerAppendRecord;
use ursula_stream::ProducerReceipt;
use ursula_stream::ProducerSnapshot;
use ursula_stream::SharedColdObjectOwnersSnapshot;
use ursula_stream::StreamMessageRecord;
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
    let snapshot_write_unit = header
        .committed_write_unit_bytes
        .unwrap_or(ursula_stream::COMMITTED_WRITE_UNIT_BYTES);
    if header.feature_level > ursula_stream::MAX_SUPPORTED_FEATURE_LEVEL {
        return Err(SnapshotStoreError::Deserialize(format!(
            "snapshot feature level {} exceeds this binary's supported level {}; \
             a binary that cannot apply that level must not run this group",
            header.feature_level,
            ursula_stream::MAX_SUPPORTED_FEATURE_LEVEL
        )));
    }
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
            buckets: header.buckets,
            erased_buckets: header.erased_buckets,
            streams,
            pending_cold_gc,
            next_cold_gc_seq: header.next_cold_gc_seq,
            shared_cold_object_owners: header
                .shared_cold_object_owners
                .into_iter()
                .map(|record| SharedColdObjectOwnersSnapshot {
                    s3_path: record.s3_path,
                    bucket_ids: record.bucket_ids,
                })
                .collect(),
            bucket_usage: header
                .bucket_usage
                .into_iter()
                .map(bucket_usage_from_proto)
                .collect(),
            feature_level: header.feature_level,
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
            shared_cold_object_owners: stream_snapshot
                .shared_cold_object_owners
                .iter()
                .map(|record| proto::SharedColdObjectOwnersV1 {
                    s3_path: record.s3_path.clone(),
                    bucket_ids: record.bucket_ids.clone(),
                })
                .collect(),
            bucket_usage: stream_snapshot
                .bucket_usage
                .iter()
                .cloned()
                .map(bucket_usage_to_proto)
                .collect(),
            committed_write_unit_bytes: Some(ursula_stream::COMMITTED_WRITE_UNIT_BYTES),
            feature_level: stream_snapshot.feature_level,
            last_created_at_ms: stream_snapshot.last_created_at_ms,
        }
    }
}

impl Iterator for GroupSnapshotFrameIter {
    type Item = Result<Bytes, SnapshotStoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        let snapshot = &*self.snapshot;
        let frame = if self.header {
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
    let (first_record, record_offsets) = entry
        .record_index
        .as_ref()
        .map(|index| {
            index.range().map(|range| {
                (
                    Some(range.first_record),
                    index.dense_offsets().iter().copied().collect::<Vec<_>>(),
                )
            })
        })
        .transpose()
        .map_err(|err| SnapshotStoreError::Serialize(format!("record index: {err:?}")))?
        .unwrap_or((None, Vec::new()));
    // F1 (level 2): marks are written only when the index has sealed records,
    // so all-dense entries stay byte-identical to earlier releases.
    let (record_mark_records, record_mark_offsets, dense_first_record) = entry
        .record_index
        .as_ref()
        .filter(|index| !index.marks().is_empty())
        .map(|index| {
            (
                index.marks().iter().map(|mark| mark.record).collect(),
                index.marks().iter().map(|mark| mark.offset).collect(),
                Some(index.dense_first_record()),
            )
        })
        .unwrap_or_default();
    Ok(proto::StreamSnapshotEntryV1 {
        metadata: Some(metadata_to_proto(entry.metadata)),
        hot_start_offset: entry.hot_start_offset,
        payload: entry.payload.into(),
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
        visible_snapshot: entry.visible_snapshot.map(visible_snapshot_to_proto),
        producer_states: entry
            .producer_states
            .into_iter()
            .map(producer_to_proto)
            .collect(),
        first_record,
        record_offsets,
        retained_offset: entry.retained_offset,
        record_mark_records,
        record_mark_offsets,
        dense_first_record,
        hot_append_starts: entry.hot_append_starts,
    })
}

fn stream_from_proto(
    entry: proto::StreamSnapshotEntryV1,
) -> Result<StreamSnapshotEntry, SnapshotStoreError> {
    let retained_offset = entry.retained_offset.unwrap_or_else(|| {
        entry
            .visible_snapshot
            .as_ref()
            .map(|snapshot| snapshot.offset)
            .unwrap_or(0)
    });
    let tail_offset = entry
        .metadata
        .as_ref()
        .map(|metadata| metadata.tail_offset)
        .unwrap_or(0);
    if entry.record_mark_records.len() != entry.record_mark_offsets.len() {
        return Err(SnapshotStoreError::Deserialize(
            "record index: mark record and offset lists differ in length".to_owned(),
        ));
    }
    let record_index = entry
        .first_record
        .map(|first_record| {
            ursula_stream::StreamRecordIndex::restore_sparse(
                first_record,
                entry
                    .record_mark_records
                    .iter()
                    .zip(&entry.record_mark_offsets)
                    .map(|(record, offset)| ursula_stream::RecordMark {
                        record: *record,
                        offset: *offset,
                    })
                    .collect(),
                entry.dense_first_record.unwrap_or(first_record),
                entry.record_offsets.clone(),
                retained_offset,
                tail_offset,
            )
        })
        .transpose()
        .map_err(|err| SnapshotStoreError::Deserialize(format!("record index: {err:?}")))?;
    Ok(StreamSnapshotEntry {
        metadata: metadata_from_proto(required(entry.metadata, "snapshot stream metadata")?)?,
        hot_start_offset: entry.hot_start_offset,
        payload: entry.payload.to_vec(),
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
        hot_append_starts: entry.hot_append_starts,
        record_index,
        retained_offset: entry.retained_offset,
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
    match proto::StreamStatusV1::try_from(status).map_err(|_| {
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
        object_offset: 0,
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
        last_items: producer
            .last_items
            .into_iter()
            .map(producer_append_record_to_proto)
            .collect(),
        receipts: producer
            .receipts
            .into_iter()
            .map(producer_receipt_to_proto)
            .collect(),
        last_seen_ms: producer.last_seen_ms,
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
        receipts: producer
            .receipts
            .into_iter()
            .map(producer_receipt_from_proto)
            .collect(),
        last_seen_ms: producer.last_seen_ms,
    }
}

fn producer_receipt_to_proto(receipt: ProducerReceipt) -> proto::ProducerReceiptV1 {
    proto::ProducerReceiptV1 {
        producer_seq: receipt.producer_seq,
        start_offset: receipt.start_offset,
        next_offset: receipt.next_offset,
        closed: receipt.closed,
        items: receipt
            .items
            .into_iter()
            .map(producer_append_record_to_proto)
            .collect(),
    }
}

fn producer_receipt_from_proto(receipt: proto::ProducerReceiptV1) -> ProducerReceipt {
    ProducerReceipt {
        producer_seq: receipt.producer_seq,
        start_offset: receipt.start_offset,
        next_offset: receipt.next_offset,
        closed: receipt.closed,
        items: receipt
            .items
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
        record_start: record.record_start,
        record_next: record.record_next,
    }
}

fn producer_append_record_from_proto(
    record: proto::ProducerAppendRecordV1,
) -> ProducerAppendRecord {
    ProducerAppendRecord {
        start_offset: record.start_offset,
        next_offset: record.next_offset,
        closed: record.closed,
        record_start: record.record_start,
        record_next: record.record_next,
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
                buckets: vec!["bucket".to_owned()],
                erased_buckets: vec!["erased-bucket".to_owned()],
                streams: Vec::new(),
                pending_cold_gc: vec![
                    ColdGcEntry {
                        seq: 7,
                        bucket_id: "bucket".to_owned(),
                        not_before_ms: 0,
                        target: ColdGcTarget::Stream(BucketStreamId::new("bucket", "legacy")),
                        cold_generation: None,
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
                shared_cold_object_owners: vec![SharedColdObjectOwnersSnapshot {
                    s3_path: "_packs/legacy.bin".to_owned(),
                    bucket_ids: vec!["bucket".to_owned(), "other-bucket".to_owned()],
                }],
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
                feature_level: ursula_stream::MAX_SUPPORTED_FEATURE_LEVEL,
                last_created_at_ms: 1_234,
            },
            stream_append_counts: vec![StreamAppendCount {
                stream_id: BucketStreamId {
                    bucket_id: "bucket".to_owned(),
                    stream_id: "stream".to_owned(),
                    affinity_key: None,
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

    /// Bounded-state F3: the producer idle clock and the level-1 receipt
    /// window survive the group snapshot codec, so an installed replica
    /// holds exactly the live replica's producer state.
    #[test]
    fn producer_last_seen_and_receipt_window_round_trip() {
        let mut machine = ursula_stream::StreamStateMachine::new();
        machine.apply(ursula_stream::StreamCommand::SetFeatureLevel { level: 1 });
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
                    record_match: None,
                });
                assert!(
                    matches!(response, ursula_stream::StreamResponse::Appended { .. }),
                    "{response:?}"
                );
            }
        }
        let stream_snapshot = machine.snapshot();
        let producers = &stream_snapshot.streams[0].producer_states;
        assert_eq!(producers[0].last_seen_ms, Some(10 + 1_099));
        assert_eq!(producers[1].last_seen_ms, Some(23));
        assert!(
            producers
                .iter()
                .all(|producer| producer.last_items.is_empty())
        );
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

    /// Bounded-state F1 (RC-16): sparse record marks survive the group
    /// snapshot codec in stream entry fields 17-19, an all-dense entry
    /// writes none of them, and a snapshot below level 2 that carries marks
    /// is refused at restore.
    #[test]
    fn sparse_record_marks_round_trip() {
        let mut machine = ursula_stream::StreamStateMachine::new();
        machine.apply(ursula_stream::StreamCommand::SetFeatureLevel {
            level: ursula_stream::FEATURE_LEVEL_SPARSE_MARKS,
        });
        machine.apply(ursula_stream::StreamCommand::CreateBucket {
            bucket_id: "bucket".to_owned(),
        });
        let stream_id = BucketStreamId::new("bucket", "marks");
        machine.apply(ursula_stream::StreamCommand::CreateStream {
            stream_id: stream_id.clone(),
            content_type: "application/json".to_owned(),
            initial_payload: bytes::Bytes::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            now_ms: 1,
        });
        let record = format!("\"{}\"\n", "x".repeat(997));
        let payload = record.repeat(3_000);
        let response = machine.apply(ursula_stream::StreamCommand::Append {
            stream_id: stream_id.clone(),
            content_type: Some("application/json".to_owned()),
            payload: bytes::Bytes::from(payload.clone().into_bytes()),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 2,
            record_match: None,
        });
        assert!(matches!(
            response,
            ursula_stream::StreamResponse::Appended { .. }
        ));
        let dense_entry = |machine: &ursula_stream::StreamStateMachine| {
            stream_to_proto(machine.snapshot().streams[0].clone()).expect("encode entry")
        };
        let entry = dense_entry(&machine);
        assert!(entry.record_mark_records.is_empty() && entry.dense_first_record.is_none());
        machine.apply(ursula_stream::StreamCommand::FlushCold {
            stream_id: stream_id.clone(),
            chunk: ursula_stream::ColdChunkRef {
                start_offset: 0,
                end_offset: 2_000_000,
                s3_path: "bucket/marks/chunks/0.bin".to_owned(),
                object_size: 2_000_000,
                ..Default::default()
            },
            cold_generation: None,
        });
        let entry = dense_entry(&machine);
        assert_eq!(entry.record_mark_records, vec![0, 1_049]);
        assert_eq!(entry.record_mark_offsets, vec![0, 1_049_000]);
        assert_eq!(entry.dense_first_record, Some(2_000));
        assert_eq!(entry.record_offsets.len(), 1_000);

        let snapshot = GroupSnapshot {
            placement: ShardPlacement {
                core_id: CoreId(0),
                shard_id: ShardId(0),
                raft_group_id: RaftGroupId(0),
            },
            group_commit_index: 3,
            stream_snapshot: machine.snapshot(),
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
        let restored = ursula_stream::StreamStateMachine::restore(decoded.stream_snapshot.clone())
            .expect("restore decoded snapshot");
        assert_eq!(restored.snapshot(), machine.snapshot());
        assert_eq!(restored.state_gauges(), machine.state_gauges());

        let mut below = decoded.stream_snapshot;
        below.feature_level = ursula_stream::FEATURE_LEVEL_KEYED_STREAMS;
        assert!(ursula_stream::StreamStateMachine::restore(below).is_err());
    }

    /// Bounded-state F4b (level 4): the codec writes no message records
    /// (field 10), binary streams carry their hot append starts in field 20,
    /// JSON streams carry neither, and the decoded snapshot restores to the
    /// live state with the same bootstrap answers.
    #[test]
    fn level_4_append_starts_round_trip_without_message_records() {
        let mut machine = ursula_stream::StreamStateMachine::new();
        machine.apply(ursula_stream::StreamCommand::SetFeatureLevel {
            level: ursula_stream::FEATURE_LEVEL_HOT_REPRESENTATION,
        });
        machine.apply(ursula_stream::StreamCommand::CreateBucket {
            bucket_id: "bucket".to_owned(),
        });
        let binary = BucketStreamId::new("bucket", "binary");
        let json = BucketStreamId::new("bucket", "json");
        for (stream_id, content_type) in [
            (&binary, "application/octet-stream"),
            (&json, "application/json"),
        ] {
            machine.apply(ursula_stream::StreamCommand::CreateStream {
                stream_id: stream_id.clone(),
                content_type: content_type.to_owned(),
                initial_payload: bytes::Bytes::new(),
                close_after: false,
                stream_seq: None,
                producer: None,
                stream_ttl_seconds: None,
                stream_expires_at_ms: None,
                now_ms: 1,
            });
            for index in 0..4u64 {
                let payload = if content_type == "application/json" {
                    format!("{{\"i\":{index}}}\n").into_bytes()
                } else {
                    vec![b'a'; 8]
                };
                let response = machine.apply(ursula_stream::StreamCommand::Append {
                    stream_id: stream_id.clone(),
                    content_type: Some(content_type.to_owned()),
                    payload: bytes::Bytes::from(payload),
                    close_after: false,
                    stream_seq: None,
                    producer: None,
                    now_ms: 2,
                    record_match: None,
                });
                assert!(matches!(
                    response,
                    ursula_stream::StreamResponse::Appended { .. }
                ));
            }
        }
        // Flush into the second binary message: its start leaves the hot
        // buffer with it.
        machine.apply(ursula_stream::StreamCommand::FlushCold {
            stream_id: binary.clone(),
            chunk: ursula_stream::ColdChunkRef {
                start_offset: 0,
                end_offset: 12,
                s3_path: "bucket/binary/chunks/0.bin".to_owned(),
                object_size: 12,
                ..Default::default()
            },
            cold_generation: None,
        });
        let entries = machine
            .snapshot()
            .streams
            .into_iter()
            .map(|entry| stream_to_proto(entry).expect("encode entry"))
            .collect::<Vec<_>>();
        let binary_entry = &entries[0];
        assert!(binary_entry.message_records.is_empty());
        assert_eq!(binary_entry.hot_append_starts, vec![16, 24]);
        let json_entry = &entries[1];
        assert!(json_entry.message_records.is_empty());
        assert!(json_entry.hot_append_starts.is_empty());

        let snapshot = GroupSnapshot {
            placement: ShardPlacement {
                core_id: CoreId(0),
                shard_id: ShardId(0),
                raft_group_id: RaftGroupId(0),
            },
            group_commit_index: 9,
            stream_snapshot: machine.snapshot(),
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
        let restored = ursula_stream::StreamStateMachine::restore(decoded.stream_snapshot.clone())
            .expect("restore decoded snapshot");
        assert_eq!(restored.snapshot(), machine.snapshot());
        assert_eq!(restored.state_gauges(), machine.state_gauges());
        for stream_id in [&binary, &json] {
            assert_eq!(
                restored.bootstrap_plan(stream_id),
                machine.bootstrap_plan(stream_id)
            );
        }

        // A level-3 binary refuses append starts.
        let mut below = decoded.stream_snapshot;
        below.feature_level = ursula_stream::FEATURE_LEVEL_EXTERNAL_LOCATORS;
        assert!(ursula_stream::StreamStateMachine::restore(below).is_err());
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
                    shared_cold_object_owners: Vec::new(),
                    bucket_usage: Vec::new(),
                    committed_write_unit_bytes: None,
                    feature_level: 0,
                    last_created_at_ms: 0,
                },
            )),
        })
        .expect("encode header");

        assert!(matches!(
            decode_group_snapshot(&header),
            Err(SnapshotStoreError::Deserialize(_))
        ));
    }

    #[test]
    fn rejects_a_snapshot_with_a_different_write_unit() {
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
            shared_cold_object_owners: Vec::new(),
            bucket_usage: Vec::new(),
            committed_write_unit_bytes: Some(4096),
            feature_level: 0,
            last_created_at_ms: 0,
        };
        let bytes = [
            encode_frame(proto::SnapshotFrameV1 {
                frame: Some(proto::snapshot_frame_v1::Frame::Header(header)),
            })
            .expect("encode header"),
            encode_frame(proto::SnapshotFrameV1 {
                frame: Some(proto::snapshot_frame_v1::Frame::Footer(
                    proto::SnapshotFooterV1 {},
                )),
            })
            .expect("encode footer"),
        ]
        .concat();

        let error = decode_group_snapshot(&bytes).expect_err("unit mismatch must fail restore");
        assert!(error.to_string().contains("4096"), "{error}");
    }

    fn header_only_snapshot(feature_level: u32) -> Vec<u8> {
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
            shared_cold_object_owners: Vec::new(),
            bucket_usage: Vec::new(),
            committed_write_unit_bytes: None,
            feature_level,
            last_created_at_ms: 0,
        };
        [
            encode_frame(proto::SnapshotFrameV1 {
                frame: Some(proto::snapshot_frame_v1::Frame::Header(header)),
            })
            .expect("encode header"),
            encode_frame(proto::SnapshotFrameV1 {
                frame: Some(proto::snapshot_frame_v1::Frame::Footer(
                    proto::SnapshotFooterV1 {},
                )),
            })
            .expect("encode footer"),
        ]
        .concat()
    }

    /// The header as written before C0, without field 10.
    #[derive(Clone, PartialEq, prost::Message)]
    struct LegacySnapshotHeaderV1 {
        #[prost(message, optional, tag = "1")]
        placement: Option<proto::ShardPlacementV1>,
        #[prost(uint64, tag = "2")]
        group_commit_index: u64,
        #[prost(string, repeated, tag = "3")]
        buckets: Vec<String>,
    }

    #[test]
    fn legacy_snapshot_without_feature_level_decodes_as_level_zero() {
        let legacy_header = LegacySnapshotHeaderV1 {
            placement: Some(placement_to_proto(ShardPlacement {
                core_id: CoreId(0),
                shard_id: ShardId(0),
                raft_group_id: RaftGroupId(3),
            })),
            group_commit_index: 5,
            buckets: vec!["bucket".to_owned()],
        };
        // Frame field 1 (header) carrying the legacy message, length-delimited
        // exactly as `encode_frame` lays out a frame.
        let mut frame = Vec::new();
        prost::encoding::message::encode(1, &legacy_header, &mut frame);
        let mut bytes = Vec::new();
        prost::encoding::encode_varint(frame.len() as u64, &mut bytes);
        bytes.extend_from_slice(&frame);
        bytes.extend(
            encode_frame(proto::SnapshotFrameV1 {
                frame: Some(proto::snapshot_frame_v1::Frame::Footer(
                    proto::SnapshotFooterV1 {},
                )),
            })
            .expect("encode footer"),
        );

        let decoded = decode_group_snapshot(&bytes).expect("decode legacy snapshot");
        assert_eq!(decoded.stream_snapshot.feature_level, 0);
        assert_eq!(decoded.stream_snapshot.last_created_at_ms, 0);
        assert_eq!(decoded.stream_snapshot.buckets, vec!["bucket".to_owned()]);
        assert_eq!(decoded.group_commit_index, 5);
    }

    #[test]
    fn feature_level_round_trips_through_the_header() {
        let decoded = decode_group_snapshot(&header_only_snapshot(1)).expect("decode level 1");
        assert_eq!(decoded.stream_snapshot.feature_level, 1);
        let reencoded = group_snapshot_frames(Arc::new(decoded.clone()))
            .collect::<Result<Vec<_>, _>>()
            .expect("encode frames")
            .concat();
        assert_eq!(
            decode_group_snapshot(&reencoded).expect("decode again"),
            decoded
        );
    }

    #[test]
    fn rejects_a_snapshot_above_the_supported_feature_level() {
        let err = decode_group_snapshot(&header_only_snapshot(
            ursula_stream::MAX_SUPPORTED_FEATURE_LEVEL + 1,
        ))
        .expect_err("future level rejected");
        assert!(
            matches!(&err, SnapshotStoreError::Deserialize(message) if message.contains("feature level")),
            "{err:?}"
        );
    }

    /// Bounded-state F16 (level 5): a cold snapshot body travels through
    /// the codec as its object reference, never as inline bytes.
    #[test]
    fn cold_snapshot_reference_round_trips() {
        let mut machine = ursula_stream::StreamStateMachine::new();
        machine.apply(ursula_stream::StreamCommand::SetFeatureLevel {
            level: ursula_stream::FEATURE_LEVEL_COLD_SNAPSHOTS,
        });
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
