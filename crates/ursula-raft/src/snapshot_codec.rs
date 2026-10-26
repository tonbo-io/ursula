use std::io::Cursor;
use std::num::NonZeroU64;
use std::sync::Arc;

use bytes::Buf;
use bytes::Bytes;
use prost::Message;
use ursula_proto as proto;
use ursula_runtime::GroupSnapshot;
use ursula_runtime::SnapshotBytesIterator;
use ursula_runtime::SnapshotStoreError;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;
use ursula_stream::BucketRetired;
use ursula_stream::ClosedEpoch;
use ursula_stream::ColdGcEntry;
use ursula_stream::ColdGcTarget;
use ursula_stream::CommittedUsage;
use ursula_stream::EndRecord;
use ursula_stream::HistoryEvent;
use ursula_stream::HotPayloadSegment;
use ursula_stream::InheritedHistory;
use ursula_stream::LastWrite;
use ursula_stream::ObjectPayloadRef;
use ursula_stream::Opened;
use ursula_stream::ProducerReceipt;
use ursula_stream::ProducerSnapshot;
use ursula_stream::StreamEnd;
use ursula_stream::StreamMetadata;
use ursula_stream::StreamSnapshot;
use ursula_stream::StreamSnapshotEntry;
use ursula_stream::StreamStatus;
use ursula_stream::StreamVisibleSnapshot;
use ursula_stream::UsageSnapshot;
use ursula_stream::UsageSnapshotError;
use ursula_stream::VersionCounters;
use ursula_stream::VersionUsage;
use ursula_stream::WriteDigest;
use ursula_stream::WriteReceipt;

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
    DECODE_CALLS.with(|calls| calls.set(calls.get().saturating_add(1)));
    let mut cursor = Cursor::new(bytes);
    let mut header = None;
    let mut streams = Vec::new();
    let mut stream_usages = Vec::new();
    let mut ends = Vec::new();
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
                return Err(SnapshotStoreError::UnsupportedVersion {
                    found: Some(value.epoch),
                });
            }
        }
        _ => return Err(SnapshotStoreError::UnsupportedVersion { found: None }),
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
                let (entry, usage) = stream_from_proto(*value)?;
                streams.push(entry);
                stream_usages.push(usage);
            }
            proto::snapshot_frame_v1::Frame::EndRecord(value) => {
                ends.push(end_record_from_proto(*value)?);
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

    // Header fields 13, 14 and 16 stay `optional` on the wire; every writer
    // sets them, so an absent value is corruption (F20).
    let usage = UsageSnapshot {
        genesis: required(header.genesis, "snapshot header genesis")?,
        usage_seq: required(header.usage_seq, "snapshot header usage_seq")?,
        retired_through: required(header.retired_through, "snapshot header retired_through")?,
        retired: header
            .retired
            .into_iter()
            .map(retired_from_proto)
            .collect::<Result<Vec<_>, _>>()?,
        streams: stream_usages,
        ends,
    };
    Ok(GroupSnapshot {
        placement: placement_from_proto(required(header.placement, "snapshot header placement")?),
        group_commit_index: header.group_commit_index,
        stream_snapshot: StreamSnapshot {
            format_version: ursula_stream::BACKUP_FORMAT_VERSION,
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
            history: header.history.map(history_from_proto).transpose()?,
        },
        usage,
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
    next_cold_gc: usize,
    next_end_record: usize,
    footer: bool,
}

impl GroupSnapshotFrameIter {
    fn new(snapshot: Arc<GroupSnapshot>) -> Self {
        Self {
            snapshot,
            format_epoch: true,
            header: true,
            next_stream: 0,
            next_cold_gc: 0,
            next_end_record: 0,
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
            history: stream_snapshot.history.map(history_to_proto),
            usage_seq: Some(snapshot.usage.usage_seq),
            retired_through: Some(snapshot.usage.retired_through),
            retired: snapshot
                .usage
                .retired
                .iter()
                .map(retired_to_proto)
                .collect(),
            genesis: Some(snapshot.usage.genesis),
        }
    }
}

fn history_to_proto(history: HistoryEvent) -> proto::HistoryEventV1 {
    proto::HistoryEventV1 {
        id: history.id.get(),
        event_index: history.event_index,
        imported: history
            .imported
            .map(|digest| Bytes::copy_from_slice(&digest)),
        kept: Some(history.kept),
        previous: Some(history.previous),
        leader_term: Some(history.leader.term),
        leader_node: Some(history.leader.node_id),
    }
}

/// A history id is never 0, and a backup digest is 32 bytes.
fn history_from_proto(history: proto::HistoryEventV1) -> Result<HistoryEvent, SnapshotStoreError> {
    Ok(HistoryEvent {
        id: std::num::NonZeroU64::new(history.id).ok_or(SnapshotStoreError::HistoryIdZero)?,
        event_index: history.event_index,
        kept: required(history.kept, "snapshot history event kept index")?,
        previous: required(
            history.previous,
            "snapshot history event's replaced history",
        )?,
        leader: ursula_stream::HistoryLeader {
            term: required(history.leader_term, "snapshot history event's leader term")?,
            node_id: required(history.leader_node, "snapshot history event's leader node")?,
        },
        imported: history
            .imported
            .map(|bytes| {
                ursula_stream::BackupDigest::try_from(bytes.as_ref()).map_err(|_wrong_length| {
                    SnapshotStoreError::BackupDigestLength { len: bytes.len() }
                })
            })
            .transpose()?,
    })
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
            // Each stream frame carries its version's usage, so the two
            // lists pair one to one.
            let streams = snapshot.stream_snapshot.streams.len();
            let usages = snapshot.usage.streams.len();
            if streams != usages {
                return Some(Err(SnapshotStoreError::UsageMismatch(
                    UsageSnapshotError::StreamCount {
                        streams: u64::try_from(streams).unwrap_or(u64::MAX),
                        usages: u64::try_from(usages).unwrap_or(u64::MAX),
                    },
                )));
            }
            proto::snapshot_frame_v1::Frame::Header(Self::header_frame(snapshot))
        } else if let (Some(stream), Some(usage)) = (
            snapshot.stream_snapshot.streams.get(self.next_stream),
            snapshot.usage.streams.get(self.next_stream),
        ) {
            self.next_stream = self.next_stream.saturating_add(1);
            match stream_to_proto(stream.clone(), *usage) {
                Ok(stream) => proto::snapshot_frame_v1::Frame::Stream(Box::new(stream)),
                Err(err) => return Some(Err(err)),
            }
        } else if let Some(cold_gc) = snapshot
            .stream_snapshot
            .pending_cold_gc
            .get(self.next_cold_gc)
        {
            self.next_cold_gc = self.next_cold_gc.saturating_add(1);
            proto::snapshot_frame_v1::Frame::ColdGc(cold_gc_to_proto(cold_gc.clone()))
        } else if let Some(record) = snapshot.usage.ends.get(self.next_end_record) {
            self.next_end_record = self.next_end_record.saturating_add(1);
            proto::snapshot_frame_v1::Frame::EndRecord(Box::new(end_record_to_proto(record)))
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
    usage: VersionUsage,
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
        retired_epoch_watermark: entry.retired_epoch_watermark,
        history: entry.history,
        inherited: entry.inherited.map(|inherited| proto::InheritedHistoryV1 {
            history: inherited.history,
            offset: inherited.offset,
        }),
        usage: Some(usage_to_proto(usage)),
    })
}

/// Entry field 16 `retained_offset` stays `optional` on the wire; every
/// writer sets it, so an absent value is corruption. (Dropping `optional`
/// would make writers omit a zero.) So is an entry without its version's
/// usage, field 24 (F20).
fn stream_from_proto(
    entry: proto::StreamSnapshotEntryV1,
) -> Result<(StreamSnapshotEntry, VersionUsage), SnapshotStoreError> {
    let usage = usage_from_proto(required(entry.usage, "snapshot stream usage")?)?;
    let entry = StreamSnapshotEntry {
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
        retired_epoch_watermark: entry.retired_epoch_watermark,
        history: entry.history,
        inherited: entry.inherited.map(|inherited| InheritedHistory {
            history: inherited.history,
            offset: inherited.offset,
        }),
    };
    Ok((entry, usage))
}

fn counters_to_proto(counters: VersionCounters) -> proto::VersionCountersV1 {
    proto::VersionCountersV1 {
        committed_append_bytes: counters.committed.committed_append_bytes,
        committed_records: counters.committed.committed_records,
        committed_write_units: counters.committed.committed_write_units,
        appends: counters.appends,
    }
}

fn counters_from_proto(counters: proto::VersionCountersV1) -> VersionCounters {
    VersionCounters {
        committed: CommittedUsage {
            committed_append_bytes: counters.committed_append_bytes,
            committed_records: counters.committed_records,
            committed_write_units: counters.committed_write_units,
        },
        appends: counters.appends,
    }
}

/// A version restored from a backup that counted no write since has no
/// last write time.
fn last_write_to_proto(last_write: LastWrite) -> Option<u64> {
    match last_write {
        LastWrite::At(now_ms) => Some(now_ms),
        LastWrite::Imported => None,
    }
}

fn last_write_from_proto(last_write_at_ms: Option<u64>) -> LastWrite {
    last_write_at_ms.map_or(LastWrite::Imported, LastWrite::At)
}

/// A seq or history id of the ledger is never 0.
fn non_zero(value: u64, field: &'static str) -> Result<NonZeroU64, SnapshotStoreError> {
    NonZeroU64::new(value).ok_or(SnapshotStoreError::UsageFieldZero { field })
}

fn usage_to_proto(usage: VersionUsage) -> proto::StreamUsageV1 {
    proto::StreamUsageV1 {
        seq: usage.seq,
        counters: Some(counters_to_proto(usage.counters)),
        history_baseline: Some(counters_to_proto(usage.history_baseline)),
        last_write_at_ms: last_write_to_proto(usage.last_write),
        counted_since: usage.counted_since.get(),
    }
}

fn usage_from_proto(usage: proto::StreamUsageV1) -> Result<VersionUsage, SnapshotStoreError> {
    Ok(VersionUsage {
        seq: non_zero(usage.seq, "stream usage seq")?.get(),
        counters: counters_from_proto(required(usage.counters, "snapshot stream usage counters")?),
        history_baseline: counters_from_proto(required(
            usage.history_baseline,
            "snapshot stream usage history_baseline",
        )?),
        last_write: last_write_from_proto(usage.last_write_at_ms),
        counted_since: non_zero(usage.counted_since, "stream usage counted_since")?,
    })
}

fn retired_to_proto(row: &BucketRetired) -> proto::BucketRetiredV1 {
    proto::BucketRetiredV1 {
        bucket_id: row.bucket_id.clone(),
        retired: Some(proto::CommittedUsageV1 {
            committed_append_bytes: row.retired.committed_append_bytes,
            committed_records: row.retired.committed_records,
            committed_write_units: row.retired.committed_write_units,
        }),
    }
}

fn retired_from_proto(row: proto::BucketRetiredV1) -> Result<BucketRetired, SnapshotStoreError> {
    let retired = required(row.retired, "snapshot header retired usage")?;
    Ok(BucketRetired {
        bucket_id: row.bucket_id,
        retired: CommittedUsage {
            committed_append_bytes: retired.committed_append_bytes,
            committed_records: retired.committed_records,
            committed_write_units: retired.committed_write_units,
        },
    })
}

fn end_to_proto(end: StreamEnd) -> proto::StreamEndV1 {
    match end {
        StreamEnd::Deleted => proto::StreamEndV1::StreamEndDeleted,
        StreamEnd::Expired => proto::StreamEndV1::StreamEndExpired,
        StreamEnd::Purged => proto::StreamEndV1::StreamEndPurged,
    }
}

/// `STREAM_END_UNSPECIFIED` and an end this build does not know are
/// refused.
fn end_from_proto(seq: u64, value: i32) -> Result<StreamEnd, SnapshotStoreError> {
    match proto::StreamEndV1::try_from(value) {
        Ok(proto::StreamEndV1::StreamEndDeleted) => Ok(StreamEnd::Deleted),
        Ok(proto::StreamEndV1::StreamEndExpired) => Ok(StreamEnd::Expired),
        Ok(proto::StreamEndV1::StreamEndPurged) => Ok(StreamEnd::Purged),
        Ok(proto::StreamEndV1::StreamEndUnspecified) => {
            Err(SnapshotStoreError::StreamEndUnknown { seq, value })
        }
        Err(_unknown) => Err(SnapshotStoreError::StreamEndUnknown { seq, value }),
    }
}

fn end_record_to_proto(record: &EndRecord) -> proto::EndRecordV1 {
    proto::EndRecordV1 {
        seq: record.seq,
        stream_id: Some(record.stream_id.clone().into()),
        created_at_ms: record.created_at_ms,
        counted_since: record.counted_since.get(),
        ended_in: record.ended_in.get(),
        end: end_to_proto(record.end) as i32,
        ended_at_ms: record.ended_at_ms,
        counters: Some(counters_to_proto(record.counters)),
        history_baseline: Some(counters_to_proto(record.history_baseline)),
        last_write_at_ms: last_write_to_proto(record.last_write),
    }
}

fn end_record_from_proto(record: proto::EndRecordV1) -> Result<EndRecord, SnapshotStoreError> {
    let seq = non_zero(record.seq, "end record seq")?.get();
    Ok(EndRecord {
        seq,
        stream_id: required(record.stream_id, "snapshot end record stream id")?.into(),
        created_at_ms: record.created_at_ms,
        counted_since: non_zero(record.counted_since, "end record counted_since")?,
        ended_in: non_zero(record.ended_in, "end record ended_in")?,
        end: end_from_proto(seq, record.end)?,
        ended_at_ms: record.ended_at_ms,
        counters: counters_from_proto(required(record.counters, "snapshot end record counters")?),
        history_baseline: counters_from_proto(required(
            record.history_baseline,
            "snapshot end record history_baseline",
        )?),
        last_write: last_write_from_proto(record.last_write_at_ms),
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
    let receipt = producer.newest.receipt;
    proto::ProducerSnapshotV1 {
        producer_id: producer.producer_id,
        epoch: producer.epoch,
        last_seq: producer.last_seq,
        newest_start_offset: receipt.start_offset,
        newest_next_offset: receipt.next_offset,
        newest_closed: receipt.closed,
        last_seen_ms: Some(producer.last_seen_ms),
        claim_digest: producer
            .claim_digest
            .map(|digest| Bytes::copy_from_slice(&digest)),
        newest_digest: producer
            .newest
            .digest
            .map(|digest| Bytes::copy_from_slice(&digest)),
        closed_epoch: producer.closed_epoch.map(|closed| proto::ClosedEpochV1 {
            epoch: closed.epoch,
            seq: closed.seq,
        }),
        opened_below_floor: producer.opened == Opened::BelowFloor,
        history: producer.history,
    }
}

/// Field 9 `last_seen_ms` stays `optional` on the wire, but every writer sets
/// it (bounded-state F3), so an absent value is corruption. A digest is 32
/// bytes.
fn producer_from_proto(
    producer: proto::ProducerSnapshotV1,
) -> Result<ProducerSnapshot, SnapshotStoreError> {
    let last_seen_ms =
        producer
            .last_seen_ms
            .ok_or_else(|| SnapshotStoreError::ProducerFieldMissing {
                producer_id: producer.producer_id.clone(),
                field: "last_seen_ms",
            })?;
    let digest = |bytes: Option<Bytes>, field: &'static str| {
        bytes
            .map(|bytes| {
                WriteDigest::try_from(bytes.as_ref()).map_err(|_wrong_length| {
                    SnapshotStoreError::ProducerDigestLength {
                        producer_id: producer.producer_id.clone(),
                        field,
                        len: bytes.len(),
                    }
                })
            })
            .transpose()
    };
    let claim_digest = digest(producer.claim_digest.clone(), "claim_digest")?;
    let newest_digest = digest(producer.newest_digest.clone(), "newest_digest")?;
    Ok(ProducerSnapshot {
        epoch: producer.epoch,
        last_seq: producer.last_seq,
        claim_digest,
        newest: ProducerReceipt {
            receipt: WriteReceipt {
                start_offset: producer.newest_start_offset,
                next_offset: producer.newest_next_offset,
                closed: producer.newest_closed,
            },
            digest: newest_digest,
        },
        closed_epoch: producer.closed_epoch.map(|closed| ClosedEpoch {
            epoch: closed.epoch,
            seq: closed.seq,
        }),
        opened: if producer.opened_below_floor {
            Opened::BelowFloor
        } else {
            Opened::AboveFloor
        },
        history: producer.history,
        last_seen_ms,
        producer_id: producer.producer_id,
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

fn required<T>(value: Option<T>, field: &'static str) -> Result<T, SnapshotStoreError> {
    value.ok_or(SnapshotStoreError::MissingField { field })
}

#[cfg(test)]
mod tests {
    use ursula_shard::BucketStreamId;

    use super::*;

    /// The history event in a snapshot's header frame, as this format epoch
    /// writes it (#613): a recovery's event, with the history it replaced,
    /// the index it kept and its leader. A frame without any of them is
    /// refused.
    #[test]
    fn a_history_event_frame_is_stable() {
        // Fields 1 id, 2 event_index, 4 kept, 5 previous, 6 leader_term,
        // 7 leader_node.
        const GOLDEN: &[u8] = &[
            8, 161, 134, 149, 187, 152, 245, 242, 246, 15, 16, 14, 32, 12, 40, 239, 155, 175, 205,
            248, 172, 209, 145, 1, 48, 7, 56, 2,
        ];
        let event = HistoryEvent {
            id: std::num::NonZeroU64::new(0x0fed_cba9_8765_4321).expect("non-zero"),
            event_index: 14,
            previous: 0x0123_4567_89ab_cdef,
            kept: 12,
            leader: ursula_stream::HistoryLeader {
                term: 7,
                node_id: 2,
            },
            imported: None,
        };
        let encoded = history_to_proto(event).encode_to_vec();
        assert_eq!(encoded, GOLDEN);
        let decoded = proto::HistoryEventV1::decode(GOLDEN).expect("decodes");
        assert_eq!(history_from_proto(decoded.clone()).expect("valid"), event);
        for missing in [
            proto::HistoryEventV1 {
                kept: None,
                ..decoded.clone()
            },
            proto::HistoryEventV1 {
                previous: None,
                ..decoded.clone()
            },
            proto::HistoryEventV1 {
                leader_term: None,
                ..decoded.clone()
            },
            proto::HistoryEventV1 {
                leader_node: None,
                ..decoded
            },
        ] {
            assert!(matches!(
                history_from_proto(missing),
                Err(SnapshotStoreError::MissingField { .. })
            ));
        }
    }

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
                format_version: ursula_stream::BACKUP_FORMAT_VERSION,
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
                history: None,
            },
            usage: UsageSnapshot {
                genesis: 0x0abc,
                usage_seq: 41,
                retired_through: 37,
                retired: vec![BucketRetired {
                    bucket_id: "bucket".to_owned(),
                    retired: CommittedUsage {
                        committed_append_bytes: 70,
                        committed_records: 5,
                        committed_write_units: 2,
                    },
                }],
                streams: Vec::new(),
                ends: vec![sample_end_record(39), EndRecord {
                    end: StreamEnd::Purged,
                    last_write: LastWrite::Imported,
                    ..sample_end_record(40)
                }],
            },
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

    /// An end record with every field set, distinct values in fields of
    /// one type.
    fn sample_end_record(seq: u64) -> EndRecord {
        EndRecord {
            seq,
            stream_id: BucketStreamId::new("bucket", "ended"),
            created_at_ms: 1_101,
            counted_since: NonZeroU64::new(0x0abc).expect("non-zero"),
            ended_in: NonZeroU64::new(0x0abd).expect("non-zero"),
            end: StreamEnd::Deleted,
            ended_at_ms: 1_202,
            counters: VersionCounters {
                committed: CommittedUsage {
                    committed_append_bytes: 15,
                    committed_records: 1,
                    committed_write_units: 1,
                },
                appends: 3,
            },
            history_baseline: VersionCounters {
                committed: CommittedUsage {
                    committed_append_bytes: 12,
                    committed_records: 4,
                    committed_write_units: 6,
                },
                appends: 2,
            },
            last_write: LastWrite::At(1_150),
        }
    }

    /// The producer record (#581 section 2.3) survives the group snapshot
    /// codec with every field set: the idle clock, both digests, the closed
    /// epoch and the stream's epoch floor, so an installed replica holds
    /// exactly the live replica's producer state.
    #[test]
    fn producer_record_and_epoch_floor_round_trip() {
        let mut machine =
            ursula_stream::StreamStateMachine::with_genesis(std::num::NonZeroU64::MIN);
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
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            now_ms: 1,
        });
        const IDLE_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
        let tidy_at = 20 + IDLE_MS;
        let mut write = |producer_id: &str, epoch: u64, seq: u64, digest: Option<u8>, now_ms| {
            let response = machine.apply(ursula_stream::StreamCommand::Append {
                stream_id: stream_id.clone(),
                content_type: Some("application/octet-stream".to_owned()),
                payload: bytes::Bytes::from_static(b"xy"),
                close_after: false,
                stream_seq: None,
                producer: Some(ursula_stream::ProducerRequest {
                    producer_id: producer_id.to_owned(),
                    producer_epoch: epoch,
                    producer_seq: seq,
                    digest: digest.map(|digest| bytes::Bytes::copy_from_slice(&[digest; 32])),
                    ..Default::default()
                }),
                now_ms,
            });
            assert!(
                matches!(response, ursula_stream::StreamResponse::Appended { .. }),
                "{response:?}"
            );
        };
        // `b` goes idle and retires, which raises the floor to its epoch 3.
        write("b", 3, 0, None, 20);
        for seq in 0..5 {
            write("a", 1, seq, Some(1), tidy_at - 1);
        }
        // `a`'s claim of epoch 2 closes epoch 1 at seq 4, with digests on
        // the claim and the newest write.
        write("a", 2, 0, Some(7), tidy_at - 1);
        write("a", 2, 1, Some(8), tidy_at - 1);
        machine.apply(ursula_stream::StreamCommand::TidyStream {
            stream_id: stream_id.clone(),
            now_ms: tidy_at,
        });
        // Without the token `b` is new, and its claim of the forgotten epoch
        // 3 opens a record below the floor.
        let reclaim = machine.apply(ursula_stream::StreamCommand::Append {
            stream_id: stream_id.clone(),
            content_type: Some("application/octet-stream".to_owned()),
            payload: bytes::Bytes::from_static(b"xy"),
            close_after: false,
            stream_seq: None,
            producer: Some(ursula_stream::ProducerRequest {
                producer_id: "b".to_owned(),
                producer_epoch: 3,
                producer_seq: 0,
                ..Default::default()
            }),
            now_ms: tidy_at,
        });
        assert!(
            matches!(reclaim, ursula_stream::StreamResponse::Appended { .. }),
            "{reclaim:?}"
        );
        let stream_snapshot = machine.snapshot();
        let usage = machine.usage_snapshot();
        let version = usage.streams[0];
        let entry = &stream_snapshot.streams[0];
        assert_eq!(entry.retired_epoch_watermark, Some(3));
        let producers = &entry.producer_states;
        assert_eq!(producers.len(), 2);
        assert_eq!(producers[1].opened, ursula_stream::Opened::BelowFloor);
        assert_eq!(producers[0].opened, ursula_stream::Opened::AboveFloor);
        assert_eq!(producers[0].last_seen_ms, tidy_at - 1);
        assert_eq!(producers[0].claim_digest, Some([7; 32]));
        assert_eq!(producers[0].newest.digest, Some([8; 32]));
        assert_eq!(
            producers[0].closed_epoch,
            Some(ursula_stream::ClosedEpoch { epoch: 1, seq: 4 })
        );
        // A digest is 32 bytes on the wire.
        let mut proto_entry =
            stream_to_proto(stream_snapshot.streams[0].clone(), version).expect("encode entry");
        proto_entry.producer_states[0].claim_digest = Some(Bytes::from_static(b"short"));
        let error = stream_from_proto(proto_entry).expect_err("a short digest");
        assert!(
            matches!(error, SnapshotStoreError::ProducerDigestLength {
                field: "claim_digest",
                len: 5,
                ..
            }),
            "{error}"
        );
        // Field 9 stays optional on the wire; an absent value is refused.
        let mut entry =
            stream_to_proto(stream_snapshot.streams[0].clone(), version).expect("encode entry");
        entry.producer_states[0].last_seen_ms = None;
        let error = stream_from_proto(entry).expect_err("absent last_seen_ms");
        assert!(
            matches!(error, SnapshotStoreError::ProducerFieldMissing {
                field: "last_seen_ms",
                ..
            }),
            "{error}"
        );
        // So does entry field 16.
        let mut entry =
            stream_to_proto(stream_snapshot.streams[0].clone(), version).expect("encode entry");
        entry.retained_offset = None;
        let error = stream_from_proto(entry).expect_err("absent retained_offset");
        assert!(
            matches!(error, SnapshotStoreError::MissingField {
                field: "snapshot stream retained_offset"
            }),
            "{error}"
        );
        let snapshot = GroupSnapshot {
            placement: ShardPlacement {
                core_id: CoreId(0),
                shard_id: ShardId(0),
                raft_group_id: RaftGroupId(0),
            },
            group_commit_index: 7,
            stream_snapshot,
            usage,
        };
        let bytes = group_snapshot_frames(Arc::new(snapshot.clone()))
            .collect::<Result<Vec<_>, _>>()
            .expect("encode frames")
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let decoded = decode_group_snapshot(&bytes).expect("decode frames");
        assert_eq!(decoded, snapshot);
        let restored = ursula_stream::StreamStateMachine::restore_group(
            decoded.stream_snapshot,
            decoded.usage,
        )
        .expect("restore decoded snapshot");
        assert_eq!(restored.snapshot(), machine.snapshot());
        assert_eq!(restored.usage_snapshot(), machine.usage_snapshot());
        assert_eq!(restored.state_gauges(), machine.state_gauges());
    }

    /// History identity (#581 section 4.2) survives the group snapshot
    /// codec: the group's history event with its import digest, each
    /// stream's history and inheritance, and each producer record's
    /// history, so an installed replica derives the same tokens, fences and
    /// HEAD reports.
    #[test]
    fn history_state_round_trips() {
        use std::num::NonZeroU64;

        let mut machine = ursula_stream::StreamStateMachine::new();
        let mut index = 0;
        let mut apply = |machine: &mut ursula_stream::StreamStateMachine, command| {
            index += 1;
            machine.apply_entry(index, command)
        };
        apply(&mut machine, ursula_stream::StreamCommand::NewHistory {
            history: NonZeroU64::new(11).unwrap(),
            now_ms: 0,
            previous: 0,
            kept: 0,
            leader: ursula_stream::HistoryLeader {
                term: 0,
                node_id: 0,
            },
        });
        apply(&mut machine, ursula_stream::StreamCommand::CreateBucket {
            bucket_id: "bucket".to_owned(),
        });
        let stream_id = BucketStreamId::new("bucket", "history");
        apply(&mut machine, ursula_stream::StreamCommand::CreateStream {
            stream_id: stream_id.clone(),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: bytes::Bytes::from_static(b"ab"),
            close_after: false,
            stream_seq: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            now_ms: 1,
        });
        let append = |seq| ursula_stream::StreamCommand::Append {
            stream_id: stream_id.clone(),
            content_type: Some("application/octet-stream".to_owned()),
            payload: bytes::Bytes::from_static(b"cd"),
            close_after: false,
            stream_seq: None,
            producer: Some(ursula_stream::ProducerRequest {
                producer_id: "p".to_owned(),
                producer_epoch: 0,
                producer_seq: seq,
                ..Default::default()
            }),
            now_ms: 1,
        };
        apply(&mut machine, append(0));
        // A restore into a fresh group: a history event with an import key.
        let mut restored = ursula_stream::StreamStateMachine::new();
        restored.apply_entry(1, ursula_stream::StreamCommand::NewHistory {
            history: NonZeroU64::new(12).unwrap(),
            now_ms: 0,
            previous: 0,
            kept: 0,
            leader: ursula_stream::HistoryLeader {
                term: 0,
                node_id: 0,
            },
        });
        restored.apply_entry(2, ursula_stream::StreamCommand::ImportSnapshot {
            snapshot: Box::new(machine.snapshot()),
            history: NonZeroU64::new(13).unwrap(),
            backup_digest: [5; 32],
            now_ms: 1,
        });
        // An accepted loss, then a write that records the inheritance. No
        // write happened between the import and the loss, so the stream
        // still names the source's history.
        restored.apply_entry(3, ursula_stream::StreamCommand::NewHistory {
            history: NonZeroU64::new(14).unwrap(),
            now_ms: 0,
            previous: 13,
            kept: 2,
            leader: ursula_stream::HistoryLeader {
                term: 0,
                node_id: 0,
            },
        });
        restored.apply_entry(4, ursula_stream::StreamCommand::Append {
            stream_id: stream_id.clone(),
            content_type: Some("application/octet-stream".to_owned()),
            payload: bytes::Bytes::from_static(b"ef"),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 1,
        });
        let stream_snapshot = restored.snapshot();
        let history = stream_snapshot.history.expect("a history");
        assert_eq!(history.id.get(), 14);
        assert_eq!(history.event_index, 3);
        assert_eq!(history.imported, Some([5; 32]));
        let entry = &stream_snapshot.streams[0];
        assert_eq!(entry.history, 14);
        assert_eq!(
            entry.inherited,
            Some(ursula_stream::InheritedHistory {
                history: 11,
                offset: 4,
            })
        );
        assert_eq!(entry.producer_states[0].history, 11);

        let snapshot = GroupSnapshot {
            placement: ShardPlacement {
                core_id: CoreId(0),
                shard_id: ShardId(0),
                raft_group_id: RaftGroupId(0),
            },
            group_commit_index: 4,
            stream_snapshot,
            usage: restored.usage_snapshot(),
        };
        let bytes = group_snapshot_frames(Arc::new(snapshot.clone()))
            .collect::<Result<Vec<_>, _>>()
            .expect("encode frames")
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let decoded = decode_group_snapshot(&bytes).expect("decode frames");
        assert_eq!(decoded, snapshot);
        let installed = ursula_stream::StreamStateMachine::restore_group(
            decoded.stream_snapshot,
            decoded.usage,
        )
        .expect("restore decoded snapshot");
        assert_eq!(installed.usage_snapshot(), restored.usage_snapshot());
        assert_eq!(installed.usage_genesis(), 12);
        assert_eq!(
            installed.stream_incarnation(&stream_id),
            restored.stream_incarnation(&stream_id)
        );
        assert_eq!(
            installed.stream_inherited(&stream_id),
            restored.stream_inherited(&stream_id)
        );

        // A history id is never 0, and an import digest is 32 bytes.
        let mut header = GroupSnapshotFrameIter::header_frame(&snapshot);
        header.history.as_mut().expect("a history").id = 0;
        assert!(matches!(
            history_from_proto(header.history.clone().expect("a history")),
            Err(SnapshotStoreError::HistoryIdZero)
        ));
        let mut header = GroupSnapshotFrameIter::header_frame(&snapshot);
        header.history.as_mut().expect("a history").imported = Some(Bytes::from_static(b"short"));
        assert!(matches!(
            history_from_proto(header.history.expect("a history")),
            Err(SnapshotStoreError::BackupDigestLength { len: 5 })
        ));
    }

    /// The ledger rides in the group snapshot (F20): each stream frame's
    /// usage (field 24), end-record frames (frame 8) after the cold-GC
    /// frames, and header fields 13 to 16. A frame without a required part,
    /// a zero seq or history id, an end record without an end and a header
    /// without fields 13, 14 or 16 are refused, and so is a snapshot to
    /// encode whose usages do not pair with its streams.
    #[test]
    fn usage_frames_round_trip_and_refuse_what_is_missing() {
        let usage = VersionUsage {
            seq: 9,
            counters: sample_end_record(1).counters,
            history_baseline: sample_end_record(1).history_baseline,
            last_write: LastWrite::At(77),
            counted_since: NonZeroU64::new(5).expect("non-zero"),
        };
        let encoded = usage_to_proto(usage);
        assert_eq!(usage_from_proto(encoded).expect("decodes"), usage);
        let imported = VersionUsage {
            last_write: LastWrite::Imported,
            ..usage
        };
        assert_eq!(usage_to_proto(imported).last_write_at_ms, None);
        assert_eq!(
            usage_from_proto(usage_to_proto(imported)).expect("decodes"),
            imported
        );
        for (broken, expected) in [
            (
                proto::StreamUsageV1 { seq: 0, ..encoded },
                "stream usage seq",
            ),
            (
                proto::StreamUsageV1 {
                    counted_since: 0,
                    ..encoded
                },
                "stream usage counted_since",
            ),
        ] {
            assert!(
                matches!(
                    usage_from_proto(broken),
                    Err(SnapshotStoreError::UsageFieldZero { field }) if field == expected
                ),
                "{expected}"
            );
        }
        for broken in [
            proto::StreamUsageV1 {
                counters: None,
                ..encoded
            },
            proto::StreamUsageV1 {
                history_baseline: None,
                ..encoded
            },
        ] {
            assert!(matches!(
                usage_from_proto(broken),
                Err(SnapshotStoreError::MissingField { .. })
            ));
        }

        let record = sample_end_record(12);
        let encoded = end_record_to_proto(&record);
        assert_eq!(
            end_record_from_proto(encoded.clone()).expect("decodes"),
            record
        );
        for broken in [
            proto::EndRecordV1 {
                seq: 0,
                ..encoded.clone()
            },
            proto::EndRecordV1 {
                counted_since: 0,
                ..encoded.clone()
            },
            proto::EndRecordV1 {
                ended_in: 0,
                ..encoded.clone()
            },
        ] {
            assert!(matches!(
                end_record_from_proto(broken),
                Err(SnapshotStoreError::UsageFieldZero { .. })
            ));
        }
        for end in [proto::StreamEndV1::StreamEndUnspecified as i32, 9] {
            assert!(matches!(
                end_record_from_proto(proto::EndRecordV1 {
                    end,
                    ..encoded.clone()
                }),
                Err(SnapshotStoreError::StreamEndUnknown { seq: 12, value }) if value == end
            ));
        }
        for broken in [
            proto::EndRecordV1 {
                stream_id: None,
                ..encoded.clone()
            },
            proto::EndRecordV1 {
                counters: None,
                ..encoded.clone()
            },
            proto::EndRecordV1 {
                history_baseline: None,
                ..encoded
            },
        ] {
            assert!(matches!(
                end_record_from_proto(broken),
                Err(SnapshotStoreError::MissingField { .. })
            ));
        }
        assert!(matches!(
            retired_from_proto(proto::BucketRetiredV1 {
                bucket_id: "bucket".to_owned(),
                retired: None,
            }),
            Err(SnapshotStoreError::MissingField { .. })
        ));

        // A stream frame without its usage, and a header without a required
        // ledger field, are refused.
        let mut machine =
            ursula_stream::StreamStateMachine::with_genesis(std::num::NonZeroU64::MIN);
        machine.apply(ursula_stream::StreamCommand::CreateBucket {
            bucket_id: "bucket".to_owned(),
        });
        machine.apply(ursula_stream::StreamCommand::CreateStream {
            stream_id: BucketStreamId::new("bucket", "s"),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: bytes::Bytes::from_static(b"ab"),
            close_after: false,
            stream_seq: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            now_ms: 1,
        });
        let entry = machine.snapshot().streams.remove(0);
        let version = machine.usage_snapshot().streams[0];
        let mut stream = stream_to_proto(entry, version).expect("encode entry");
        stream.usage = None;
        assert!(matches!(
            stream_from_proto(stream),
            Err(SnapshotStoreError::MissingField {
                field: "snapshot stream usage"
            })
        ));
        for header in [
            proto::SnapshotHeaderV1 {
                usage_seq: None,
                ..header_v1()
            },
            proto::SnapshotHeaderV1 {
                retired_through: None,
                ..header_v1()
            },
            proto::SnapshotHeaderV1 {
                genesis: None,
                ..header_v1()
            },
        ] {
            let bytes = [
                epoch_frame(ursula_stream::FORMAT_EPOCH),
                encode_frame(proto::SnapshotFrameV1 {
                    frame: Some(proto::snapshot_frame_v1::Frame::Header(header)),
                })
                .expect("encode header"),
                footer_frame(),
            ]
            .concat();
            assert!(matches!(
                decode_group_snapshot(&bytes),
                Err(SnapshotStoreError::MissingField { .. })
            ));
        }

        // A group snapshot whose usages do not pair with its streams does
        // not encode.
        let snapshot = GroupSnapshot {
            placement: ShardPlacement {
                core_id: CoreId(0),
                shard_id: ShardId(0),
                raft_group_id: RaftGroupId(0),
            },
            group_commit_index: 2,
            stream_snapshot: machine.snapshot(),
            usage: UsageSnapshot {
                streams: Vec::new(),
                ..machine.usage_snapshot()
            },
        };
        let error = group_snapshot_frames(Arc::new(snapshot))
            .collect::<Result<Vec<_>, _>>()
            .expect_err("a usage per stream");
        assert!(
            matches!(
                error,
                SnapshotStoreError::UsageMismatch(UsageSnapshotError::StreamCount {
                    streams: 1,
                    usages: 0,
                })
            ),
            "{error}"
        );
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
            history: None,
            usage_seq: Some(0),
            retired_through: Some(0),
            retired: Vec::new(),
            genesis: Some(0),
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

    /// E5: the epoch frame comes first and is checked before any other frame.
    #[test]
    fn decode_requires_a_leading_epoch_frame_of_this_epoch() {
        let epoch = ursula_stream::FORMAT_EPOCH;
        let valid = [epoch_frame(epoch), header_frame(), footer_frame()].concat();
        decode_group_snapshot(&valid).expect("a snapshot of this epoch decodes");

        for (bytes, expected) in [
            ([header_frame(), footer_frame()].concat(), None),
            (
                [epoch_frame(epoch - 1), header_frame(), footer_frame()].concat(),
                Some(epoch - 1),
            ),
            (
                [epoch_frame(epoch + 1), header_frame(), footer_frame()].concat(),
                Some(epoch + 1),
            ),
            (
                [header_frame(), epoch_frame(epoch), footer_frame()].concat(),
                None,
            ),
        ] {
            let error = decode_group_snapshot(&bytes).expect_err("refused");
            assert!(
                matches!(error, SnapshotStoreError::UnsupportedVersion { found } if found == expected),
                "{error}"
            );
        }
        let repeated = [
            epoch_frame(epoch),
            header_frame(),
            epoch_frame(epoch),
            footer_frame(),
        ]
        .concat();
        let error = decode_group_snapshot(&repeated).expect_err("refused");
        assert!(
            matches!(&error, SnapshotStoreError::Deserialize(message) if message.contains("not the first frame")),
            "{error}"
        );
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
                    history: None,
                    usage_seq: Some(0),
                    retired_through: Some(0),
                    retired: Vec::new(),
                    genesis: Some(0),
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
            history: None,
            ..header_v1()
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
        let mut machine =
            ursula_stream::StreamStateMachine::with_genesis(std::num::NonZeroU64::MIN);
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
        let version = machine.usage_snapshot().streams[0];
        let encoded = stream_to_proto(entry.clone(), version).expect("encode entry");
        let visible = encoded.visible_snapshot.as_ref().expect("visible snapshot");
        assert!(visible.payload.is_empty());
        assert_eq!(visible.object.as_ref(), Some(&object));
        assert_eq!(
            stream_from_proto(encoded).expect("decode entry"),
            (entry, version)
        );
    }
}
