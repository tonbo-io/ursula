//! One sample of every replicated command, every write response, the
//! forwarded read responses and a fully populated backup snapshot, for the
//! tests that pin their MessagePack form (the byte fixtures of
//! `ursula-raft`). Each list is built behind an exhaustive match with no
//! wildcard ([`every`]), so a new variant does not compile until it has a
//! sample.
//!
//! Every field is set explicitly, never through `..Default::default()`, and
//! fields of one type in one value hold distinct values, so a fixture shows
//! two of them swapping places. The `vary_*` functions clear the `Option`s
//! of a sample that a mask selects, so a property test reaches every
//! combination of present and absent fields.

use std::num::NonZeroU32;
use std::num::NonZeroU64;

use bytes::Bytes;
use ursula_proto::ContentFaultV1;
use ursula_shard::BucketStreamId;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;
use ursula_stream::BACKUP_FORMAT_VERSION;
use ursula_stream::BucketUsage;
use ursula_stream::BucketUsageSnapshot;
use ursula_stream::ClosedEpoch;
use ursula_stream::ColdChunkRef;
use ursula_stream::ColdGcEntry;
use ursula_stream::ColdGcTarget;
use ursula_stream::ExternalPayloadRef;
use ursula_stream::HistoryEvent;
use ursula_stream::HistoryLeader;
use ursula_stream::HotPayloadSegment;
use ursula_stream::InheritedHistory;
use ursula_stream::InheritedReport;
use ursula_stream::ObjectPayloadRef;
use ursula_stream::Opened;
use ursula_stream::ProducerAck;
use ursula_stream::ProducerDelivery;
use ursula_stream::ProducerReceipt;
use ursula_stream::ProducerRequest;
use ursula_stream::ProducerSnapshot;
use ursula_stream::StreamCommand;
use ursula_stream::StreamMetadata;
use ursula_stream::StreamSnapshot;
use ursula_stream::StreamSnapshotEntry;
use ursula_stream::StreamStatus;
use ursula_stream::StreamVisibleSnapshot;
use ursula_stream::WriteReceipt;

use crate::AckColdGcResponse;
use crate::AdvanceRetentionResponse;
use crate::AppendResponse;
use crate::CloseStreamResponse;
use crate::CompactColdResponse;
use crate::CreateStreamResponse;
use crate::DeferColdGcResponse;
use crate::DeleteStreamResponse;
use crate::ExpireStreamsResponse;
use crate::FlushColdResponse;
use crate::GroupWriteResponse;
use crate::HeadStreamResponse;
use crate::ImportGroupStateResponse;
use crate::NewHistoryResponse;
use crate::OffloadStreamColdRefsResponse;
use crate::PublishSnapshotResponse;
use crate::PurgeBucketResponse;
use crate::ReadStreamResponse;
use crate::TidyStreamResponse;
use crate::TouchStreamAccessResponse;
use crate::WriteHotBacklog;
use crate::engine::every;

const NOW_MS: u64 = 1_700_000_000_000;
const HISTORY: u64 = 0x0fed_cba9_8765_4321;
const PREVIOUS_HISTORY: u64 = 0x0123_4567_89ab_cdef;
const INCARNATION: u64 = 0x0012_3456_789a_bcde;

fn history() -> NonZeroU64 {
    NonZeroU64::new(HISTORY).expect("non-zero")
}

fn stream() -> BucketStreamId {
    BucketStreamId::new("tenant", "orders")
}

fn placement() -> ShardPlacement {
    ShardPlacement {
        core_id: CoreId(3),
        shard_id: ShardId(5),
        raft_group_id: RaftGroupId(7),
    }
}

fn producer() -> ProducerRequest {
    ProducerRequest {
        producer_id: "writer".to_owned(),
        producer_epoch: 2,
        producer_seq: 3,
        digest: Some(Bytes::from_static(&[0xd1; 32])),
        content_fault: i32::from(ContentFaultV1::ContentFaultMissingContentType),
    }
}

fn external(s3_path: &str) -> ExternalPayloadRef {
    ExternalPayloadRef {
        s3_path: s3_path.to_owned(),
        payload_len: 40,
        object_size: 48,
    }
}

fn chunk(start_offset: u64, end_offset: u64, s3_path: &str) -> ColdChunkRef {
    ColdChunkRef {
        start_offset,
        end_offset,
        s3_path: s3_path.to_owned(),
        object_size: 4096,
        object_offset: 32,
        shared_object: true,
        payload_digest: "chunk-digest".to_owned(),
    }
}

fn object_ref() -> ObjectPayloadRef {
    ObjectPayloadRef {
        start_offset: 10,
        end_offset: 18,
        s3_path: "cold/tenant/orders/external/10-18".to_owned(),
        object_size: 64,
        object_offset: 8,
    }
}

fn leader() -> HistoryLeader {
    HistoryLeader {
        term: 7,
        node_id: 2,
    }
}

fn hot_backlog() -> Option<WriteHotBacklog> {
    Some(WriteHotBacklog {
        stream_hot_bytes: 9,
        group_hot_bytes: 90,
    })
}

fn receipt() -> WriteReceipt {
    WriteReceipt {
        start_offset: 18,
        next_offset: 27,
        closed: true,
    }
}

fn producer_ack(delivery: ProducerDelivery) -> ProducerAck {
    ProducerAck {
        epoch: 2,
        seq: 3,
        closed_epoch: Some(ClosedEpoch { epoch: 1, seq: 6 }),
        delivery,
    }
}

/// Every [`StreamCommand`] variant once, every `Option` present.
pub fn stream_commands() -> Vec<StreamCommand> {
    every!(StreamCommand {
        StreamCommand::CreateBucket { .. } => StreamCommand::CreateBucket {
            bucket_id: "tenant".to_owned(),
        },
        StreamCommand::CreateStream { .. } => StreamCommand::CreateStream {
            stream_id: stream(),
            content_type: "application/json".to_owned(),
            initial_payload: Bytes::from_static(br#"{"id":1}"#),
            close_after: true,
            stream_seq: Some("seq-1".to_owned()),
            stream_ttl_seconds: Some(3600),
            stream_expires_at_ms: Some(1_700_000_900_000),
            now_ms: NOW_MS,
        },
        StreamCommand::CreateExternal { .. } => StreamCommand::CreateExternal {
            stream_id: stream(),
            content_type: "application/json".to_owned(),
            initial_payload: external("cold/tenant/orders/external/create"),
            message_count: 4,
            close_after: true,
            stream_seq: Some("seq-2".to_owned()),
            stream_ttl_seconds: Some(3601),
            stream_expires_at_ms: Some(1_700_000_900_001),
            now_ms: 1_700_000_000_001,
        },
        StreamCommand::Append { .. } => StreamCommand::Append {
            stream_id: stream(),
            content_type: Some("text/plain".to_owned()),
            payload: Bytes::from_static(b"hello"),
            close_after: true,
            stream_seq: Some("seq-3".to_owned()),
            producer: Some(producer()),
            now_ms: 1_700_000_000_002,
        },
        StreamCommand::AppendExternal { .. } => StreamCommand::AppendExternal {
            stream_id: stream(),
            content_type: Some("text/plain".to_owned()),
            payload: external("cold/tenant/orders/external/append"),
            message_count: 5,
            close_after: true,
            stream_seq: Some("seq-4".to_owned()),
            producer: Some(producer()),
            now_ms: 1_700_000_000_003,
        },
        StreamCommand::PublishSnapshot { .. } => StreamCommand::PublishSnapshot {
            stream_id: stream(),
            snapshot_offset: 18,
            content_type: "application/json".to_owned(),
            payload: Bytes::from_static(br#"{"count":2}"#),
            now_ms: 1_700_000_000_004,
            expected_incarnation: Some(INCARNATION),
        },
        StreamCommand::PublishSnapshotExternal { .. } => StreamCommand::PublishSnapshotExternal {
            stream_id: stream(),
            snapshot_offset: 19,
            content_type: "application/json".to_owned(),
            object: external("cold/tenant/orders/snapshot/19"),
            digest: "snapshot-digest".to_owned(),
            now_ms: 1_700_000_000_005,
            expected_incarnation: Some(INCARNATION),
        },
        StreamCommand::AdvanceRetention { .. } => StreamCommand::AdvanceRetention {
            stream_id: stream(),
            retained_offset: 17,
            now_ms: 1_700_000_000_006,
            expected_incarnation: Some(INCARNATION),
        },
        StreamCommand::TouchStreamAccess { .. } => StreamCommand::TouchStreamAccess {
            stream_id: stream(),
            now_ms: 1_700_000_000_007,
            renew_ttl: true,
        },
        StreamCommand::FlushCold { .. } => StreamCommand::FlushCold {
            stream_id: stream(),
            chunk: chunk(100, 164, "cold/tenant/orders/5/100-164"),
            cold_generation: 9,
        },
        StreamCommand::CompactCold { .. } => StreamCommand::CompactCold {
            stream_id: stream(),
            old_chunks: vec![
                chunk(100, 164, "cold/tenant/orders/5/100-164"),
                chunk(164, 200, "cold/tenant/orders/5/164-200"),
            ],
            replacement: chunk(100, 200, "cold/tenant/orders/5/100-200"),
            gc_not_before_ms: 1_700_000_100_000,
        },
        StreamCommand::Close { .. } => StreamCommand::Close {
            stream_id: stream(),
            stream_seq: Some("seq-5".to_owned()),
            producer: Some(producer()),
            now_ms: 1_700_000_000_008,
        },
        StreamCommand::DeleteStream { .. } => StreamCommand::DeleteStream {
            stream_id: stream(),
            now_ms: 1_700_000_000_009,
        },
        StreamCommand::PurgeBucket { .. } => StreamCommand::PurgeBucket {
            bucket_id: "tenant-c".to_owned(),
        },
        StreamCommand::AckColdGc { .. } => StreamCommand::AckColdGc { up_to_seq: 21 },
        StreamCommand::DeferColdGc { .. } => StreamCommand::DeferColdGc {
            seq: 22,
            not_before_ms: 1_700_000_100_001,
        },
        StreamCommand::ImportSnapshot { .. } => StreamCommand::ImportSnapshot {
            snapshot: Box::new(small_stream_snapshot()),
            history: history(),
            backup_digest: [0xab; 32],
            now_ms: 1_700_000_000_010,
        },
        StreamCommand::NewHistory { .. } => StreamCommand::NewHistory {
            history: history(),
            now_ms: 1_700_000_000_011,
            previous: PREVIOUS_HISTORY,
            kept: 12,
            leader: leader(),
        },
        StreamCommand::TidyStream { .. } => StreamCommand::TidyStream {
            stream_id: stream(),
            now_ms: 1_700_000_000_012,
        },
        StreamCommand::ExpireStreams { .. } => StreamCommand::ExpireStreams {
            now_ms: 1_700_000_000_013,
            limit: NonZeroU32::new(64).expect("non-zero"),
        },
        StreamCommand::OffloadColdRefs { .. } => StreamCommand::OffloadColdRefs {
            stream_id: stream(),
            refs: vec![object_ref()],
        },
        StreamCommand::IfIncarnation { .. } => StreamCommand::IfIncarnation {
            incarnation: INCARNATION,
            command: Box::new(StreamCommand::DeleteStream {
                stream_id: stream(),
                now_ms: 1_700_000_000_014,
            }),
        },
    })
}

/// A backup snapshot of one bucket and no stream, as an import command in
/// a sample carries it.
fn small_stream_snapshot() -> StreamSnapshot {
    StreamSnapshot {
        format_version: BACKUP_FORMAT_VERSION,
        buckets: vec!["tenant".to_owned()],
        erased_buckets: Vec::new(),
        streams: Vec::new(),
        pending_cold_gc: Vec::new(),
        next_cold_gc_seq: 1,
        bucket_usage: Vec::new(),
        last_created_at_ms: 2,
        history: None,
    }
}

/// Every [`GroupWriteResponse`] variant once, every `Option` present, and
/// an append answer of every [`ProducerDelivery`].
pub fn group_write_responses() -> Vec<GroupWriteResponse> {
    let append = |delivery| AppendResponse {
        placement: placement(),
        start_offset: 18,
        next_offset: 27,
        stream_append_count: 3,
        group_commit_index: 42,
        closed: true,
        producer: Some(producer_ack(delivery)),
        stream_hot_bytes: 9,
        group_hot_bytes: 90,
        incarnation: INCARNATION,
    };
    let mut responses = every!(GroupWriteResponse {
        GroupWriteResponse::CreateStream(_) => GroupWriteResponse::CreateStream(CreateStreamResponse {
            placement: placement(),
            next_offset: 27,
            closed: true,
            already_exists: false,
            group_commit_index: 41,
            incarnation: INCARNATION,
            hot_backlog: hot_backlog(),
        }),
        GroupWriteResponse::Append(_) => {
            GroupWriteResponse::Append(append(ProducerDelivery::Applied(receipt())))
        },
        GroupWriteResponse::PublishSnapshot(_) => {
            GroupWriteResponse::PublishSnapshot(PublishSnapshotResponse {
                placement: placement(),
                snapshot_offset: 18,
                snapshot_digest: "snapshot-digest".to_owned(),
                group_commit_index: 43,
                incarnation: INCARNATION,
                hot_backlog: hot_backlog(),
            })
        },
        GroupWriteResponse::AdvanceRetention(_) => {
            GroupWriteResponse::AdvanceRetention(AdvanceRetentionResponse {
                placement: placement(),
                retained_offset: 17,
                group_commit_index: 44,
                incarnation: INCARNATION,
                hot_backlog: hot_backlog(),
            })
        },
        GroupWriteResponse::TouchStreamAccess(_) => {
            GroupWriteResponse::TouchStreamAccess(TouchStreamAccessResponse {
                placement: placement(),
                changed: true,
                expired: false,
                group_commit_index: 45,
            })
        },
        GroupWriteResponse::FlushCold(_) => GroupWriteResponse::FlushCold(FlushColdResponse {
            placement: placement(),
            hot_start_offset: 18,
            group_commit_index: 46,
            hot_backlog: hot_backlog(),
        }),
        GroupWriteResponse::CompactCold(_) => GroupWriteResponse::CompactCold(CompactColdResponse {
            placement: placement(),
            compacted_chunks: 2,
            compacted_bytes: 100,
            group_commit_index: 47,
        }),
        GroupWriteResponse::CloseStream(_) => GroupWriteResponse::CloseStream(CloseStreamResponse {
            placement: placement(),
            next_offset: 27,
            group_commit_index: 48,
            producer: Some(producer_ack(ProducerDelivery::Applied(receipt()))),
            incarnation: INCARNATION,
        }),
        GroupWriteResponse::DeleteStream(_) => {
            GroupWriteResponse::DeleteStream(DeleteStreamResponse {
                placement: placement(),
                group_commit_index: 49,
                expired: true,
                hot_backlog: hot_backlog(),
            })
        },
        GroupWriteResponse::AckColdGc(_) => GroupWriteResponse::AckColdGc(AckColdGcResponse {
            placement: placement(),
            removed: 2,
            group_commit_index: 50,
        }),
        GroupWriteResponse::PurgeBucket(_) => GroupWriteResponse::PurgeBucket(PurgeBucketResponse {
            placement: placement(),
            removed_streams: 3,
            pending_cold_gc_entries: 4,
            group_commit_index: 51,
        }),
        GroupWriteResponse::ImportGroupState(_) => {
            GroupWriteResponse::ImportGroupState(ImportGroupStateResponse {
                placement: placement(),
                buckets: 2,
                streams: 7,
                group_commit_index: 52,
                history: history(),
                already: true,
            })
        },
        GroupWriteResponse::NewHistory(_) => GroupWriteResponse::NewHistory(NewHistoryResponse {
            placement: placement(),
            history: history(),
            group_commit_index: 53,
        }),
        GroupWriteResponse::TidyStream(_) => GroupWriteResponse::TidyStream(TidyStreamResponse {
            placement: placement(),
            debt_remaining: true,
            group_commit_index: 54,
        }),
        GroupWriteResponse::DeferColdGc(_) => GroupWriteResponse::DeferColdGc(DeferColdGcResponse {
            placement: placement(),
            new_seq: Some(33),
            group_commit_index: 55,
        }),
        GroupWriteResponse::OffloadColdRefs(_) => {
            GroupWriteResponse::OffloadColdRefs(OffloadStreamColdRefsResponse {
                placement: placement(),
                removed: 2,
                remaining: 3,
                group_commit_index: 56,
            })
        },
        GroupWriteResponse::ExpireStreams(_) => {
            GroupWriteResponse::ExpireStreams(ExpireStreamsResponse {
                placement: placement(),
                removed: 4,
                group_commit_index: 57,
            })
        },
    });
    responses.extend(
        every!(ProducerDelivery {
            ProducerDelivery::Applied(_) => ProducerDelivery::Applied(receipt()),
            ProducerDelivery::Repeat(_) => ProducerDelivery::Repeat(receipt()),
            ProducerDelivery::Evicted => ProducerDelivery::Evicted,
        })
        .into_iter()
        // The sample above carries this delivery.
        .filter(|delivery| !matches!(delivery, ProducerDelivery::Applied(_)))
        .map(|delivery| GroupWriteResponse::Append(append(delivery))),
    );
    responses
}

/// A `HEAD` answer with every `Option` present and one with none.
pub fn head_stream_responses() -> Vec<HeadStreamResponse> {
    let present = HeadStreamResponse {
        placement: placement(),
        content_type: "application/json".to_owned(),
        tail_offset: 27,
        cold_hot_start_offset: 18,
        closed: true,
        stream_ttl_seconds: Some(3600),
        stream_expires_at_ms: Some(1_700_000_900_000),
        snapshot_offset: Some(16),
        snapshot_digest: Some("snapshot-digest".to_owned()),
        retained_offset: 4,
        incarnation: INCARNATION,
        inherited: Some(InheritedReport {
            previous_incarnation: 0x0034_5678_9abc_def0,
            offset: 17,
        }),
    };
    let absent = vary_head(present.clone(), u64::MAX);
    vec![present, absent]
}

/// A read answer.
pub fn read_stream_responses() -> Vec<ReadStreamResponse> {
    vec![ReadStreamResponse {
        placement: placement(),
        offset: 18,
        next_offset: 27,
        content_type: "application/json".to_owned(),
        payload: br#"{"id":3}"#.to_vec(),
        up_to_date: true,
        closed: false,
        incarnation: INCARNATION,
    }]
}

/// A backup snapshot that holds a value in every field: two buckets, an
/// erased one, a stream with every `Option` present and one with none, a
/// cold GC entry of each target, usage and a history event.
pub fn stream_snapshot() -> StreamSnapshot {
    StreamSnapshot {
        format_version: BACKUP_FORMAT_VERSION,
        buckets: vec!["tenant".to_owned(), "tenant-b".to_owned()],
        erased_buckets: vec!["tenant-c".to_owned()],
        streams: vec![full_stream(), bare_stream()],
        pending_cold_gc: vec![
            ColdGcEntry {
                seq: 31,
                bucket_id: "tenant".to_owned(),
                not_before_ms: 1_700_000_200_000,
                target: ColdGcTarget::Stream(BucketStreamId::new("tenant", "gone")),
                cold_generation: Some(6),
                defer_attempts: 0,
            },
            ColdGcEntry {
                seq: 32,
                bucket_id: "tenant-b".to_owned(),
                not_before_ms: 1_700_000_300_000,
                target: ColdGcTarget::Paths(vec![
                    "cold/tenant-b/journal/a".to_owned(),
                    "cold/tenant-b/journal/b".to_owned(),
                ]),
                cold_generation: None,
                defer_attempts: 3,
            },
        ],
        next_cold_gc_seq: 33,
        bucket_usage: vec![BucketUsageSnapshot {
            bucket_id: "tenant".to_owned(),
            usage: BucketUsage {
                committed_append_bytes: 1001,
                committed_records: 1002,
                committed_write_units: 1003,
                retained_bytes: 1004,
                stream_count: 2,
            },
        }],
        last_created_at_ms: 1_700_000_400_000,
        history: Some(HistoryEvent {
            id: history(),
            event_index: 14,
            previous: PREVIOUS_HISTORY,
            kept: 12,
            leader: leader(),
            imported: Some([0xcd; 32]),
        }),
    }
}

/// A stream with every `Option` present.
fn full_stream() -> StreamSnapshotEntry {
    StreamSnapshotEntry {
        metadata: StreamMetadata {
            stream_id: stream(),
            content_type: "application/json".to_owned(),
            status: StreamStatus::Closed,
            tail_offset: 27,
            last_stream_seq: Some("seq-9".to_owned()),
            stream_ttl_seconds: Some(3600),
            stream_expires_at_ms: Some(1_700_000_900_000),
            created_at_ms: NOW_MS,
            last_ttl_touch_at_ms: 1_700_000_050_000,
        },
        hot_start_offset: 18,
        payload: br#"{"id":3}"#.to_vec(),
        hot_segments: vec![HotPayloadSegment {
            start_offset: 19,
            end_offset: 26,
            payload_start: 1,
            payload_end: 8,
        }],
        cold_index_generation: 5,
        cold_chunks: vec![chunk(0, 10, "cold/tenant/orders/5/0-10")],
        external_segments: vec![object_ref()],
        retained_offset: 4,
        visible_snapshot: Some(StreamVisibleSnapshot {
            offset: 16,
            content_type: "application/json".to_owned(),
            payload: br#"{"count":2}"#.to_vec(),
            digest: "snapshot-digest".to_owned(),
            object: Some(external("cold/tenant/orders/snapshot/16")),
        }),
        producer_states: vec![ProducerSnapshot {
            producer_id: "writer".to_owned(),
            epoch: 2,
            last_seq: 3,
            claim_digest: Some([0xc1; 32]),
            newest: ProducerReceipt {
                receipt: WriteReceipt {
                    start_offset: 21,
                    next_offset: 27,
                    closed: true,
                },
                digest: Some([0xc2; 32]),
            },
            closed_epoch: Some(ClosedEpoch { epoch: 1, seq: 6 }),
            opened: Opened::BelowFloor,
            history: HISTORY,
            last_seen_ms: 1_700_000_060_000,
        }],
        retired_epoch_watermark: Some(7),
        history: HISTORY,
        inherited: Some(InheritedHistory {
            history: PREVIOUS_HISTORY,
            offset: 17,
        }),
    }
}

/// A stream with every `Option` absent: [`full_stream`] with them cleared,
/// under another name.
fn bare_stream() -> StreamSnapshotEntry {
    let bare = vary_stream_by(full_stream(), &mut Mask(u64::MAX));
    StreamSnapshotEntry {
        metadata: StreamMetadata {
            stream_id: BucketStreamId::new("tenant-b", "journal"),
            status: StreamStatus::Open,
            ..bare.metadata
        },
        ..bare
    }
}

/// Clears `Option`s by the bits of a mask, lowest first: a set bit makes
/// the next `Option` absent.
struct Mask(u64);

impl Mask {
    fn keep<T>(&mut self, value: Option<T>) -> Option<T> {
        let clear = self.0 & 1 == 1;
        self.0 = self.0.rotate_right(1);
        if clear { None } else { value }
    }
}

/// `command` with the `Option`s that `mask` selects absent.
pub fn vary_command(command: StreamCommand, mask: u64) -> StreamCommand {
    vary_command_by(command, &mut Mask(mask))
}

fn vary_producer(producer: Option<ProducerRequest>, mask: &mut Mask) -> Option<ProducerRequest> {
    let ProducerRequest {
        producer_id,
        producer_epoch,
        producer_seq,
        digest,
        content_fault,
    } = mask.keep(producer)?;
    Some(ProducerRequest {
        producer_id,
        producer_epoch,
        producer_seq,
        digest: mask.keep(digest),
        content_fault,
    })
}

fn vary_command_by(command: StreamCommand, mask: &mut Mask) -> StreamCommand {
    match command {
        StreamCommand::CreateStream {
            stream_id,
            content_type,
            initial_payload,
            close_after,
            stream_seq,
            stream_ttl_seconds,
            stream_expires_at_ms,
            now_ms,
        } => StreamCommand::CreateStream {
            stream_id,
            content_type,
            initial_payload,
            close_after,
            stream_seq: mask.keep(stream_seq),
            stream_ttl_seconds: mask.keep(stream_ttl_seconds),
            stream_expires_at_ms: mask.keep(stream_expires_at_ms),
            now_ms,
        },
        StreamCommand::CreateExternal {
            stream_id,
            content_type,
            initial_payload,
            message_count,
            close_after,
            stream_seq,
            stream_ttl_seconds,
            stream_expires_at_ms,
            now_ms,
        } => StreamCommand::CreateExternal {
            stream_id,
            content_type,
            initial_payload,
            message_count,
            close_after,
            stream_seq: mask.keep(stream_seq),
            stream_ttl_seconds: mask.keep(stream_ttl_seconds),
            stream_expires_at_ms: mask.keep(stream_expires_at_ms),
            now_ms,
        },
        StreamCommand::Append {
            stream_id,
            content_type,
            payload,
            close_after,
            stream_seq,
            producer,
            now_ms,
        } => StreamCommand::Append {
            stream_id,
            content_type: mask.keep(content_type),
            payload,
            close_after,
            stream_seq: mask.keep(stream_seq),
            producer: vary_producer(producer, mask),
            now_ms,
        },
        StreamCommand::AppendExternal {
            stream_id,
            content_type,
            payload,
            message_count,
            close_after,
            stream_seq,
            producer,
            now_ms,
        } => StreamCommand::AppendExternal {
            stream_id,
            content_type: mask.keep(content_type),
            payload,
            message_count,
            close_after,
            stream_seq: mask.keep(stream_seq),
            producer: vary_producer(producer, mask),
            now_ms,
        },
        StreamCommand::PublishSnapshot {
            stream_id,
            snapshot_offset,
            content_type,
            payload,
            now_ms,
            expected_incarnation,
        } => StreamCommand::PublishSnapshot {
            stream_id,
            snapshot_offset,
            content_type,
            payload,
            now_ms,
            expected_incarnation: mask.keep(expected_incarnation),
        },
        StreamCommand::PublishSnapshotExternal {
            stream_id,
            snapshot_offset,
            content_type,
            object,
            digest,
            now_ms,
            expected_incarnation,
        } => StreamCommand::PublishSnapshotExternal {
            stream_id,
            snapshot_offset,
            content_type,
            object,
            digest,
            now_ms,
            expected_incarnation: mask.keep(expected_incarnation),
        },
        StreamCommand::AdvanceRetention {
            stream_id,
            retained_offset,
            now_ms,
            expected_incarnation,
        } => StreamCommand::AdvanceRetention {
            stream_id,
            retained_offset,
            now_ms,
            expected_incarnation: mask.keep(expected_incarnation),
        },
        StreamCommand::Close {
            stream_id,
            stream_seq,
            producer,
            now_ms,
        } => StreamCommand::Close {
            stream_id,
            stream_seq: mask.keep(stream_seq),
            producer: vary_producer(producer, mask),
            now_ms,
        },
        StreamCommand::ImportSnapshot {
            snapshot,
            history,
            backup_digest,
            now_ms,
        } => StreamCommand::ImportSnapshot {
            snapshot: Box::new(vary_snapshot_by(*snapshot, mask)),
            history,
            backup_digest,
            now_ms,
        },
        StreamCommand::IfIncarnation {
            incarnation,
            command,
        } => StreamCommand::IfIncarnation {
            incarnation,
            command: Box::new(vary_command_by(*command, mask)),
        },
        // No `Option` in these.
        StreamCommand::CreateBucket { bucket_id } => StreamCommand::CreateBucket { bucket_id },
        StreamCommand::TouchStreamAccess {
            stream_id,
            now_ms,
            renew_ttl,
        } => StreamCommand::TouchStreamAccess {
            stream_id,
            now_ms,
            renew_ttl,
        },
        StreamCommand::FlushCold {
            stream_id,
            chunk,
            cold_generation,
        } => StreamCommand::FlushCold {
            stream_id,
            chunk,
            cold_generation,
        },
        StreamCommand::CompactCold {
            stream_id,
            old_chunks,
            replacement,
            gc_not_before_ms,
        } => StreamCommand::CompactCold {
            stream_id,
            old_chunks,
            replacement,
            gc_not_before_ms,
        },
        StreamCommand::DeleteStream { stream_id, now_ms } => {
            StreamCommand::DeleteStream { stream_id, now_ms }
        }
        StreamCommand::PurgeBucket { bucket_id } => StreamCommand::PurgeBucket { bucket_id },
        StreamCommand::AckColdGc { up_to_seq } => StreamCommand::AckColdGc { up_to_seq },
        StreamCommand::DeferColdGc { seq, not_before_ms } => {
            StreamCommand::DeferColdGc { seq, not_before_ms }
        }
        StreamCommand::NewHistory {
            history,
            now_ms,
            previous,
            kept,
            leader,
        } => StreamCommand::NewHistory {
            history,
            now_ms,
            previous,
            kept,
            leader,
        },
        StreamCommand::TidyStream { stream_id, now_ms } => {
            StreamCommand::TidyStream { stream_id, now_ms }
        }
        StreamCommand::ExpireStreams { now_ms, limit } => {
            StreamCommand::ExpireStreams { now_ms, limit }
        }
        StreamCommand::OffloadColdRefs { stream_id, refs } => {
            StreamCommand::OffloadColdRefs { stream_id, refs }
        }
    }
}

fn vary_ack(ack: Option<ProducerAck>, mask: &mut Mask) -> Option<ProducerAck> {
    let ProducerAck {
        epoch,
        seq,
        closed_epoch,
        delivery,
    } = mask.keep(ack)?;
    Some(ProducerAck {
        epoch,
        seq,
        closed_epoch: mask.keep(closed_epoch),
        delivery,
    })
}

/// `response` with the `Option`s that `mask` selects absent.
pub fn vary_response(response: GroupWriteResponse, mask: u64) -> GroupWriteResponse {
    let mask = &mut Mask(mask);
    match response {
        GroupWriteResponse::CreateStream(CreateStreamResponse {
            placement,
            next_offset,
            closed,
            already_exists,
            group_commit_index,
            incarnation,
            hot_backlog,
        }) => GroupWriteResponse::CreateStream(CreateStreamResponse {
            placement,
            next_offset,
            closed,
            already_exists,
            group_commit_index,
            incarnation,
            hot_backlog: mask.keep(hot_backlog),
        }),
        GroupWriteResponse::Append(AppendResponse {
            placement,
            start_offset,
            next_offset,
            stream_append_count,
            group_commit_index,
            closed,
            producer,
            stream_hot_bytes,
            group_hot_bytes,
            incarnation,
        }) => GroupWriteResponse::Append(AppendResponse {
            placement,
            start_offset,
            next_offset,
            stream_append_count,
            group_commit_index,
            closed,
            producer: vary_ack(producer, mask),
            stream_hot_bytes,
            group_hot_bytes,
            incarnation,
        }),
        GroupWriteResponse::PublishSnapshot(PublishSnapshotResponse {
            placement,
            snapshot_offset,
            snapshot_digest,
            group_commit_index,
            incarnation,
            hot_backlog,
        }) => GroupWriteResponse::PublishSnapshot(PublishSnapshotResponse {
            placement,
            snapshot_offset,
            snapshot_digest,
            group_commit_index,
            incarnation,
            hot_backlog: mask.keep(hot_backlog),
        }),
        GroupWriteResponse::AdvanceRetention(AdvanceRetentionResponse {
            placement,
            retained_offset,
            group_commit_index,
            incarnation,
            hot_backlog,
        }) => GroupWriteResponse::AdvanceRetention(AdvanceRetentionResponse {
            placement,
            retained_offset,
            group_commit_index,
            incarnation,
            hot_backlog: mask.keep(hot_backlog),
        }),
        GroupWriteResponse::TouchStreamAccess(TouchStreamAccessResponse {
            placement,
            changed,
            expired,
            group_commit_index,
        }) => GroupWriteResponse::TouchStreamAccess(TouchStreamAccessResponse {
            placement,
            changed,
            expired,
            group_commit_index,
        }),
        GroupWriteResponse::FlushCold(FlushColdResponse {
            placement,
            hot_start_offset,
            group_commit_index,
            hot_backlog,
        }) => GroupWriteResponse::FlushCold(FlushColdResponse {
            placement,
            hot_start_offset,
            group_commit_index,
            hot_backlog: mask.keep(hot_backlog),
        }),
        GroupWriteResponse::CompactCold(CompactColdResponse {
            placement,
            compacted_chunks,
            compacted_bytes,
            group_commit_index,
        }) => GroupWriteResponse::CompactCold(CompactColdResponse {
            placement,
            compacted_chunks,
            compacted_bytes,
            group_commit_index,
        }),
        GroupWriteResponse::CloseStream(CloseStreamResponse {
            placement,
            next_offset,
            group_commit_index,
            producer,
            incarnation,
        }) => GroupWriteResponse::CloseStream(CloseStreamResponse {
            placement,
            next_offset,
            group_commit_index,
            producer: vary_ack(producer, mask),
            incarnation,
        }),
        GroupWriteResponse::DeleteStream(DeleteStreamResponse {
            placement,
            group_commit_index,
            expired,
            hot_backlog,
        }) => GroupWriteResponse::DeleteStream(DeleteStreamResponse {
            placement,
            group_commit_index,
            expired,
            hot_backlog: mask.keep(hot_backlog),
        }),
        GroupWriteResponse::AckColdGc(AckColdGcResponse {
            placement,
            removed,
            group_commit_index,
        }) => GroupWriteResponse::AckColdGc(AckColdGcResponse {
            placement,
            removed,
            group_commit_index,
        }),
        GroupWriteResponse::PurgeBucket(PurgeBucketResponse {
            placement,
            removed_streams,
            pending_cold_gc_entries,
            group_commit_index,
        }) => GroupWriteResponse::PurgeBucket(PurgeBucketResponse {
            placement,
            removed_streams,
            pending_cold_gc_entries,
            group_commit_index,
        }),
        GroupWriteResponse::ImportGroupState(ImportGroupStateResponse {
            placement,
            buckets,
            streams,
            group_commit_index,
            history,
            already,
        }) => GroupWriteResponse::ImportGroupState(ImportGroupStateResponse {
            placement,
            buckets,
            streams,
            group_commit_index,
            history,
            already,
        }),
        GroupWriteResponse::NewHistory(NewHistoryResponse {
            placement,
            history,
            group_commit_index,
        }) => GroupWriteResponse::NewHistory(NewHistoryResponse {
            placement,
            history,
            group_commit_index,
        }),
        GroupWriteResponse::TidyStream(TidyStreamResponse {
            placement,
            debt_remaining,
            group_commit_index,
        }) => GroupWriteResponse::TidyStream(TidyStreamResponse {
            placement,
            debt_remaining,
            group_commit_index,
        }),
        GroupWriteResponse::DeferColdGc(DeferColdGcResponse {
            placement,
            new_seq,
            group_commit_index,
        }) => GroupWriteResponse::DeferColdGc(DeferColdGcResponse {
            placement,
            new_seq: mask.keep(new_seq),
            group_commit_index,
        }),
        GroupWriteResponse::OffloadColdRefs(OffloadStreamColdRefsResponse {
            placement,
            removed,
            remaining,
            group_commit_index,
        }) => GroupWriteResponse::OffloadColdRefs(OffloadStreamColdRefsResponse {
            placement,
            removed,
            remaining,
            group_commit_index,
        }),
        GroupWriteResponse::ExpireStreams(ExpireStreamsResponse {
            placement,
            removed,
            group_commit_index,
        }) => GroupWriteResponse::ExpireStreams(ExpireStreamsResponse {
            placement,
            removed,
            group_commit_index,
        }),
    }
}

/// `head` with the `Option`s that `mask` selects absent.
pub fn vary_head(head: HeadStreamResponse, mask: u64) -> HeadStreamResponse {
    let mask = &mut Mask(mask);
    let HeadStreamResponse {
        placement,
        content_type,
        tail_offset,
        cold_hot_start_offset,
        closed,
        stream_ttl_seconds,
        stream_expires_at_ms,
        snapshot_offset,
        snapshot_digest,
        retained_offset,
        incarnation,
        inherited,
    } = head;
    HeadStreamResponse {
        placement,
        content_type,
        tail_offset,
        cold_hot_start_offset,
        closed,
        stream_ttl_seconds: mask.keep(stream_ttl_seconds),
        stream_expires_at_ms: mask.keep(stream_expires_at_ms),
        snapshot_offset: mask.keep(snapshot_offset),
        snapshot_digest: mask.keep(snapshot_digest),
        retained_offset,
        incarnation,
        inherited: mask.keep(inherited),
    }
}

/// `snapshot` with the `Option`s that `mask` selects absent.
pub fn vary_snapshot(snapshot: StreamSnapshot, mask: u64) -> StreamSnapshot {
    vary_snapshot_by(snapshot, &mut Mask(mask))
}

fn vary_snapshot_by(snapshot: StreamSnapshot, mask: &mut Mask) -> StreamSnapshot {
    let StreamSnapshot {
        format_version,
        buckets,
        erased_buckets,
        streams,
        pending_cold_gc,
        next_cold_gc_seq,
        bucket_usage,
        last_created_at_ms,
        history,
    } = snapshot;
    StreamSnapshot {
        format_version,
        buckets,
        erased_buckets,
        streams: streams
            .into_iter()
            .map(|stream| vary_stream_by(stream, mask))
            .collect(),
        pending_cold_gc: pending_cold_gc
            .into_iter()
            .map(
                |ColdGcEntry {
                     seq,
                     bucket_id,
                     not_before_ms,
                     target,
                     cold_generation,
                     defer_attempts,
                 }| ColdGcEntry {
                    seq,
                    bucket_id,
                    not_before_ms,
                    target,
                    cold_generation: mask.keep(cold_generation),
                    defer_attempts,
                },
            )
            .collect(),
        next_cold_gc_seq,
        bucket_usage,
        last_created_at_ms,
        history: mask.keep(history).map(|history| HistoryEvent {
            imported: mask.keep(history.imported),
            ..history
        }),
    }
}

fn vary_stream_by(stream: StreamSnapshotEntry, mask: &mut Mask) -> StreamSnapshotEntry {
    let StreamSnapshotEntry {
        metadata,
        hot_start_offset,
        payload,
        hot_segments,
        cold_index_generation,
        cold_chunks,
        external_segments,
        retained_offset,
        visible_snapshot,
        producer_states,
        retired_epoch_watermark,
        history,
        inherited,
    } = stream;
    let StreamMetadata {
        stream_id,
        content_type,
        status,
        tail_offset,
        last_stream_seq,
        stream_ttl_seconds,
        stream_expires_at_ms,
        created_at_ms,
        last_ttl_touch_at_ms,
    } = metadata;
    StreamSnapshotEntry {
        metadata: StreamMetadata {
            stream_id,
            content_type,
            status,
            tail_offset,
            last_stream_seq: mask.keep(last_stream_seq),
            stream_ttl_seconds: mask.keep(stream_ttl_seconds),
            stream_expires_at_ms: mask.keep(stream_expires_at_ms),
            created_at_ms,
            last_ttl_touch_at_ms,
        },
        hot_start_offset,
        payload,
        hot_segments,
        cold_index_generation,
        cold_chunks,
        external_segments,
        retained_offset,
        visible_snapshot: mask
            .keep(visible_snapshot)
            .map(|visible| StreamVisibleSnapshot {
                object: mask.keep(visible.object),
                ..visible
            }),
        producer_states: producer_states
            .into_iter()
            .map(|producer| ProducerSnapshot {
                claim_digest: mask.keep(producer.claim_digest),
                newest: ProducerReceipt {
                    digest: mask.keep(producer.newest.digest),
                    ..producer.newest
                },
                closed_epoch: mask.keep(producer.closed_epoch),
                ..producer
            })
            .collect(),
        retired_epoch_watermark: mask.keep(retired_epoch_watermark),
        history,
        inherited: mask.keep(inherited),
    }
}
