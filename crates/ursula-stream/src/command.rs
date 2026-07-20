use std::fmt;

use bytes::Bytes;
use serde::Deserialize;
use serde::Serialize;
use ursula_shard::BucketStreamId;

use crate::model::ColdChunkRef;
use crate::model::ExternalPayloadRef;
use crate::model::ProducerRequest;
use crate::model::StreamAttrs;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamCommand {
    CreateBucket {
        bucket_id: String,
    },
    DeleteBucket {
        bucket_id: String,
    },
    CreateStream {
        stream_id: BucketStreamId,
        content_type: String,
        initial_payload: Bytes,
        close_after: bool,
        stream_seq: Option<String>,
        producer: Option<ProducerRequest>,
        stream_ttl_seconds: Option<u64>,
        stream_expires_at_ms: Option<u64>,
        // `default` keeps pre-attrs replicated records decodable.
        #[serde(default)]
        attrs: Option<StreamAttrs>,
        now_ms: u64,
    },
    CreateExternal {
        stream_id: BucketStreamId,
        content_type: String,
        initial_payload: ExternalPayloadRef,
        #[serde(default)]
        record_ends: Vec<u64>,
        close_after: bool,
        stream_seq: Option<String>,
        producer: Option<ProducerRequest>,
        stream_ttl_seconds: Option<u64>,
        stream_expires_at_ms: Option<u64>,
        // `default` keeps pre-attrs replicated records decodable.
        #[serde(default)]
        attrs: Option<StreamAttrs>,
        now_ms: u64,
    },
    Append {
        stream_id: BucketStreamId,
        content_type: Option<String>,
        payload: Bytes,
        close_after: bool,
        stream_seq: Option<String>,
        producer: Option<ProducerRequest>,
        now_ms: u64,
        record_match: Option<u64>,
    },
    AppendExternal {
        stream_id: BucketStreamId,
        content_type: Option<String>,
        payload: ExternalPayloadRef,
        #[serde(default)]
        record_ends: Vec<u64>,
        close_after: bool,
        stream_seq: Option<String>,
        producer: Option<ProducerRequest>,
        now_ms: u64,
        record_match: Option<u64>,
    },
    AppendBatch {
        stream_id: BucketStreamId,
        content_type: Option<String>,
        payloads: Vec<Bytes>,
        producer: Option<ProducerRequest>,
        now_ms: u64,
    },
    PublishSnapshot {
        stream_id: BucketStreamId,
        snapshot_offset: u64,
        content_type: String,
        payload: Bytes,
        now_ms: u64,
    },
    TouchStreamAccess {
        stream_id: BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
    },
    UpdateStreamAttrs {
        stream_id: BucketStreamId,
        attrs: Option<StreamAttrs>,
        now_ms: u64,
    },
    FlushCold {
        stream_id: BucketStreamId,
        chunk: ColdChunkRef,
    },
    Close {
        stream_id: BucketStreamId,
        stream_seq: Option<String>,
        producer: Option<ProducerRequest>,
        now_ms: u64,
    },
    DeleteStream {
        stream_id: BucketStreamId,
    },
    /// Confirms the leader's background worker has physically reclaimed every
    /// queued cold-GC entry with `seq <= up_to_seq`; removes them from the
    /// replicated queue. Idempotent under replay.
    AckColdGc {
        up_to_seq: u64,
    },
}

impl fmt::Display for StreamCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreateBucket { bucket_id } => write!(f, "create_bucket:{bucket_id}"),
            Self::DeleteBucket { bucket_id } => write!(f, "delete_bucket:{bucket_id}"),
            Self::CreateStream { stream_id, .. } => write!(f, "create_stream:{stream_id}"),
            Self::CreateExternal {
                stream_id,
                initial_payload,
                ..
            } => write!(
                f,
                "create_external:{stream_id}:{} bytes",
                initial_payload.payload_len
            ),
            Self::Append {
                stream_id, payload, ..
            } => write!(f, "append:{stream_id}:{} bytes", payload.len()),
            Self::AppendExternal {
                stream_id, payload, ..
            } => write!(
                f,
                "append_external:{stream_id}:{} bytes",
                payload.payload_len
            ),
            Self::AppendBatch {
                stream_id,
                payloads,
                ..
            } => write!(f, "append_batch:{stream_id}:{} items", payloads.len()),
            Self::PublishSnapshot {
                stream_id,
                snapshot_offset,
                payload,
                ..
            } => write!(
                f,
                "publish_snapshot:{stream_id}:{snapshot_offset}:{} bytes",
                payload.len()
            ),
            Self::TouchStreamAccess {
                stream_id,
                renew_ttl,
                ..
            } => write!(f, "touch_stream_access:{stream_id}:renew_ttl={renew_ttl}"),
            Self::UpdateStreamAttrs { stream_id, .. } => {
                write!(f, "update_stream_attrs:{stream_id}")
            }
            Self::FlushCold { stream_id, chunk } => write!(
                f,
                "flush_cold:{stream_id}:{}..{}",
                chunk.start_offset, chunk.end_offset
            ),
            Self::Close { stream_id, .. } => write!(f, "close_stream:{stream_id}"),
            Self::DeleteStream { stream_id } => write!(f, "delete_stream:{stream_id}"),
            Self::AckColdGc { up_to_seq } => write!(f, "ack_cold_gc:up_to_seq={up_to_seq}"),
        }
    }
}
