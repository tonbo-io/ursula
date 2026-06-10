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
        initial_payload: Vec<u8>,
        close_after: bool,
        stream_seq: Option<String>,
        producer: Option<ProducerRequest>,
        stream_ttl_seconds: Option<u64>,
        stream_expires_at_ms: Option<u64>,
        forked_from: Option<BucketStreamId>,
        fork_offset: Option<u64>,
        // `default` keeps pre-attrs WAL records decodable.
        #[serde(default)]
        attrs: Option<StreamAttrs>,
        now_ms: u64,
    },
    CreateExternal {
        stream_id: BucketStreamId,
        content_type: String,
        initial_payload: ExternalPayloadRef,
        close_after: bool,
        stream_seq: Option<String>,
        producer: Option<ProducerRequest>,
        stream_ttl_seconds: Option<u64>,
        stream_expires_at_ms: Option<u64>,
        forked_from: Option<BucketStreamId>,
        fork_offset: Option<u64>,
        // `default` keeps pre-attrs WAL records decodable.
        #[serde(default)]
        attrs: Option<StreamAttrs>,
        now_ms: u64,
    },
    Append {
        stream_id: BucketStreamId,
        content_type: Option<String>,
        payload: Vec<u8>,
        close_after: bool,
        stream_seq: Option<String>,
        producer: Option<ProducerRequest>,
        now_ms: u64,
    },
    AppendExternal {
        stream_id: BucketStreamId,
        content_type: Option<String>,
        payload: ExternalPayloadRef,
        close_after: bool,
        stream_seq: Option<String>,
        producer: Option<ProducerRequest>,
        now_ms: u64,
    },
    AppendBatch {
        stream_id: BucketStreamId,
        content_type: Option<String>,
        payloads: Vec<Vec<u8>>,
        producer: Option<ProducerRequest>,
        now_ms: u64,
    },
    PublishSnapshot {
        stream_id: BucketStreamId,
        snapshot_offset: u64,
        content_type: String,
        payload: Vec<u8>,
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
    AddForkRef {
        stream_id: BucketStreamId,
        now_ms: u64,
    },
    ReleaseForkRef {
        stream_id: BucketStreamId,
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
