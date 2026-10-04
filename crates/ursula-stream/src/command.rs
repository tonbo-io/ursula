use std::fmt;

use bytes::Bytes;
use serde::Deserialize;
use serde::Serialize;
use ursula_shard::BucketStreamId;

use crate::model::ColdChunkRef;
use crate::model::ExternalPayloadRef;
use crate::model::ObjectPayloadRef;
use crate::model::ProducerRequest;
use crate::model::StreamAttrs;
use crate::snapshot::StreamSnapshot;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamCommand {
    CreateBucket {
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
        #[serde(default)]
        expected_digest: Option<String>,
        now_ms: u64,
    },
    /// Publishes a snapshot whose body the proposer staged as a cold-tier
    /// object (feature level 5, bounded-state F16). The proposer computed
    /// `digest` with [`crate::SnapshotDigest`] while staging; replicated
    /// state keeps the reference, never the body.
    PublishSnapshotExternal {
        stream_id: BucketStreamId,
        snapshot_offset: u64,
        content_type: String,
        object: ExternalPayloadRef,
        digest: String,
        expected_digest: Option<String>,
        now_ms: u64,
    },
    AdvanceRetention {
        stream_id: BucketStreamId,
        retained_offset: u64,
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
        /// Cold generation of the incarnation the chunk was planned from
        /// (F14g). From feature level 1 apply rejects the flush as stale when
        /// it differs from the live stream's, so a flush racing a delete and
        /// recreate cannot publish one incarnation's chunk into another.
        /// `None` (proposers before this field) skips the check.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cold_generation: Option<u64>,
    },
    /// Replaces a contiguous run of immutable cold chunks with one equivalent
    /// object. The external cold-index page update is completed before this
    /// command is replicated; applying it schedules the old paths for delayed
    /// reclamation.
    CompactCold {
        stream_id: BucketStreamId,
        old_chunks: Vec<ColdChunkRef>,
        replacement: ColdChunkRef,
        gc_not_before_ms: u64,
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
    /// Administrator-triggered tenant offboarding: removes every stream in
    /// the bucket and the bucket itself in this group. Monotonic
    /// aggregate usage is retained for asynchronous accounting. Idempotent —
    /// purging an absent bucket reports zero removals.
    PurgeBucket {
        bucket_id: String,
    },
    /// Confirms the leader's background worker has physically reclaimed every
    /// queued cold-GC entry with `seq <= up_to_seq`; removes them from the
    /// replicated queue. Idempotent under replay.
    AckColdGc {
        up_to_seq: u64,
    },
    /// Bounded-state F14b (feature level 1): moves the pending cold-GC entry
    /// `seq`, whose reclamation failed, to the tail of the queue under a new
    /// sequence number, due no earlier than `not_before_ms`, so one failing
    /// entry no longer blocks every entry behind it. An absent `seq` is a
    /// no-op success, which keeps replays idempotent.
    DeferColdGc {
        seq: u64,
        not_before_ms: u64,
    },
    /// Replaces this group's entire state with a backup snapshot.
    ///
    /// Restore-only: the target group must be empty. Travelling as a normal
    /// replicated command keeps every replica of the restored cluster
    /// deterministic while the cluster retains its own raft identity and
    /// membership -- nothing from the backed-up cluster's raft metadata is
    /// reused.
    ImportSnapshot {
        snapshot: Box<StreamSnapshot>,
    },
    /// Raises this group's replicated feature level to
    /// `max(current, level)`; never lowers it. Idempotent under replay.
    /// See [`crate::MAX_SUPPORTED_FEATURE_LEVEL`] for what each level enables.
    /// Proposers must only send levels every replica supports; apply itself
    /// accepts any value so that every replica applies it identically.
    SetFeatureLevel {
        level: u32,
    },
    /// Bounded normalization of one stream's replicated state (bounded-state
    /// F0 `TidyStream`, feature level 1): collapses message records below
    /// the seal point (F4a; from level 4 converts legacy message records to
    /// the F4b representation instead), stamps and expires idle producers
    /// and trims the receipt window (F3). Each command does bounded work; a leader-side
    /// driver repeats it while debt remains. Idempotent. Appended last so
    /// older variants keep their serialized positions.
    TidyStream {
        stream_id: BucketStreamId,
        now_ms: u64,
    },
    /// Bounded-state F5 (feature level 3): removes `refs` from the stream's
    /// state-held external payload locators after the leader wrote their
    /// cold-index page entries. Apply removes exactly the listed refs that are
    /// still present and queues no GC, because the pages now reference the
    /// objects; refs already gone (replay, retention, delete) are skipped, so
    /// replays are idempotent. Appended last so older variants keep their
    /// serialized positions.
    OffloadColdRefs {
        stream_id: BucketStreamId,
        refs: Vec<ObjectPayloadRef>,
    },
}

/// Fixed per-command allowance of [`StreamCommand::log_bytes_estimate`]:
/// ids, offsets, headers and framing.
pub const COMMAND_LOG_OVERHEAD_BYTES: u64 = 128;
/// Allowance per cold-chunk reference a command carries.
const CHUNK_REF_LOG_BYTES: u64 = 160;

fn len_u64(len: usize) -> u64 {
    u64::try_from(len).unwrap_or(u64::MAX)
}

impl StreamCommand {
    /// Approximate bytes this command occupies in the Raft log: its payload
    /// bytes plus a fixed allowance per command, record boundary and chunk
    /// reference. Snapshot cadence (bounded-state F12e) counts log bytes
    /// with it, so it needs to track payload volume, not exact encodings.
    pub fn log_bytes_estimate(&self) -> u64 {
        let variable = match self {
            Self::CreateStream {
                initial_payload, ..
            } => len_u64(initial_payload.len()),
            Self::CreateExternal { record_ends, .. } | Self::AppendExternal { record_ends, .. } => {
                len_u64(record_ends.len()).saturating_mul(8)
            }
            Self::Append { payload, .. } | Self::PublishSnapshot { payload, .. } => {
                len_u64(payload.len())
            }
            Self::AppendBatch { payloads, .. } => payloads
                .iter()
                .map(|payload| len_u64(payload.len()).saturating_add(8))
                .fold(0, u64::saturating_add),
            Self::FlushCold { .. } => CHUNK_REF_LOG_BYTES,
            Self::CompactCold { old_chunks, .. } => len_u64(old_chunks.len())
                .saturating_add(1)
                .saturating_mul(CHUNK_REF_LOG_BYTES),
            Self::ImportSnapshot { snapshot } => {
                len_u64(snapshot.streams.len()).saturating_mul(COMMAND_LOG_OVERHEAD_BYTES)
            }
            _ => 0,
        };
        COMMAND_LOG_OVERHEAD_BYTES.saturating_add(variable)
    }
}

impl fmt::Display for StreamCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreateBucket { bucket_id } => write!(f, "create_bucket:{bucket_id}"),
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
            Self::PublishSnapshotExternal {
                stream_id,
                snapshot_offset,
                object,
                ..
            } => write!(
                f,
                "publish_snapshot_external:{stream_id}:{snapshot_offset}:{} bytes",
                object.payload_len
            ),
            Self::AdvanceRetention {
                stream_id,
                retained_offset,
                ..
            } => write!(f, "advance_retention:{stream_id}:{retained_offset}"),
            Self::TouchStreamAccess {
                stream_id,
                renew_ttl,
                ..
            } => write!(f, "touch_stream_access:{stream_id}:renew_ttl={renew_ttl}"),
            Self::UpdateStreamAttrs { stream_id, .. } => {
                write!(f, "update_stream_attrs:{stream_id}")
            }
            Self::FlushCold {
                stream_id, chunk, ..
            } => write!(
                f,
                "flush_cold:{stream_id}:{}..{}",
                chunk.start_offset, chunk.end_offset
            ),
            Self::CompactCold {
                stream_id,
                old_chunks,
                replacement,
                ..
            } => write!(
                f,
                "compact_cold:{stream_id}:{} chunks:{}..{}",
                old_chunks.len(),
                replacement.start_offset,
                replacement.end_offset
            ),
            Self::Close { stream_id, .. } => write!(f, "close_stream:{stream_id}"),
            Self::DeleteStream { stream_id } => write!(f, "delete_stream:{stream_id}"),
            Self::PurgeBucket { bucket_id } => write!(f, "purge_bucket:{bucket_id}"),
            Self::AckColdGc { up_to_seq } => write!(f, "ack_cold_gc:up_to_seq={up_to_seq}"),
            Self::DeferColdGc { seq, .. } => write!(f, "defer_cold_gc:seq={seq}"),
            Self::ImportSnapshot { snapshot } => write!(
                f,
                "import_snapshot:buckets={}:streams={}",
                snapshot.buckets.len(),
                snapshot.streams.len()
            ),
            Self::SetFeatureLevel { level } => write!(f, "set_feature_level:{level}"),
            Self::TidyStream { stream_id, .. } => write!(f, "tidy_stream:{stream_id}"),
            Self::OffloadColdRefs { stream_id, refs } => {
                write!(f, "offload_cold_refs:{stream_id}:{} refs", refs.len())
            }
        }
    }
}
