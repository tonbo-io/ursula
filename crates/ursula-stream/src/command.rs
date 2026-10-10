use std::fmt;

use bytes::Bytes;
use serde::Deserialize;
use serde::Serialize;
use ursula_shard::BucketStreamId;

use crate::model::ColdChunkRef;
use crate::model::ExternalPayloadRef;
use crate::model::ObjectPayloadRef;
use crate::model::ProducerRequest;
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
        now_ms: u64,
    },
    CreateExternal {
        stream_id: BucketStreamId,
        content_type: String,
        initial_payload: ExternalPayloadRef,
        record_ends: Vec<u64>,
        close_after: bool,
        stream_seq: Option<String>,
        producer: Option<ProducerRequest>,
        stream_ttl_seconds: Option<u64>,
        stream_expires_at_ms: Option<u64>,
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
    },
    AppendExternal {
        stream_id: BucketStreamId,
        content_type: Option<String>,
        payload: ExternalPayloadRef,
        record_ends: Vec<u64>,
        close_after: bool,
        stream_seq: Option<String>,
        producer: Option<ProducerRequest>,
        now_ms: u64,
    },
    /// `expected_incarnation` (JSON streams only): the stream incarnation
    /// whose byte before `snapshot_offset` the proposer read and found LF.
    /// Apply needs it when that byte is not hot, and refuses a mismatch.
    PublishSnapshot {
        stream_id: BucketStreamId,
        snapshot_offset: u64,
        content_type: String,
        payload: Bytes,
        now_ms: u64,
        expected_incarnation: Option<u64>,
    },
    /// Publishes a snapshot whose body the proposer staged as a cold-tier
    /// object (bounded-state F16). The proposer computed
    /// `digest` with [`crate::SnapshotDigest`] while staging; replicated
    /// state keeps the reference, never the body.
    PublishSnapshotExternal {
        stream_id: BucketStreamId,
        snapshot_offset: u64,
        content_type: String,
        object: ExternalPayloadRef,
        digest: String,
        now_ms: u64,
        expected_incarnation: Option<u64>,
    },
    /// `expected_incarnation` as on [`Self::PublishSnapshot`].
    AdvanceRetention {
        stream_id: BucketStreamId,
        retained_offset: u64,
        now_ms: u64,
        expected_incarnation: Option<u64>,
    },
    TouchStreamAccess {
        stream_id: BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
    },
    FlushCold {
        stream_id: BucketStreamId,
        chunk: ColdChunkRef,
        /// Cold generation of the incarnation the chunk was planned from
        /// (F14g). Apply rejects the flush as stale when it differs from the
        /// live stream's, so a flush racing a delete and recreate cannot
        /// publish one incarnation's chunk into another.
        cold_generation: u64,
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
    /// Bounded-state F14b: moves the pending cold-GC entry
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
    /// Bounded normalization of one stream's replicated state (bounded-state
    /// `TidyStream`): seals record offsets below the seal point (F1),
    /// expires idle producers and trims the receipt window (F3). Each command
    /// does bounded work; a leader-side driver repeats it while debt remains.
    /// Idempotent.
    TidyStream {
        stream_id: BucketStreamId,
        now_ms: u64,
    },
    /// Bounded-state F5: removes `refs` from the stream's
    /// state-held external payload locators after the leader wrote their
    /// cold-index page entries. Apply removes exactly the listed refs that are
    /// still present and queues no GC, because the pages now reference the
    /// objects; refs already gone (replay, retention, delete) are skipped, so
    /// replays are idempotent.
    OffloadColdRefs {
        stream_id: BucketStreamId,
        refs: Vec<ObjectPayloadRef>,
    },
    /// The `Stream-Incarnation` request precondition (D12): `command` (a
    /// create, append, close, delete, snapshot publish or retention move)
    /// applies only if its stream is the incarnation `incarnation` (its
    /// `created_at_ms`) when this entry applies; otherwise apply refuses it
    /// with [`crate::StreamErrorCode::IncarnationMismatch`] and changes
    /// nothing (see [`crate::StreamStateMachine::incarnation_precondition`]).
    /// A request without the header is never wrapped, so its command and log
    /// encoding are unchanged.
    IfIncarnation {
        incarnation: u64,
        command: Box<StreamCommand>,
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
    /// bytes plus a fixed allowance per command, message end and chunk
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
            Self::FlushCold { .. } => CHUNK_REF_LOG_BYTES,
            Self::CompactCold { old_chunks, .. } => len_u64(old_chunks.len())
                .saturating_add(1)
                .saturating_mul(CHUNK_REF_LOG_BYTES),
            // Each stream's allowance, hot payload, inline snapshot body and
            // chunk references: a restored group's import is one entry.
            Self::ImportSnapshot { snapshot } => snapshot
                .streams
                .iter()
                .map(|stream| {
                    let visible = stream
                        .visible_snapshot
                        .as_ref()
                        .map_or(0, |visible| visible.payload.len());
                    COMMAND_LOG_OVERHEAD_BYTES
                        .saturating_add(len_u64(stream.payload.len()))
                        .saturating_add(len_u64(visible))
                        .saturating_add(
                            len_u64(stream.cold_chunks.len()).saturating_mul(CHUNK_REF_LOG_BYTES),
                        )
                })
                .fold(0, u64::saturating_add),
            // The wrapped command plus the expected incarnation (a u64).
            Self::IfIncarnation { command, .. } => {
                return command.log_bytes_estimate().saturating_add(8);
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
            Self::TidyStream { stream_id, .. } => write!(f, "tidy_stream:{stream_id}"),
            Self::OffloadColdRefs { stream_id, refs } => {
                write!(f, "offload_cold_refs:{stream_id}:{} refs", refs.len())
            }
            Self::IfIncarnation {
                incarnation,
                command,
            } => write!(f, "{command}:if_incarnation={incarnation}"),
        }
    }
}
