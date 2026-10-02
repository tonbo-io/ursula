//! L1 drivers: the real `StreamStateMachine` driven with exactly the commands
//! the runtime issues (create, append, external append, flush and pack,
//! compaction, checkpoint and retention).

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use bytes::Bytes;
use ursula_runtime::StreamAppendCount;
use ursula_shard::BucketStreamId;
use ursula_stream::ColdChunkRef;
use ursula_stream::ColdFlushCandidate;
use ursula_stream::ExternalPayloadRef;
use ursula_stream::ProducerRequest;
use ursula_stream::StreamCommand;
use ursula_stream::StreamResponse;
use ursula_stream::StreamStateMachine;

pub const T0: u64 = 1_759_300_000_000;
pub const MIB: usize = 1 << 20;
pub const JSON: &str = "application/json";

pub fn ok(response: StreamResponse, what: &str) -> Result<StreamResponse> {
    if let StreamResponse::Error { code, message, .. } = &response {
        bail!("{what}: {code:?}: {message}");
    }
    Ok(response)
}

/// A stream id with a distinct name per stream. Streams that differ only by
/// affinity fall back to `HashMap` order in the flush planner, which makes
/// multi-stream runs differ between processes (§7.1).
pub fn sid(bucket: &str, affinity: &str, name: &str) -> BucketStreamId {
    BucketStreamId::with_affinity(bucket, affinity, name)
}

/// Raises the group to `level` (C0) so level-gated bounds apply, as on a
/// cluster whose operator raised it (W3/W4 run at level 1: F3, F4a).
pub fn raise_feature_level(m: &mut StreamStateMachine, level: u32) -> Result<()> {
    ok(
        m.apply(StreamCommand::SetFeatureLevel { level }),
        "set feature level",
    )?;
    Ok(())
}

pub fn create_bucket(m: &mut StreamStateMachine, bucket: &str) -> Result<()> {
    ok(
        m.apply(StreamCommand::CreateBucket {
            bucket_id: bucket.to_owned(),
        }),
        "create bucket",
    )?;
    Ok(())
}

pub fn create_stream(
    m: &mut StreamStateMachine,
    id: &BucketStreamId,
    ttl_seconds: Option<u64>,
    expires_at_ms: Option<u64>,
    now_ms: u64,
) -> Result<()> {
    ok(
        m.apply(StreamCommand::CreateStream {
            stream_id: id.clone(),
            content_type: JSON.to_owned(),
            initial_payload: Bytes::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: ttl_seconds,
            stream_expires_at_ms: expires_at_ms,
            attrs: None,
            now_ms,
        }),
        "create stream",
    )?;
    Ok(())
}

pub fn append(
    m: &mut StreamStateMachine,
    id: &BucketStreamId,
    payload: Vec<u8>,
    producer: Option<ProducerRequest>,
    now_ms: u64,
) -> StreamResponse {
    m.apply(StreamCommand::Append {
        stream_id: id.clone(),
        content_type: Some(JSON.to_owned()),
        payload: Bytes::from(payload),
        close_after: false,
        stream_seq: None,
        producer,
        now_ms,
        record_match: None,
    })
}

/// What the HTTP layer submits for a body of 1 MiB or more once it has staged
/// the bytes at `new_external_payload_path` (record ends computed over the body).
pub fn append_external(
    m: &mut StreamStateMachine,
    id: &BucketStreamId,
    payload_len: u64,
    record_ends: Vec<u64>,
    now_ms: u64,
) -> StreamResponse {
    m.apply(StreamCommand::AppendExternal {
        stream_id: id.clone(),
        content_type: Some(JSON.to_owned()),
        payload: ExternalPayloadRef {
            s3_path: ursula_runtime::new_external_payload_path(id),
            payload_len,
            object_size: payload_len,
        },
        record_ends,
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms,
        record_match: None,
    })
}

/// Deterministic pack path with the same shape and length as
/// `ursula_runtime`'s `new_cold_pack_path` (whose clock component would make
/// snapshot sizes vary between runs only in content, not length).
#[derive(Debug, Default)]
pub struct PackPaths {
    next: u64,
}

impl PackPaths {
    pub fn next(&mut self, bucket_id: &str, raft_group_id: u32) -> String {
        self.next = self.next.saturating_add(1);
        let sequence = self.next.saturating_add(1 << 40);
        let pseudo_nanos = u128::from(T0).saturating_mul(1_000_000) + u128::from(self.next);
        format!("{bucket_id}/_packs/{raft_group_id:08x}/{pseudo_nanos:032x}-{sequence:016x}.bin")
    }
}

#[derive(Debug, Default, Clone, Copy, serde::Serialize)]
pub struct FlushStats {
    pub passes: u64,
    pub packs: u64,
    pub pack_slices: u64,
    pub exclusive: u64,
    pub bytes: u64,
}

/// One group flush pass, mirroring `ShardRuntime`'s group flush: plan with the
/// state machine's own planner, partition by bucket (the erasure domain), pack
/// when a bucket batch has more than one candidate, otherwise write an
/// exclusive chunk. Returns the published chunk refs.
pub fn flush_pass(
    m: &mut StreamStateMachine,
    min_hot_bytes: usize,
    max_flush_bytes: usize,
    packs: &mut PackPaths,
    stats: &mut FlushStats,
) -> Result<Vec<(BucketStreamId, ColdChunkRef)>> {
    let candidates = m
        .plan_next_cold_flush_batch(min_hot_bytes, max_flush_bytes, max_flush_bytes, 4096)
        .map_err(|err| anyhow::anyhow!("plan flush: {err:?}"))?;
    let mut published = Vec::new();
    if candidates.is_empty() {
        return Ok(published);
    }
    stats.passes += 1;
    let mut batches: Vec<Vec<ColdFlushCandidate>> = Vec::new();
    for candidate in candidates {
        let bucket = candidate.stream_id.bucket_id.clone();
        if let Some(batch) = batches
            .iter_mut()
            .find(|b| b.first().is_some_and(|c| c.stream_id.bucket_id == bucket))
        {
            batch.push(candidate);
        } else {
            batches.push(vec![candidate]);
        }
    }
    for batch in batches {
        if batch.len() > 1 {
            let total: u64 = batch.iter().map(|c| c.payload.len() as u64).sum();
            let bucket = batch
                .first()
                .map(|c| c.stream_id.bucket_id.clone())
                .unwrap_or_default();
            let path = packs.next(&bucket, 0);
            stats.packs += 1;
            let mut object_offset = 0u64;
            for candidate in batch {
                let len = candidate.payload.len() as u64;
                let chunk = ColdChunkRef {
                    start_offset: candidate.start_offset,
                    end_offset: candidate.end_offset,
                    s3_path: path.clone(),
                    object_size: total,
                    object_offset,
                    shared_object: true,
                    payload_digest: candidate.payload_digest,
                };
                ok(
                    m.apply(StreamCommand::FlushCold {
                        cold_generation: None,
                        stream_id: candidate.stream_id.clone(),
                        chunk: chunk.clone(),
                    }),
                    "flush packed",
                )?;
                published.push((candidate.stream_id, chunk));
                object_offset += len;
                stats.pack_slices += 1;
                stats.bytes += len;
            }
        } else {
            for candidate in batch {
                let len = candidate.payload.len() as u64;
                let chunk = ColdChunkRef {
                    start_offset: candidate.start_offset,
                    end_offset: candidate.end_offset,
                    s3_path: ursula_runtime::new_cold_chunk_path(
                        &candidate.stream_id,
                        candidate.start_offset,
                        candidate.end_offset,
                    ),
                    object_size: len,
                    object_offset: 0,
                    shared_object: false,
                    payload_digest: candidate.payload_digest,
                };
                ok(
                    m.apply(StreamCommand::FlushCold {
                        cold_generation: None,
                        stream_id: candidate.stream_id.clone(),
                        chunk: chunk.clone(),
                    }),
                    "flush exclusive",
                )?;
                published.push((candidate.stream_id, chunk));
                stats.exclusive += 1;
                stats.bytes += len;
            }
        }
    }
    Ok(published)
}

/// Publish a tiny checkpoint at `record` and advance retention to it.
pub fn checkpoint_and_retain(
    m: &mut StreamStateMachine,
    id: &BucketStreamId,
    record: u64,
    checkpoint: &[u8],
    now_ms: u64,
) -> Result<()> {
    let offset = m
        .offset_for_record(id, record)
        .map_err(|err| anyhow::anyhow!("record index: {err:?}"))?
        .context("not a JSON stream")?;
    ok(
        m.apply(StreamCommand::PublishSnapshot {
            stream_id: id.clone(),
            snapshot_offset: offset,
            content_type: JSON.to_owned(),
            payload: Bytes::copy_from_slice(checkpoint),
            expected_digest: None,
            now_ms,
        }),
        "publish snapshot",
    )?;
    ok(
        m.apply(StreamCommand::AdvanceRetention {
            stream_id: id.clone(),
            retained_offset: offset,
            now_ms,
        }),
        "advance retention",
    )?;
    Ok(())
}

/// Drain the cold-GC queue the way the GC worker acknowledges it.
pub fn ack_all_cold_gc(m: &mut StreamStateMachine) -> Result<()> {
    if let Some(last) = m.pending_cold_gc_batch(usize::MAX).last() {
        ok(
            m.apply(StreamCommand::AckColdGc {
                up_to_seq: last.seq,
            }),
            "ack cold gc",
        )?;
    }
    Ok(())
}

pub fn append_counts(ids: &[BucketStreamId], counts: &[u64]) -> Vec<StreamAppendCount> {
    ids.iter()
        .zip(counts)
        .map(|(id, count)| StreamAppendCount {
            stream_id: id.clone(),
            append_count: *count,
        })
        .collect()
}
