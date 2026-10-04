//! Per-group bounded-state gauges (`docs/architecture/bounded-stream-state.md`
//! §7.5): counts of every replicated structure that can grow with stream
//! history, plus the node-local TTL heap. Computed on demand by walking the
//! group's streams (O(streams + producers)), so metric scrapes pay for it and
//! apply does not.

use serde::Deserialize;
use serde::Serialize;

use super::ProducerAppendRecord;
use super::ProducerReceipt;
use super::ProducerState;
use super::StreamStateMachine;
use super::stream_expiry_at_ms;

/// Snapshot of one group's bounded-state gauges.
///
/// Counts are exact. Byte figures are length-based estimates (element counts
/// times in-memory element sizes, without allocator slack), which is what the
/// §7.2 formula checks compare against.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupStateGauges {
    /// Replicated group feature level (C0 / F0).
    pub feature_level: u32,
    /// Live streams in the group.
    pub streams: u64,
    /// Sparse cold record marks (F1, level 2). Always 0 below level 2.
    pub record_marks: u64,
    /// Dense record-index entries across JSON streams (F1 target: unflushed
    /// records only).
    pub dense_record_entries: u64,
    /// Largest dense record index held by one stream.
    pub max_dense_record_entries_per_stream: u64,
    /// Message records across streams (F4).
    pub message_records: u64,
    /// Largest message-record list held by one stream.
    pub max_message_records_per_stream: u64,
    /// Shared pack-slice references held in stream state (F2).
    pub shared_refs: u64,
    /// Largest shared-reference list held by one stream.
    pub max_shared_refs_per_stream: u64,
    /// Distinct live shared pack objects in the group maps (F2).
    pub live_packs: u64,
    /// External payload locators held in stream state (F5).
    pub staged_external_refs: u64,
    /// Producer ids across streams (F3).
    pub producers: u64,
    /// Largest producer map held by one stream.
    pub max_producers_per_stream: u64,
    /// Producer receipts across streams (F3).
    pub receipts: u64,
    /// Receipt items (one per receipt; legacy receipts may hold more) plus each
    /// producer's `last_items` (F3 window target: 1,024 per stream).
    pub receipt_items: u64,
    /// Largest receipt-item count held by one stream.
    pub max_receipt_items_per_stream: u64,
    /// Length-based producer state bytes across streams (F3 `Prod(s)`).
    pub producer_bytes: u64,
    /// Largest length-based producer state of one stream.
    pub max_producer_bytes_per_stream: u64,
    /// Live streams with a TTL or absolute expiry.
    pub ttl_streams: u64,
    /// Entries in the node-local TTL heap, stale ones included (F8 target:
    /// at most two per TTL stream).
    pub ttl_heap_entries: u64,
    /// Unflushed payload bytes (the group hot gauge).
    pub hot_payload_bytes: u64,
    /// Hot blocks of up to 64 KiB (F6b; one per append before it).
    pub hot_chunks: u64,
    /// Hot-window block headers beyond payload (F6b).
    pub hot_overhead_bytes: u64,
    /// Hot records: message records at or above each stream's first hot
    /// byte (F6c).
    pub hot_records: u64,
    /// Hot payload plus per-record overhead, what admission and the flush
    /// planner count (F6c).
    pub hot_real_bytes: u64,
    /// Pending cold-GC queue entries (F14).
    pub pending_cold_gc: u64,
    /// Per-bucket usage rows (F15, by design O(buckets ever written)).
    pub bucket_usage_rows: u64,
    /// Tenant-erasure fences (F15, by design O(buckets ever purged)).
    pub erased_buckets: u64,
}

fn as_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn producer_state_bytes(producer_id: &str, state: &ProducerState) -> u64 {
    let receipt_items: usize = state
        .receipts
        .iter()
        .map(|receipt| receipt.items.len())
        .sum();
    let bytes = producer_id
        .len()
        .saturating_add(std::mem::size_of::<ProducerState>())
        .saturating_add(
            state
                .receipts
                .len()
                .saturating_mul(std::mem::size_of::<ProducerReceipt>()),
        )
        .saturating_add(
            receipt_items
                .saturating_add(state.last_items.len())
                .saturating_mul(std::mem::size_of::<ProducerAppendRecord>()),
        );
    as_u64(bytes)
}

fn producer_receipt_items(state: &ProducerState) -> u64 {
    let items: usize = state
        .receipts
        .iter()
        .map(|receipt| receipt.items.len().max(1))
        .sum();
    as_u64(items.saturating_add(state.last_items.len()))
}

impl StreamStateMachine {
    /// Bounded-state gauges for this group (§7.5). Walks every stream once.
    pub fn state_gauges(&self) -> GroupStateGauges {
        let mut gauges = GroupStateGauges {
            feature_level: self.feature_level,
            live_packs: as_u64(self.shared_cold_object_refs.len()),
            ttl_heap_entries: as_u64(self.registry.ttl_heap_len()),
            hot_payload_bytes: self.hot_payload_bytes,
            hot_records: self.hot_records,
            hot_real_bytes: self.total_hot_real_bytes(),
            pending_cold_gc: as_u64(self.cold_gc.len()),
            bucket_usage_rows: as_u64(self.bucket_usage.len()),
            erased_buckets: as_u64(self.erased_buckets.len()),
            ..GroupStateGauges::default()
        };
        for slot in self.registry.slots() {
            gauges.streams = gauges.streams.saturating_add(1);
            if stream_expiry_at_ms(&slot.metadata).is_some() {
                gauges.ttl_streams = gauges.ttl_streams.saturating_add(1);
            }
            let dense = slot
                .record_index
                .as_ref()
                .map_or(0, |index| as_u64(index.dense_len()));
            let marks = slot
                .record_index
                .as_ref()
                .map_or(0, |index| as_u64(index.marks().len()));
            gauges.record_marks = gauges.record_marks.saturating_add(marks);
            gauges.dense_record_entries = gauges.dense_record_entries.saturating_add(dense);
            gauges.max_dense_record_entries_per_stream =
                gauges.max_dense_record_entries_per_stream.max(dense);
            let message_records = as_u64(slot.message_records.len());
            gauges.message_records = gauges.message_records.saturating_add(message_records);
            gauges.max_message_records_per_stream =
                gauges.max_message_records_per_stream.max(message_records);
            let shared = as_u64(
                slot.cold
                    .cold_chunks()
                    .iter()
                    .filter(|chunk| chunk.shared_object)
                    .count(),
            );
            gauges.shared_refs = gauges.shared_refs.saturating_add(shared);
            gauges.max_shared_refs_per_stream = gauges.max_shared_refs_per_stream.max(shared);
            gauges.staged_external_refs = gauges
                .staged_external_refs
                .saturating_add(as_u64(slot.cold.external_segments().len()));
            let producers = as_u64(slot.producers.len());
            gauges.producers = gauges.producers.saturating_add(producers);
            gauges.max_producers_per_stream = gauges.max_producers_per_stream.max(producers);
            let mut stream_items = 0u64;
            let mut stream_producer_bytes = 0u64;
            for (producer_id, state) in &slot.producers {
                gauges.receipts = gauges.receipts.saturating_add(as_u64(state.receipts.len()));
                stream_items = stream_items.saturating_add(producer_receipt_items(state));
                stream_producer_bytes =
                    stream_producer_bytes.saturating_add(producer_state_bytes(producer_id, state));
            }
            gauges.receipt_items = gauges.receipt_items.saturating_add(stream_items);
            gauges.max_receipt_items_per_stream =
                gauges.max_receipt_items_per_stream.max(stream_items);
            gauges.producer_bytes = gauges.producer_bytes.saturating_add(stream_producer_bytes);
            gauges.max_producer_bytes_per_stream = gauges
                .max_producer_bytes_per_stream
                .max(stream_producer_bytes);
            gauges.hot_chunks = gauges
                .hot_chunks
                .saturating_add(as_u64(slot.hot_buffer.chunk_count()));
            gauges.hot_overhead_bytes = gauges
                .hot_overhead_bytes
                .saturating_add(as_u64(slot.hot_buffer.chunk_overhead_bytes()));
        }
        gauges
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use ursula_shard::BucketStreamId;

    use super::GroupStateGauges;
    use crate::ColdChunkRef;
    use crate::ProducerRequest;
    use crate::StreamCommand;
    use crate::StreamResponse;
    use crate::StreamStateMachine;

    const T0: u64 = 1_759_300_000_000;

    fn create(m: &mut StreamStateMachine, id: &BucketStreamId, ttl: Option<u64>) {
        let response = m.apply(StreamCommand::CreateStream {
            stream_id: id.clone(),
            content_type: "application/json".to_owned(),
            initial_payload: Bytes::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: ttl,
            stream_expires_at_ms: None,
            now_ms: T0,
        });
        assert!(
            !matches!(response, StreamResponse::Error { .. }),
            "{response:?}"
        );
    }

    fn append(
        m: &mut StreamStateMachine,
        id: &BucketStreamId,
        body: &'static [u8],
        producer: Option<ProducerRequest>,
    ) {
        let response = m.apply(StreamCommand::Append {
            stream_id: id.clone(),
            content_type: Some("application/json".to_owned()),
            payload: Bytes::from_static(body),
            close_after: false,
            stream_seq: None,
            producer,
            now_ms: T0,
            record_match: None,
        });
        assert!(
            !matches!(response, StreamResponse::Error { .. }),
            "{response:?}"
        );
    }

    #[test]
    fn empty_group_reports_zero_gauges() {
        // Every group starts at the top level (format epoch 2).
        assert_eq!(StreamStateMachine::new().state_gauges(), GroupStateGauges {
            feature_level: crate::MAX_SUPPORTED_FEATURE_LEVEL,
            ..GroupStateGauges::default()
        });
    }

    #[test]
    fn gauges_count_records_producers_ttl_and_shared_refs() {
        let mut m = StreamStateMachine::new();
        m.apply(StreamCommand::CreateBucket {
            bucket_id: "bkt1".to_owned(),
        });
        let a = BucketStreamId::new("bkt1", "a");
        let t = BucketStreamId::new("bkt1", "t");
        create(&mut m, &a, None);
        create(&mut m, &t, Some(60));
        append(&mut m, &a, b"{\"x\":1}\n{\"x\":2}\n", None);
        for seq in 0..3 {
            append(
                &mut m,
                &t,
                b"{\"y\":1}\n",
                Some(ProducerRequest {
                    producer_id: "writer".to_owned(),
                    producer_epoch: 1,
                    producer_seq: seq,
                }),
            );
        }
        let gauges = m.state_gauges();
        assert_eq!(gauges.streams, 2);
        assert_eq!(gauges.record_marks, 0);
        assert_eq!(gauges.dense_record_entries, 5);
        assert_eq!(gauges.max_dense_record_entries_per_stream, 3);
        assert_eq!(gauges.producers, 1);
        assert_eq!(gauges.receipts, 3);
        assert!(gauges.receipt_items >= 3);
        assert!(gauges.producer_bytes > 0);
        assert_eq!(gauges.ttl_streams, 1);
        // F8: the heap keeps one armed entry per TTL stream.
        assert_eq!(gauges.ttl_heap_entries, 1);
        // F6b: contiguous appends share one hot block per stream.
        assert_eq!(gauges.hot_chunks, 2);
        assert_eq!(gauges.hot_payload_bytes, 16 + 3 * 8);
        assert!(gauges.hot_overhead_bytes > 0);
        assert_eq!(gauges.shared_refs, 0);

        let candidates = m
            .plan_next_cold_flush_batch(1, 1 << 20, 1 << 20, 16)
            .expect("plan");
        assert_eq!(candidates.len(), 2);
        let total: u64 = candidates.iter().map(|c| c.payload.len() as u64).sum();
        let mut object_offset = 0;
        for candidate in candidates {
            let len = candidate.payload.len() as u64;
            let response = m.apply(StreamCommand::FlushCold {
                cold_generation: None,
                stream_id: candidate.stream_id.clone(),
                chunk: ColdChunkRef {
                    start_offset: candidate.start_offset,
                    end_offset: candidate.end_offset,
                    s3_path: "bkt1/_packs/00000000/pack.bin".to_owned(),
                    object_size: total,
                    object_offset,
                    shared_object: true,
                    payload_digest: candidate.payload_digest,
                },
            });
            assert!(
                !matches!(response, StreamResponse::Error { .. }),
                "{response:?}"
            );
            object_offset += len;
        }
        let gauges = m.state_gauges();
        assert_eq!(gauges.shared_refs, 2);
        assert_eq!(gauges.max_shared_refs_per_stream, 1);
        assert_eq!(gauges.live_packs, 1);
        assert_eq!(gauges.hot_chunks, 0);
        assert_eq!(gauges.hot_payload_bytes, 0);
    }
}
