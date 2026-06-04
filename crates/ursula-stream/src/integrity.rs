use serde::{Deserialize, Serialize};
use setsum::Setsum;
use ursula_shard::BucketStreamId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamIntegritySnapshot {
    pub live_setsum: String,
    pub evicted_setsum: String,
    pub total_setsum: String,
    pub live_start_offset: u64,
    pub tail_offset: u64,
    pub live_records: u64,
    pub evicted_records: u64,
    pub total_records: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct StreamIntegrity {
    live: Setsum,
    total: Setsum,
    total_records: u64,
}

impl StreamIntegrity {
    pub(crate) fn append_payload(
        &mut self,
        stream_id: &BucketStreamId,
        start_offset: u64,
        end_offset: u64,
        payload: &[u8],
    ) {
        let record = record_setsum(stream_id, start_offset, end_offset, b"inline", &[payload]);
        self.append_record(start_offset, end_offset, record);
    }

    pub(crate) fn append_external(
        &mut self,
        stream_id: &BucketStreamId,
        start_offset: u64,
        end_offset: u64,
        s3_path: &str,
        object_size: u64,
    ) {
        let object_size = object_size.to_le_bytes();
        let record = record_setsum(
            stream_id,
            start_offset,
            end_offset,
            b"external",
            &[s3_path.as_bytes(), &object_size],
        );
        self.append_record(start_offset, end_offset, record);
    }

    pub(crate) fn evict_before(&mut self, retained_offset: u64) {
        let _ = retained_offset;
    }

    pub(crate) fn snapshot(
        &self,
        live_start_offset: u64,
        tail_offset: u64,
    ) -> StreamIntegritySnapshot {
        StreamIntegritySnapshot {
            live_setsum: self.live.hexdigest(),
            evicted_setsum: Setsum::default().hexdigest(),
            total_setsum: self.total.hexdigest(),
            live_start_offset,
            tail_offset,
            live_records: self.total_records,
            evicted_records: 0,
            total_records: self.total_records,
        }
    }

    pub(crate) fn restore(snapshot: StreamIntegritySnapshot) -> Option<Self> {
        let live = Setsum::from_hexdigest(&snapshot.live_setsum)?;
        let evicted = Setsum::from_hexdigest(&snapshot.evicted_setsum)?;
        let total = Setsum::from_hexdigest(&snapshot.total_setsum)?;
        if live + evicted != total {
            return None;
        }
        Some(Self {
            live: total,
            total,
            total_records: snapshot.total_records,
        })
    }

    fn append_record(&mut self, start_offset: u64, end_offset: u64, record: Setsum) {
        if start_offset == end_offset {
            return;
        }
        self.live += record;
        self.total += record;
        self.total_records = self.total_records.saturating_add(1);
    }
}

fn record_setsum(
    stream_id: &BucketStreamId,
    start_offset: u64,
    end_offset: u64,
    kind: &[u8],
    pieces: &[&[u8]],
) -> Setsum {
    let mut setsum = Setsum::default();
    let start = start_offset.to_le_bytes();
    let end = end_offset.to_le_bytes();
    let mut item = vec![
        b"ursula-stream-record-v1".as_slice(),
        stream_id.bucket_id.as_bytes(),
        b"\0",
        stream_id.stream_id.as_bytes(),
        b"\0",
        &start,
        &end,
        kind,
    ];
    item.extend_from_slice(pieces);
    setsum.insert_vectored(&item);
    setsum
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream_id() -> BucketStreamId {
        BucketStreamId::new("benchcmp", "integrity")
    }

    #[test]
    fn snapshot_omits_per_append_records() {
        let stream_id = stream_id();
        let mut integrity = StreamIntegrity::default();
        integrity.append_payload(&stream_id, 0, 3, b"abc");
        integrity.append_payload(&stream_id, 3, 5, b"de");

        let snapshot = integrity.snapshot(0, 5);

        assert_eq!(snapshot.live_records, 2);
        assert_eq!(snapshot.evicted_records, 0);
        assert_eq!(snapshot.total_records, 2);
        assert_eq!(snapshot.live_setsum, snapshot.total_setsum);
    }

    #[test]
    fn snapshot_wire_format_has_no_records_field() {
        let stream_id = stream_id();
        let mut integrity = StreamIntegrity::default();
        integrity.append_payload(&stream_id, 0, 3, b"abc");

        let encoded =
            serde_json::to_value(integrity.snapshot(0, 3)).expect("encode compacted integrity");

        assert!(encoded.get("records").is_none());
    }
}
