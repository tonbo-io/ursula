use std::collections::VecDeque;

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
    pub records: Vec<StreamIntegrityRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamIntegrityRecord {
    pub start_offset: u64,
    pub end_offset: u64,
    pub setsum: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct StreamIntegrity {
    live: Setsum,
    evicted: Setsum,
    total: Setsum,
    records: VecDeque<StreamIntegrityRecord>,
    evicted_records: u64,
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
        while self
            .records
            .front()
            .is_some_and(|record| record.end_offset <= retained_offset)
        {
            let record = self.records.pop_front().expect("record exists");
            let setsum = Setsum::from_hexdigest(&record.setsum)
                .expect("integrity records are created from valid setsum hex");
            self.live -= setsum;
            self.evicted += setsum;
            self.evicted_records += 1;
        }
    }

    pub(crate) fn snapshot(
        &self,
        live_start_offset: u64,
        tail_offset: u64,
    ) -> StreamIntegritySnapshot {
        StreamIntegritySnapshot {
            live_setsum: self.live.hexdigest(),
            evicted_setsum: self.evicted.hexdigest(),
            total_setsum: self.total.hexdigest(),
            live_start_offset,
            tail_offset,
            live_records: u64::try_from(self.records.len()).expect("record count fits u64"),
            evicted_records: self.evicted_records,
            total_records: self
                .evicted_records
                .saturating_add(u64::try_from(self.records.len()).expect("record count fits u64")),
            records: self.records.iter().cloned().collect(),
        }
    }

    pub(crate) fn restore(snapshot: StreamIntegritySnapshot) -> Option<Self> {
        let live = Setsum::from_hexdigest(&snapshot.live_setsum)?;
        let evicted = Setsum::from_hexdigest(&snapshot.evicted_setsum)?;
        let total = Setsum::from_hexdigest(&snapshot.total_setsum)?;
        let records = snapshot.records.into_iter().collect::<VecDeque<_>>();
        let mut computed_live = Setsum::default();
        for record in &records {
            if record.end_offset <= record.start_offset {
                return None;
            }
            computed_live += Setsum::from_hexdigest(&record.setsum)?;
        }
        if computed_live != live || live + evicted != total {
            return None;
        }
        let live_records = u64::try_from(records.len()).ok()?;
        if snapshot.live_records != live_records {
            return None;
        }
        if snapshot.total_records != snapshot.evicted_records.saturating_add(live_records) {
            return None;
        }
        Some(Self {
            live,
            evicted,
            total,
            records,
            evicted_records: snapshot.evicted_records,
        })
    }

    fn append_record(&mut self, start_offset: u64, end_offset: u64, record: Setsum) {
        if start_offset == end_offset {
            return;
        }
        self.live += record;
        self.total += record;
        self.records.push_back(StreamIntegrityRecord {
            start_offset,
            end_offset,
            setsum: record.hexdigest(),
        });
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
