//! F1 sparse cold record marks (bounded-stream-state §5.2, feature level 2)
//! through the in-memory engine with a real cold store and cold-index pages:
//! sealing at `FlushCold` and `TidyStream`, bracketed record reads (RC-6),
//! offset reads unchanged (RC-5), continuation anchors (RC-20), corruption
//! detection (RC-21) and acknowledgements from apply (RC-10).

use std::sync::Arc;

use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;
use ursula_stream::FEATURE_LEVEL_KEYED_STREAMS;
use ursula_stream::FEATURE_LEVEL_SPARSE_MARKS;
use ursula_stream::MARK_BLOCK_BYTES;
use ursula_stream::StreamRecordRange;

use crate::AppendRequest;
use crate::ColdStore;
use crate::CreateStreamRequest;
use crate::InMemoryGroupEngineFactory;
use crate::PlanColdFlushRequest;
use crate::ReadStreamRequest;
use crate::ReadStreamResponse;
use crate::RecordAnchor;
use crate::RuntimeConfig;
use crate::RuntimeError;
use crate::ShardRuntime;
use crate::TidyStreamsRequest;
use crate::cold_store::cold_chunk_dir;

const JSON: &str = "application/json";

fn spawn(cold_store: Arc<ColdStore>) -> ShardRuntime {
    ShardRuntime::spawn_with_engine_factory_and_cold_store(
        RuntimeConfig::new(1, 1),
        InMemoryGroupEngineFactory::with_cold_store(Some(cold_store.clone())),
        Some(cold_store),
    )
    .expect("spawn runtime")
}

async fn raise(runtime: &ShardRuntime, level: u32) {
    for (group, result) in runtime.set_feature_level_all_groups(level).await {
        result.unwrap_or_else(|err| panic!("raise group {group:?}: {err}"));
    }
}

/// The stream's canonical bytes and record starts, kept by the test.
#[derive(Default)]
struct Model {
    bytes: Vec<u8>,
    starts: Vec<u64>,
}

impl Model {
    fn offset(&self, record: u64) -> u64 {
        self.starts
            .get(usize::try_from(record).unwrap())
            .copied()
            .unwrap_or(self.bytes.len() as u64)
    }

    fn slice(&self, from: u64, to: u64) -> &[u8] {
        &self.bytes[usize::try_from(from).unwrap()..usize::try_from(to).unwrap()]
    }
}

/// An NDJSON body of records of the given sizes (LF included, at least 4).
fn records(model: &mut Model, sizes: &[u64]) -> Vec<u8> {
    let mut body = Vec::new();
    for size in sizes {
        model
            .starts
            .push(model.bytes.len() as u64 + body.len() as u64);
        body.push(b'"');
        body.extend(std::iter::repeat_n(
            b'r',
            usize::try_from(size - 3).unwrap(),
        ));
        body.extend_from_slice(b"\"\n");
    }
    model.bytes.extend_from_slice(&body);
    body
}

struct Fixture {
    cold_store: Arc<ColdStore>,
    runtime: ShardRuntime,
    stream: BucketStreamId,
    model: Model,
}

impl Fixture {
    async fn new(level: u32) -> Self {
        let cold_store = Arc::new(ColdStore::memory().expect("memory cold store"));
        let runtime = spawn(cold_store.clone());
        raise(&runtime, level).await;
        let stream = BucketStreamId::new("marks", "log");
        runtime
            .create_stream(CreateStreamRequest::new(stream.clone(), JSON))
            .await
            .expect("create stream");
        Self {
            cold_store,
            runtime,
            stream,
            model: Model::default(),
        }
    }

    async fn append(&mut self, sizes: &[u64]) -> Option<StreamRecordRange> {
        let body = records(&mut self.model, sizes);
        let mut request = AppendRequest::from_bytes(self.stream.clone(), body);
        request.content_type = JSON.to_owned();
        self.runtime
            .append(request)
            .await
            .expect("append")
            .record_range
    }

    async fn flush_all(&self, max_flush_bytes: usize) {
        while self
            .runtime
            .flush_cold_once(PlanColdFlushRequest {
                stream_id: self.stream.clone(),
                min_hot_bytes: 1,
                max_flush_bytes,
            })
            .await
            .expect("flush cold")
            .is_some()
        {}
    }

    async fn marks_and_dense(&self) -> (u64, u64) {
        let gauges = self
            .runtime
            .state_gauges(RaftGroupId(0))
            .await
            .expect("gauges");
        (gauges.record_marks, gauges.dense_record_entries)
    }

    async fn read(
        &self,
        record: u64,
        max_records: Option<u64>,
        max_len: usize,
        record_anchor: Option<RecordAnchor>,
    ) -> Result<ReadStreamResponse, RuntimeError> {
        self.runtime
            .read_stream(ReadStreamRequest {
                stream_id: self.stream.clone(),
                offset: 0,
                max_len,
                now_ms: 0,
                record: Some(record),
                max_records,
                leader_only: false,
                record_anchor,
                read_index: None,
            })
            .await
    }

    /// RC-6: the response equals the model's answer.
    async fn assert_read(&self, record: u64, max_records: Option<u64>, max_len: usize) {
        let response = self
            .read(record, max_records, max_len, None)
            .await
            .expect("record read");
        self.assert_matches(&response, record, max_records, max_len);
    }

    fn assert_matches(
        &self,
        response: &ReadStreamResponse,
        record: u64,
        max_records: Option<u64>,
        max_len: usize,
    ) {
        let next = self.model.starts.len() as u64;
        let end = max_records.map_or(next, |k| (record + k).min(next));
        let start = self.model.offset(record);
        let mut taken = record + 1;
        while taken < end && self.model.offset(taken + 1) - start <= max_len as u64 {
            taken += 1;
        }
        let taken = taken.min(end);
        let next_offset = self.model.offset(taken);
        assert_eq!(response.offset, start, "offset of record {record}");
        assert_eq!(response.next_offset, next_offset, "next offset of {record}");
        assert_eq!(
            response.record_range,
            Some(StreamRecordRange {
                first_record: record,
                next_record: taken,
            })
        );
        assert_eq!(response.payload, self.model.slice(start, next_offset));
        assert_eq!(
            response.up_to_date,
            next_offset == self.model.bytes.len() as u64
        );
    }
}

/// Sealing at `FlushCold` keeps one mark per cold MiB and only the unflushed
/// records dense, and every record read, exact or bracketed, returns the
/// same bytes and coordinates as the dense layout.
#[tokio::test]
async fn bracketed_record_reads_match_the_dense_layout() {
    let mut fixture = Fixture::new(FEATURE_LEVEL_SPARSE_MARKS).await;
    let mut sizes = Vec::new();
    for index in 0..3_000_u64 {
        sizes.push(40 + (index * 37) % 1_500);
    }
    sizes.push(3 * MARK_BLOCK_BYTES);
    sizes.extend([64; 50]);
    // RC-10: the acknowledgement range comes from apply.
    assert_eq!(
        fixture.append(&sizes).await,
        Some(StreamRecordRange {
            first_record: 0,
            next_record: 3_051,
        })
    );
    // Cut flushes mid-record; the tail stays hot.
    fixture.flush_all(700_001).await;
    let hot_tail = fixture.append(&[100; 20]).await.expect("range");
    let (marks, dense) = fixture.marks_and_dense().await;
    let cold_mib = fixture
        .model
        .offset(hot_tail.first_record)
        .div_ceil(MARK_BLOCK_BYTES);
    assert!(marks > 1 && marks <= cold_mib + 2, "marks {marks}");
    assert!(dense <= 21, "dense {dense}");

    for (record, max_records, max_len) in [
        (0, Some(1), 1),
        (1, Some(3), usize::MAX),
        (777, Some(40), 5_000),
        (1_500, None, 64 * 1024),
        (2_999, Some(3), usize::MAX),
        (3_000, Some(2), 1),
        (3_001, Some(10), 2_000),
        (3_049, Some(10), usize::MAX),
        (3_060, None, usize::MAX),
        (3_071, None, usize::MAX),
    ] {
        fixture.assert_read(record, max_records, max_len).await;
    }

    // RC-5: offset reads never consult the index.
    let offset_read = fixture
        .runtime
        .read_stream(ReadStreamRequest {
            stream_id: fixture.stream.clone(),
            offset: 12_345,
            max_len: 70_000,
            now_ms: 0,
            record: None,
            max_records: None,
            leader_only: false,
            record_anchor: None,
            read_index: None,
        })
        .await
        .expect("offset read");
    assert_eq!(offset_read.payload, fixture.model.slice(12_345, 82_345));
}

/// RC-20: a continuation anchor is used only when it names the record of
/// this incarnation inside its bracket; a wrong one resolves from the mark,
/// and one whose preceding byte is not LF fails the read.
#[tokio::test]
async fn continuation_anchors_are_validated() {
    let mut fixture = Fixture::new(FEATURE_LEVEL_SPARSE_MARKS).await;
    fixture.append(&[1_000; 3_000]).await;
    fixture.flush_all(8 << 20).await;
    let head = fixture
        .runtime
        .head_stream(crate::HeadStreamRequest {
            stream_id: fixture.stream.clone(),
            now_ms: 0,
            linearizable: true,
            read_index: None,
        })
        .await
        .expect("head");
    let incarnation = head.created_at_ms.expect("incarnation");
    let record = 1_234;
    let exact = RecordAnchor {
        incarnation,
        record,
        offset: fixture.model.offset(record),
    };
    let response = fixture
        .read(record, Some(5), usize::MAX, Some(exact))
        .await
        .expect("anchored read");
    fixture.assert_matches(&response, record, Some(5), usize::MAX);
    // Another incarnation, another record, or an offset outside the bracket
    // fall back to the mark.
    for anchor in [
        RecordAnchor {
            incarnation: incarnation + 1,
            offset: exact.offset + 1,
            ..exact
        },
        RecordAnchor {
            record: record + 1,
            offset: exact.offset + 1,
            ..exact
        },
        RecordAnchor {
            offset: exact.offset + 10 * MARK_BLOCK_BYTES,
            ..exact
        },
    ] {
        let response = fixture
            .read(record, Some(5), usize::MAX, Some(anchor))
            .await
            .expect("fallback read");
        fixture.assert_matches(&response, record, Some(5), usize::MAX);
    }
    // Inside the bracket but not after an LF: the read fails rather than
    // returning shifted records.
    let shifted = RecordAnchor {
        offset: exact.offset + 1,
        ..exact
    };
    assert!(
        fixture
            .read(record, Some(5), usize::MAX, Some(shifted))
            .await
            .is_err()
    );
}

/// RC-21: cold bytes that disagree with the marks fail bracketed reads with
/// a corruption error and a metric, instead of returning shifted records.
#[tokio::test]
async fn corrupt_chunk_bytes_fail_bracketed_reads() {
    let mut fixture = Fixture::new(FEATURE_LEVEL_SPARSE_MARKS).await;
    fixture.append(&[1_000; 3_000]).await;
    fixture.flush_all(8 << 20).await;
    let dir = cold_chunk_dir(&fixture.stream, {
        let head = fixture
            .runtime
            .head_stream(crate::HeadStreamRequest {
                stream_id: fixture.stream.clone(),
                now_ms: 0,
                linearizable: true,
                read_index: None,
            })
            .await
            .expect("head");
        head.created_at_ms.expect("incarnation")
    });
    let names = fixture
        .cold_store
        .list_file_names(&dir)
        .await
        .expect("list chunks");
    assert_eq!(
        names.len(),
        1,
        "one chunk of {} bytes",
        fixture.model.bytes.len()
    );
    // Record 1_049 is the first to start in the second block, so it is
    // marked; the LF before it disappears from the cold bytes.
    let mut corrupted = fixture.model.bytes.clone();
    let lf = usize::try_from(fixture.model.offset(1_049)).unwrap() - 1;
    assert_eq!(corrupted[lf], b'\n');
    corrupted[lf] = b'x';
    let path = format!("{dir}{}", names[0]);
    fixture
        .cold_store
        .write_chunk(&path, &corrupted)
        .await
        .expect("overwrite chunk");
    let before = crate::record_coordinate_corruptions();
    let err = fixture
        .read(1_080, Some(40), usize::MAX, None)
        .await
        .expect_err("corruption is an error");
    assert!(
        err.to_string().contains("record coordinate corruption"),
        "{err}"
    );
    assert!(crate::record_coordinate_corruptions() > before);
}

/// A legacy stream (records written below level 2) seals through
/// `TidyStream` after the raise, in commands of at most
/// `SEAL_BUDGET_RECORDS`, and reads stay exact throughout.
#[tokio::test]
async fn legacy_streams_seal_after_the_raise_through_tidy() {
    let mut fixture = Fixture::new(FEATURE_LEVEL_KEYED_STREAMS).await;
    fixture.append(&[300; 5_000]).await;
    fixture.flush_all(8 << 20).await;
    assert_eq!(fixture.marks_and_dense().await, (0, 5_000));
    raise(&fixture.runtime, FEATURE_LEVEL_SPARSE_MARKS).await;
    let report = fixture
        .runtime
        .tidy_streams(RaftGroupId(0), TidyStreamsRequest {
            max_streams: 16,
            now_ms: 1,
        })
        .await
        .expect("tidy");
    assert_eq!(report.tidied, 1);
    let (marks, dense) = fixture.marks_and_dense().await;
    assert_eq!(dense, 0);
    assert_eq!(marks, (5_000_u64 * 300).div_ceil(MARK_BLOCK_BYTES));
    fixture.assert_read(4_321, Some(7), usize::MAX).await;
    fixture.assert_read(0, None, usize::MAX).await;
}
