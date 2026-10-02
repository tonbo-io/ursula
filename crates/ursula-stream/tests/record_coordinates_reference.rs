use std::collections::HashMap;

use serde::Deserialize;
use serde_json::Value;

const VECTORS: &str = include_str!("fixtures/record_coordinates_v1.json");
const HTTP_VECTORS: &str = include_str!("fixtures/record_coordinates_http_v1.json");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AppendAck {
    record_start: u64,
    record_next: u64,
    next_offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecordBoundary {
    ordinal: u64,
    start_offset: u64,
    next_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ModelError {
    InvalidJson,
    EmptyAppendArray,
    RecordGone { first: u64, next: u64 },
    RecordBeyondTail { next: u64 },
    InvalidRetentionBoundary,
}

#[derive(Debug, Default)]
struct ReferenceStream {
    canonical: Vec<u8>,
    records: Vec<RecordBoundary>,
    first_record: u64,
    deduplicated: HashMap<String, AppendAck>,
}

impl ReferenceStream {
    fn append_json(
        &mut self,
        body: &[u8],
        allow_empty_array: bool,
        idempotency_key: Option<&str>,
    ) -> Result<AppendAck, ModelError> {
        if let Some(key) = idempotency_key
            && let Some(ack) = self.deduplicated.get(key)
        {
            return Ok(*ack);
        }

        let messages = normalize_json(body, allow_empty_array)?;
        let record_start = self.next_record();
        let mut encoded = Vec::new();
        let mut relative_boundaries = Vec::with_capacity(messages.len());
        for message in messages {
            let start = encoded.len();
            serde_json::to_writer(&mut encoded, &message).map_err(|_| ModelError::InvalidJson)?;
            encoded.push(b'\n');
            relative_boundaries.push((start, encoded.len()));
        }

        let base_offset = u64::try_from(self.canonical.len()).expect("canonical length fits u64");
        for (index, (start, end)) in relative_boundaries.into_iter().enumerate() {
            let ordinal = record_start + u64::try_from(index).expect("record index fits u64");
            self.records.push(RecordBoundary {
                ordinal,
                start_offset: base_offset + u64::try_from(start).expect("start offset fits u64"),
                next_offset: base_offset + u64::try_from(end).expect("end offset fits u64"),
            });
        }
        self.canonical.extend_from_slice(&encoded);

        let ack = AppendAck {
            record_start,
            record_next: self.next_record(),
            next_offset: self.next_offset(),
        };
        if let Some(key) = idempotency_key {
            self.deduplicated.insert(key.to_owned(), ack);
        }
        Ok(ack)
    }

    fn read_records(
        &self,
        record: u64,
        max_records: usize,
    ) -> Result<(&[u8], AppendAck), ModelError> {
        self.validate_record(record)?;
        let available =
            usize::try_from(self.next_record() - record).expect("record range fits usize");
        let count = available.min(max_records);
        let record_next = record + u64::try_from(count).expect("record count fits u64");
        let start_offset = self.offset_for(record)?;
        let next_offset = self.offset_for(record_next)?;
        let start = usize::try_from(start_offset).expect("start offset fits usize");
        let end = usize::try_from(next_offset).expect("next offset fits usize");
        Ok((&self.canonical[start..end], AppendAck {
            record_start: record,
            record_next,
            next_offset,
        }))
    }

    fn tail_start(&self, count: u64) -> u64 {
        self.next_record()
            .saturating_sub(count)
            .max(self.first_record)
    }

    fn retain_from(&mut self, record: u64) -> Result<(), ModelError> {
        self.validate_record(record)?;
        self.first_record = record;
        Ok(())
    }

    fn offset_for(&self, record: u64) -> Result<u64, ModelError> {
        self.validate_record(record)?;
        if record == self.next_record() {
            return Ok(self.next_offset());
        }
        let index = usize::try_from(record).map_err(|_| ModelError::InvalidRetentionBoundary)?;
        self.records
            .get(index)
            .map(|boundary| boundary.start_offset)
            .ok_or(ModelError::InvalidRetentionBoundary)
    }

    fn validate_record(&self, record: u64) -> Result<(), ModelError> {
        if record < self.first_record {
            return Err(ModelError::RecordGone {
                first: self.first_record,
                next: self.next_record(),
            });
        }
        if record > self.next_record() {
            return Err(ModelError::RecordBeyondTail {
                next: self.next_record(),
            });
        }
        Ok(())
    }

    fn next_record(&self) -> u64 {
        u64::try_from(self.records.len()).expect("record count fits u64")
    }

    fn next_offset(&self) -> u64 {
        u64::try_from(self.canonical.len()).expect("canonical length fits u64")
    }
}

fn normalize_json(body: &[u8], allow_empty_array: bool) -> Result<Vec<Value>, ModelError> {
    let value = serde_json::from_slice(body).map_err(|_| ModelError::InvalidJson)?;
    match value {
        Value::Array(items) if items.is_empty() && !allow_empty_array => {
            Err(ModelError::EmptyAppendArray)
        }
        Value::Array(items) => Ok(items),
        other => Ok(vec![other]),
    }
}

fn extension_active(content_type: &str) -> bool {
    content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim()
        .eq_ignore_ascii_case("application/json")
}

#[derive(Debug, Deserialize)]
struct ConformanceVectors {
    capability_cases: Vec<CapabilityCase>,
    append_cases: Vec<AppendCase>,
    invalid_append_cases: Vec<InvalidAppendCase>,
}

#[derive(Debug, Deserialize)]
struct CapabilityCase {
    content_type: String,
    active: bool,
}

#[derive(Debug, Deserialize)]
struct AppendCase {
    name: String,
    body: String,
    allow_empty_array: bool,
    canonical: String,
    record_count: u64,
}

#[derive(Debug, Deserialize)]
struct InvalidAppendCase {
    name: String,
    body: String,
    allow_empty_array: bool,
}

#[derive(Debug, Deserialize)]
struct HttpConformanceVectors {
    extension_token: String,
    cases: Vec<HttpCase>,
}

#[derive(Debug, Deserialize)]
struct HttpCase {
    name: String,
    content_type: String,
    #[serde(default)]
    setup_bodies: Vec<String>,
    #[serde(default)]
    retain_from: Option<u64>,
    operation: String,
    #[serde(default)]
    body: Option<String>,
    #[serde(default)]
    record: Option<u64>,
    #[serde(default)]
    max_records: Option<usize>,
    #[serde(default)]
    record_match: Option<u64>,
    #[serde(default)]
    record_view: Option<String>,
    #[serde(default)]
    live: Option<String>,
    #[serde(default)]
    offset_present: bool,
    expected: HttpOutcome,
}

#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
struct HttpOutcome {
    status: u16,
    extension: bool,
    #[serde(default)]
    record_first: Option<u64>,
    #[serde(default)]
    record_start: Option<u64>,
    #[serde(default)]
    record_next: Option<u64>,
    #[serde(default)]
    next_offset: Option<u64>,
    #[serde(default)]
    body: Option<String>,
    #[serde(default)]
    sse_control: Option<SseControl>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct SseControl {
    stream_first_record: u64,
    stream_next_record: u64,
    stream_next_offset: u64,
    up_to_date: bool,
}

fn execute_http_case(case: &HttpCase) -> HttpOutcome {
    let active = extension_active(&case.content_type);
    let mut stream = ReferenceStream::default();
    if active {
        for body in &case.setup_bodies {
            stream
                .append_json(body.as_bytes(), false, None)
                .expect("valid HTTP setup append");
        }
        if let Some(record) = case.retain_from {
            stream
                .retain_from(record)
                .expect("valid retention boundary");
        }
    }

    match case.operation.as_str() {
        "head" => HttpOutcome {
            status: 200,
            extension: active,
            record_first: active.then_some(stream.first_record),
            record_next: active.then_some(stream.next_record()),
            next_offset: Some(stream.next_offset()),
            ..HttpOutcome::default()
        },
        "append" => execute_http_append(case, &mut stream, active),
        "read" => execute_http_read(case, &stream, active),
        other => panic!("unsupported HTTP conformance operation: {other}"),
    }
}

fn execute_http_append(case: &HttpCase, stream: &mut ReferenceStream, active: bool) -> HttpOutcome {
    if !active {
        return HttpOutcome {
            status: 204,
            extension: false,
            next_offset: Some(stream.next_offset()),
            ..HttpOutcome::default()
        };
    }
    if let Some(expected) = case.record_match
        && expected != stream.next_record()
    {
        return HttpOutcome {
            status: 412,
            extension: true,
            record_next: Some(stream.next_record()),
            next_offset: Some(stream.next_offset()),
            ..HttpOutcome::default()
        };
    }
    let body = case.body.as_deref().expect("append case body");
    match stream.append_json(body.as_bytes(), false, None) {
        Ok(ack) => HttpOutcome {
            status: 204,
            extension: true,
            record_start: Some(ack.record_start),
            record_next: Some(ack.record_next),
            next_offset: Some(ack.next_offset),
            ..HttpOutcome::default()
        },
        Err(_) => HttpOutcome {
            status: 400,
            extension: true,
            ..HttpOutcome::default()
        },
    }
}

fn execute_http_read(case: &HttpCase, stream: &ReferenceStream, active: bool) -> HttpOutcome {
    if !active || case.record.is_none() {
        return HttpOutcome {
            status: 200,
            extension: active,
            next_offset: Some(stream.next_offset()),
            body: Some(String::from_utf8(stream.canonical.clone()).expect("canonical UTF-8")),
            ..HttpOutcome::default()
        };
    }
    if case.offset_present {
        return HttpOutcome {
            status: 400,
            extension: true,
            ..HttpOutcome::default()
        };
    }

    let record = case.record.expect("record-aware case");
    let max_records = case.max_records.unwrap_or(usize::MAX);
    match stream.read_records(record, max_records) {
        Ok((payload, ack)) => {
            let empty_long_poll = payload.is_empty() && case.live.as_deref() == Some("long-poll");
            let body = if case.record_view.as_deref() == Some("envelope") {
                envelope_payload(payload, ack.record_start)
            } else {
                String::from_utf8(payload.to_vec()).expect("canonical UTF-8")
            };
            HttpOutcome {
                status: if empty_long_poll { 204 } else { 200 },
                extension: true,
                record_first: Some(stream.first_record),
                record_start: Some(ack.record_start),
                record_next: Some(ack.record_next),
                next_offset: Some(ack.next_offset),
                body: Some(body),
                sse_control: (case.live.as_deref() == Some("sse")).then_some(SseControl {
                    stream_first_record: stream.first_record,
                    stream_next_record: ack.record_next,
                    stream_next_offset: ack.next_offset,
                    up_to_date: ack.record_next == stream.next_record(),
                }),
            }
        }
        Err(ModelError::RecordGone { first, next }) => HttpOutcome {
            status: 410,
            extension: true,
            record_first: Some(first),
            record_next: Some(next),
            ..HttpOutcome::default()
        },
        Err(ModelError::RecordBeyondTail { next }) => HttpOutcome {
            status: 400,
            extension: true,
            record_next: Some(next),
            ..HttpOutcome::default()
        },
        Err(other) => panic!("unexpected read model error: {other:?}"),
    }
}

fn envelope_payload(payload: &[u8], record_start: u64) -> String {
    let mut output = Vec::new();
    for (index, line) in payload
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .enumerate()
    {
        let value: Value = serde_json::from_slice(line).expect("canonical record JSON");
        let record = record_start + u64::try_from(index).expect("record index fits u64");
        serde_json::to_writer(
            &mut output,
            &serde_json::json!({"record": record, "value": value}),
        )
        .expect("serialize envelope");
        output.push(b'\n');
    }
    String::from_utf8(output).expect("envelope UTF-8")
}

#[test]
fn http_conformance_vectors_match_the_reference_model() {
    let vectors: HttpConformanceVectors =
        serde_json::from_str(HTTP_VECTORS).expect("valid HTTP vectors");
    assert_eq!(vectors.extension_token, "json-record-coordinates-v1");
    for case in vectors.cases {
        assert_eq!(execute_http_case(&case), case.expected, "{}", case.name);
    }
}

#[test]
fn conformance_vectors_define_activation_and_json_normalization() {
    let vectors: ConformanceVectors = serde_json::from_str(VECTORS).expect("valid vectors");
    for case in vectors.capability_cases {
        assert_eq!(
            extension_active(&case.content_type),
            case.active,
            "{}",
            case.content_type
        );
    }

    for case in vectors.append_cases {
        let mut stream = ReferenceStream::default();
        let ack = stream
            .append_json(case.body.as_bytes(), case.allow_empty_array, None)
            .unwrap_or_else(|err| panic!("{} failed: {err:?}", case.name));
        assert_eq!(stream.canonical, case.canonical.as_bytes(), "{}", case.name);
        assert_eq!(ack.record_start, 0, "{}", case.name);
        assert_eq!(ack.record_next, case.record_count, "{}", case.name);
        assert_eq!(
            ack.next_offset,
            case.canonical.len() as u64,
            "{}",
            case.name
        );
    }

    for case in vectors.invalid_append_cases {
        let mut stream = ReferenceStream::default();
        let before = stream.canonical.clone();
        assert!(
            stream
                .append_json(case.body.as_bytes(), case.allow_empty_array, None)
                .is_err(),
            "{}",
            case.name
        );
        assert_eq!(stream.canonical, before, "{} mutated the stream", case.name);
        assert_eq!(stream.next_record(), 0, "{} assigned an ordinal", case.name);
    }
}

#[test]
fn ordinals_and_offsets_identify_the_same_boundaries() {
    let mut stream = ReferenceStream::default();
    let first = stream
        .append_json(br#"[{"id":0},{"id":1}]"#, false, None)
        .expect("first append");
    let second = stream
        .append_json(br#"{"id":2}"#, false, None)
        .expect("second append");

    assert_eq!(first.record_start, 0);
    assert_eq!(first.record_next, 2);
    assert_eq!(second.record_start, 2);
    assert_eq!(second.record_next, 3);
    for boundary in &stream.records {
        assert_eq!(
            stream.offset_for(boundary.ordinal),
            Ok(boundary.start_offset)
        );
        assert!(boundary.next_offset > boundary.start_offset);
    }
    assert_eq!(
        stream.offset_for(stream.next_record()),
        Ok(stream.next_offset())
    );

    let (payload, ack) = stream.read_records(1, 2).expect("record-aligned read");
    assert_eq!(payload, b"{\"id\":1}\n{\"id\":2}\n");
    assert_eq!(ack.record_start, 1);
    assert_eq!(ack.record_next, 3);
    assert_eq!(ack.next_offset, stream.next_offset());
}

#[test]
fn idempotent_retry_returns_original_range_without_mutation() {
    let mut stream = ReferenceStream::default();
    let original = stream
        .append_json(br#"[{"id":0},{"id":1}]"#, false, Some("producer:0:0"))
        .expect("original append");
    let retry = stream
        .append_json(br#"{"different":true}"#, false, Some("producer:0:0"))
        .expect("deduplicated retry");

    assert_eq!(retry, original);
    assert_eq!(stream.next_record(), 2);
    assert_eq!(stream.canonical, b"{\"id\":0}\n{\"id\":1}\n");
}

#[test]
fn committed_order_controls_ordinals_not_client_event_time() {
    let mut stream = ReferenceStream::default();
    let later_event = stream
        .append_json(
            br#"{"captured_at_ms":120,"id":"submitted-first"}"#,
            false,
            None,
        )
        .expect("first committed append");
    let earlier_event = stream
        .append_json(br#"{"captured_at_ms":100,"id":"backfill"}"#, false, None)
        .expect("second committed append");

    assert_eq!(later_event.record_start, 0);
    assert_eq!(earlier_event.record_start, 1);
    assert_eq!(stream.next_record(), 2);
}

#[test]
fn retention_advances_first_without_renumbering_survivors() {
    let mut stream = ReferenceStream::default();
    stream
        .append_json(br#"[{"id":0},{"id":1},{"id":2}]"#, false, None)
        .expect("append");
    let record_two_offset = stream.offset_for(2).expect("record two offset");

    stream.retain_from(2).expect("retain from record two");
    assert_eq!(stream.first_record, 2);
    assert_eq!(stream.tail_start(100), 2);
    assert!(matches!(
        stream.read_records(1, 1),
        Err(ModelError::RecordGone { first: 2, next: 3 })
    ));
    assert_eq!(stream.offset_for(2), Ok(record_two_offset));
    let (payload, ack) = stream.read_records(2, 1).expect("surviving record read");
    assert_eq!(payload, b"{\"id\":2}\n");
    assert_eq!(ack.record_start, 2);
    assert_eq!(ack.record_next, 3);
}

/// F1 sparse cold record marks (bounded-stream-state §6) against this
/// reference model: a real [`ursula_stream::StreamStateMachine`] at feature
/// level 2, with an in-memory byte store standing in for S3, must agree with
/// [`ReferenceStream`] on every acknowledgement, record range, record read
/// (exact and bracketed), retention and persistence path.
mod sparse_marks_differential {
    use std::collections::BTreeMap;

    use bytes::Bytes;
    use proptest::prelude::*;
    use ursula_shard::BucketStreamId;
    use ursula_stream::ColdChunkRef;
    use ursula_stream::ExternalPayloadRef;
    use ursula_stream::FEATURE_LEVEL_SPARSE_MARKS;
    use ursula_stream::MARK_BLOCK_BYTES;
    use ursula_stream::ProducerRequest;
    use ursula_stream::RecordOffset;
    use ursula_stream::RecordPlanError;
    use ursula_stream::RecordReadRequest;
    use ursula_stream::StreamCommand;
    use ursula_stream::StreamReadPlan;
    use ursula_stream::StreamReadSegment;
    use ursula_stream::StreamRecordRange;
    use ursula_stream::StreamResponse;
    use ursula_stream::StreamSnapshot;
    use ursula_stream::StreamStateMachine;
    use ursula_stream::trim_record_window;

    use super::AppendAck;
    use super::ReferenceStream;

    const JSON: &str = "application/json";

    /// Bytes the state machine moved to "S3", by stream offset.
    #[derive(Default)]
    struct ColdBytes {
        ranges: BTreeMap<u64, Vec<u8>>,
    }

    impl ColdBytes {
        fn put(&mut self, start: u64, bytes: Vec<u8>) {
            self.ranges.insert(start, bytes);
        }

        fn read(&self, start: u64, len: usize) -> Vec<u8> {
            let mut out = Vec::with_capacity(len);
            let mut cursor = start;
            let end = start + len as u64;
            while cursor < end {
                let (range_start, bytes) = self
                    .ranges
                    .range(..=cursor)
                    .next_back()
                    .unwrap_or_else(|| panic!("no cold bytes at {cursor}"));
                let range_end = range_start + bytes.len() as u64;
                assert!(cursor < range_end, "cold gap at {cursor}");
                let take_end = range_end.min(end);
                out.extend_from_slice(
                    &bytes[usize::try_from(cursor - range_start).unwrap()
                        ..usize::try_from(take_end - range_start).unwrap()],
                );
                cursor = take_end;
            }
            out
        }
    }

    pub(super) struct Harness {
        pub(super) machine: StreamStateMachine,
        pub(super) oracle: ReferenceStream,
        cold: ColdBytes,
        stream: BucketStreamId,
        now_ms: u64,
        next_path: u64,
        producer_seq: u64,
        last_producer_ack: Option<(u64, AppendAck)>,
    }

    fn ok(response: StreamResponse, what: &str) -> StreamResponse {
        assert!(
            !matches!(response, StreamResponse::Error { .. }),
            "{what}: {response:?}"
        );
        response
    }

    /// A JSON body of `sizes.len()` values whose canonical records are
    /// exactly `sizes` bytes (LF included; sizes below 4 become numbers).
    pub(super) fn body(sizes: &[u64], seed: u64) -> Vec<u8> {
        let values = sizes
            .iter()
            .enumerate()
            .map(|(index, size)| {
                let size = usize::try_from((*size).max(2)).unwrap();
                if size < 4 {
                    serde_json::Value::from((seed as usize + index) % 10)
                } else {
                    // `"..."` plus LF.
                    serde_json::Value::String("x".repeat(size - 3))
                }
            })
            .collect::<Vec<_>>();
        serde_json::to_vec(&values).unwrap()
    }

    impl Harness {
        pub(super) fn new() -> Self {
            let mut machine = StreamStateMachine::new();
            ok(
                machine.apply(StreamCommand::SetFeatureLevel {
                    level: FEATURE_LEVEL_SPARSE_MARKS,
                }),
                "raise",
            );
            ok(
                machine.apply(StreamCommand::CreateBucket {
                    bucket_id: "rcmarks".to_owned(),
                }),
                "bucket",
            );
            let stream = BucketStreamId::new("rcmarks", "marks");
            ok(
                machine.apply(StreamCommand::CreateStream {
                    stream_id: stream.clone(),
                    content_type: JSON.to_owned(),
                    initial_payload: Bytes::new(),
                    close_after: false,
                    stream_seq: None,
                    producer: None,
                    stream_ttl_seconds: None,
                    stream_expires_at_ms: None,
                    attrs: None,
                    now_ms: 1,
                }),
                "create",
            );
            Self {
                machine,
                oracle: ReferenceStream::default(),
                cold: ColdBytes::default(),
                stream,
                now_ms: 2,
                next_path: 0,
                producer_seq: 0,
                last_producer_ack: None,
            }
        }

        fn tick(&mut self) -> u64 {
            self.now_ms += 1;
            self.now_ms
        }

        fn canonical_tail(&self, before: u64) -> Vec<u8> {
            self.oracle.canonical[usize::try_from(before).unwrap()..].to_vec()
        }

        fn ack_of(response: &StreamResponse) -> (u64, u64, Option<StreamRecordRange>) {
            match response {
                StreamResponse::Appended {
                    offset,
                    next_offset,
                    record_range,
                    ..
                } => (*offset, *next_offset, *record_range),
                other => panic!("not an append: {other:?}"),
            }
        }

        fn assert_ack(response: &StreamResponse, oracle: AppendAck) {
            let (_, next_offset, range) = Self::ack_of(response);
            assert_eq!(next_offset, oracle.next_offset);
            assert_eq!(
                range,
                Some(StreamRecordRange {
                    first_record: oracle.record_start,
                    next_record: oracle.record_next,
                }),
                "RC-10: acknowledgement range comes from apply"
            );
        }

        /// RC-10, RC-11: inline append, optionally as a producer, and a
        /// retry of the previous producer sequence.
        pub(super) fn append_inline(&mut self, sizes: &[u64], producer: bool, retry: bool) {
            let now_ms = self.tick();
            if retry && self.last_producer_ack.is_none() {
                return;
            }
            if retry && let Some((seq, original)) = self.last_producer_ack {
                let response = ok(
                    self.machine.apply(StreamCommand::Append {
                        stream_id: self.stream.clone(),
                        content_type: Some(JSON.to_owned()),
                        payload: Bytes::from_static(b"{\"retry\":1}\n"),
                        close_after: false,
                        stream_seq: None,
                        producer: Some(ProducerRequest {
                            producer_id: "p".to_owned(),
                            producer_epoch: 0,
                            producer_seq: seq,
                        }),
                        now_ms,
                        record_match: None,
                    }),
                    "retry",
                );
                // RC-11: the stored receipt's original ranges, after any
                // sealing or retention.
                assert!(matches!(response, StreamResponse::Appended {
                    deduplicated: true,
                    ..
                }));
                Self::assert_ack(&response, original);
                return;
            }
            let before = self.oracle.next_offset();
            let oracle = self
                .oracle
                .append_json(&body(sizes, before), false, None)
                .unwrap();
            let payload = self.canonical_tail(before);
            let producer_request = producer.then(|| {
                let seq = self.producer_seq;
                self.producer_seq += 1;
                ProducerRequest {
                    producer_id: "p".to_owned(),
                    producer_epoch: 0,
                    producer_seq: seq,
                }
            });
            let response = ok(
                self.machine.apply(StreamCommand::Append {
                    stream_id: self.stream.clone(),
                    content_type: Some(JSON.to_owned()),
                    payload: payload.into(),
                    close_after: false,
                    stream_seq: None,
                    producer: producer_request.clone(),
                    now_ms,
                    record_match: Some(oracle.record_start),
                }),
                "append",
            );
            Self::assert_ack(&response, oracle);
            if let Some(producer) = producer_request {
                self.last_producer_ack = Some((producer.producer_seq, oracle));
            }
        }

        /// RC-10 for append batches: one range per frame.
        pub(super) fn append_batch(&mut self, frames: &[Vec<u64>]) {
            let now_ms = self.tick();
            let mut payloads = Vec::new();
            let mut acks = Vec::new();
            for frame in frames {
                let before = self.oracle.next_offset();
                acks.push(
                    self.oracle
                        .append_json(&body(frame, before), false, None)
                        .unwrap(),
                );
                payloads.push(self.canonical_tail(before));
            }
            let refs = payloads.iter().map(Vec::as_slice).collect::<Vec<_>>();
            let batch = self
                .machine
                .append_batch_borrowed(self.stream.clone(), Some(JSON), &refs, None, now_ms)
                .expect("append batch");
            assert_eq!(batch.items.len(), acks.len());
            for (item, ack) in batch.items.iter().zip(acks) {
                assert_eq!(item.next_offset, ack.next_offset);
                assert_eq!(
                    item.record_range,
                    Some(StreamRecordRange {
                        first_record: ack.record_start,
                        next_record: ack.record_next,
                    })
                );
            }
        }

        /// RC-10 for external appends, which seal their own records when no
        /// hot bytes lie below them.
        pub(super) fn append_external(&mut self, sizes: &[u64]) {
            let now_ms = self.tick();
            let before = self.oracle.next_offset();
            let oracle = self
                .oracle
                .append_json(&body(sizes, before), false, None)
                .unwrap();
            let payload = self.canonical_tail(before);
            let record_ends = payload
                .iter()
                .enumerate()
                .filter(|(_, byte)| **byte == b'\n')
                .map(|(index, _)| index as u64 + 1)
                .collect::<Vec<_>>();
            let len = payload.len() as u64;
            self.next_path += 1;
            self.cold.put(before, payload);
            let response = ok(
                self.machine.apply(StreamCommand::AppendExternal {
                    stream_id: self.stream.clone(),
                    content_type: Some(JSON.to_owned()),
                    payload: ExternalPayloadRef {
                        s3_path: format!("external/{}", self.next_path),
                        payload_len: len,
                        object_size: len,
                    },
                    record_ends,
                    close_after: false,
                    stream_seq: None,
                    producer: None,
                    now_ms,
                    record_match: None,
                }),
                "external append",
            );
            Self::assert_ack(&response, oracle);
        }

        /// Flushes at most `max_bytes` of the hot prefix; byte cuts split
        /// records.
        pub(super) fn flush(&mut self, max_bytes: usize) {
            let Some(candidate) = self
                .machine
                .plan_cold_flush(&self.stream, 1, max_bytes.max(1))
                .expect("plan flush")
            else {
                return;
            };
            self.next_path += 1;
            let len = candidate.payload.len() as u64;
            self.cold
                .put(candidate.start_offset, candidate.payload.clone());
            ok(
                self.machine.apply(StreamCommand::FlushCold {
                    stream_id: self.stream.clone(),
                    chunk: ColdChunkRef {
                        start_offset: candidate.start_offset,
                        end_offset: candidate.end_offset,
                        s3_path: format!("chunk/{}", self.next_path),
                        object_size: len,
                        object_offset: 0,
                        shared_object: false,
                        payload_digest: String::new(),
                    },
                    cold_generation: Some(candidate.cold_generation),
                }),
                "flush",
            );
        }

        pub(super) fn tidy(&mut self) {
            let now_ms = self.tick();
            ok(
                self.machine.apply(StreamCommand::TidyStream {
                    stream_id: self.stream.clone(),
                    now_ms,
                }),
                "tidy",
            );
        }

        /// RC-12, RC-13: publish a checkpoint at record `target` and retain
        /// to it. Retention lands on the target when it is dense or a mark,
        /// otherwise on the mark at or below it; ordinals never change.
        pub(super) fn retain(&mut self, fraction: u64) {
            let first = self.oracle.first_record;
            let next = self.oracle.next_record();
            if next == first {
                return;
            }
            let target = first + (next - first) * fraction / 1_000;
            let offset = self.oracle.offset_for(target).unwrap();
            let now_ms = self.tick();
            let published = self
                .machine
                .latest_snapshot(&self.stream)
                .unwrap()
                .map_or(0, |snapshot| snapshot.offset);
            if offset >= published {
                ok(
                    self.machine.apply(StreamCommand::PublishSnapshot {
                        stream_id: self.stream.clone(),
                        snapshot_offset: offset,
                        content_type: JSON.to_owned(),
                        payload: Bytes::from_static(b"{}"),
                        expected_digest: None,
                        now_ms,
                    }),
                    "publish snapshot",
                );
            }
            let response = ok(
                self.machine.apply(StreamCommand::AdvanceRetention {
                    stream_id: self.stream.clone(),
                    retained_offset: offset,
                    now_ms,
                }),
                "retention",
            );
            let StreamResponse::RetentionAdvanced {
                retained_offset,
                record_range: Some(range),
            } = response
            else {
                panic!("retention response {response:?}");
            };
            assert!(retained_offset <= offset);
            assert!(range.first_record <= target && range.first_record >= first);
            assert_eq!(
                self.oracle.offset_for(range.first_record),
                Ok(retained_offset),
                "RC-12: the effective retained offset is a boundary with the oracle's ordinal"
            );
            if range.first_record < target {
                // Landed on a mark below a sealed target.
                assert!(offset - retained_offset < MARK_BLOCK_BYTES * 4);
            }
            self.oracle.retain_from(range.first_record).unwrap();
            // Retention seals (level 2): dropping the hot bytes below dense
            // records leaves no seal debt for the tidy driver.
            self.assert_dense_bound(&self.machine);
        }

        /// The dense part holds the unflushed records plus at most the one
        /// straddling the seal point.
        fn assert_dense_bound(&self, machine: &StreamStateMachine) {
            let seal_point = machine.hot_start_offset(&self.stream);
            let unflushed = self
                .oracle
                .records
                .iter()
                .filter(|boundary| boundary.start_offset >= seal_point)
                .count() as u64;
            let dense = machine.state_gauges().dense_record_entries;
            assert!(
                dense <= unflushed + 1,
                "dense {dense} unflushed {unflushed}"
            );
        }

        /// RC-18: a failed transaction rolls back only dense records.
        pub(super) fn failed_transaction(&mut self, sizes: &[u64]) {
            let now_ms = self.tick();
            let before = self.machine.record_range(&self.stream).unwrap();
            let commands = vec![
                StreamCommand::Append {
                    stream_id: self.stream.clone(),
                    content_type: Some(JSON.to_owned()),
                    payload: {
                        let mut scratch = ReferenceStream::default();
                        scratch.append_json(&body(sizes, 0), false, None).unwrap();
                        scratch.canonical.into()
                    },
                    close_after: false,
                    stream_seq: None,
                    producer: None,
                    now_ms,
                    record_match: None,
                },
                StreamCommand::Append {
                    stream_id: self.stream.clone(),
                    content_type: Some("text/plain".to_owned()),
                    payload: Bytes::from_static(b"x"),
                    close_after: false,
                    stream_seq: None,
                    producer: None,
                    now_ms,
                    record_match: None,
                },
            ];
            assert!(self.machine.append_transaction(commands).is_err());
            assert_eq!(self.machine.record_range(&self.stream).unwrap(), before);
        }

        /// RC-16: restore from the group snapshot, and from its serde form
        /// (backup export and `ImportSnapshot`).
        pub(super) fn round_trip(&mut self, via_serde: bool) {
            let snapshot = self.machine.snapshot();
            let snapshot = if via_serde {
                let json = serde_json::to_vec(&snapshot).unwrap();
                serde_json::from_slice::<StreamSnapshot>(&json).unwrap()
            } else {
                snapshot
            };
            let restored = StreamStateMachine::restore(snapshot).expect("restore");
            assert_eq!(restored.snapshot(), self.machine.snapshot());
            self.machine = restored;
        }

        fn materialize(&self, plan: &StreamReadPlan) -> Vec<u8> {
            let mut out = Vec::new();
            for segment in &plan.segments {
                match segment {
                    StreamReadSegment::Hot(bytes) => out.extend_from_slice(bytes),
                    StreamReadSegment::ColdIndex(segment) => {
                        out.extend(self.cold.read(segment.read_start_offset, segment.len));
                    }
                    StreamReadSegment::Object(segment) => {
                        out.extend(self.cold.read(segment.read_start_offset, segment.len));
                    }
                }
            }
            out
        }

        /// RC-6: `?record=r&max_records=k&max_bytes=b`.
        pub(super) fn read_records(
            &self,
            record: u64,
            max_records: Option<u64>,
            max_bytes: usize,
        ) -> Result<(Vec<u8>, u64, u64, StreamRecordRange, bool), RecordPlanError> {
            let plan = self.machine.record_read_plan(&RecordReadRequest {
                stream_id: &self.stream,
                record,
                max_records,
                max_bytes,
                now_ms: self.now_ms,
                anchor: None,
            })?;
            let window = self.materialize(&plan);
            assert_eq!(window.len() as u64, plan.next_offset - plan.offset);
            Ok(match &plan.record_trim {
                Some(trim) => {
                    let trimmed = trim_record_window(&window, plan.offset, trim).unwrap();
                    (
                        window[trimmed.start..trimmed.end].to_vec(),
                        trimmed.offset,
                        trimmed.next_offset,
                        trimmed.record_range,
                        trimmed.up_to_date,
                    )
                }
                None => (
                    window,
                    plan.offset,
                    plan.next_offset,
                    plan.record_range.unwrap(),
                    plan.up_to_date,
                ),
            })
        }

        /// RC-19: a plan computed before a flush, seal and retention still
        /// materializes the records it was planned for.
        pub(super) fn race(&mut self, fraction: u64, flush: usize, retain: u64) {
            let first = self.oracle.first_record;
            let count = self.oracle.next_record() - first;
            if count == 0 {
                return;
            }
            let record = first + count * fraction / 1_000;
            let expected = self.oracle_read(record, Some(5), usize::MAX);
            let plan = self
                .machine
                .record_read_plan(&RecordReadRequest {
                    stream_id: &self.stream,
                    record,
                    max_records: Some(5),
                    max_bytes: usize::MAX,
                    now_ms: self.now_ms,
                    anchor: None,
                })
                .expect("plan");
            self.flush(flush);
            self.tidy();
            self.retain(retain);
            let window = self.materialize(&plan);
            let actual = match &plan.record_trim {
                Some(trim) => {
                    let trimmed = trim_record_window(&window, plan.offset, trim).unwrap();
                    (
                        window[trimmed.start..trimmed.end].to_vec(),
                        trimmed.offset,
                        trimmed.next_offset,
                        trimmed.record_range,
                    )
                }
                None => (
                    window,
                    plan.offset,
                    plan.next_offset,
                    plan.record_range.unwrap(),
                ),
            };
            assert_eq!(actual, (expected.0, expected.1, expected.2, expected.3));
        }

        /// The oracle's answer to the same read.
        fn oracle_read(
            &self,
            record: u64,
            max_records: Option<u64>,
            max_bytes: usize,
        ) -> (Vec<u8>, u64, u64, StreamRecordRange, bool) {
            let next = self.oracle.next_record();
            let end = max_records.map_or(next, |k| (record + k).min(next));
            let start_offset = self.oracle.offset_for(record).unwrap();
            let mut taken = record;
            while taken < end {
                let candidate_end = self.oracle.offset_for(taken + 1).unwrap();
                if taken > record && candidate_end - start_offset > max_bytes as u64 {
                    break;
                }
                taken += 1;
            }
            let next_offset = self.oracle.offset_for(taken).unwrap();
            (
                self.oracle.canonical
                    [usize::try_from(start_offset).unwrap()..usize::try_from(next_offset).unwrap()]
                    .to_vec(),
                start_offset,
                next_offset,
                StreamRecordRange {
                    first_record: record,
                    next_record: taken,
                },
                next_offset == self.oracle.next_offset(),
            )
        }

        /// RC-2 and RC-6 over every retained record (sampled when there are
        /// many), plus invariants M1-M5, bounded marks and dense entries.
        pub(super) fn check(&self, reads: &[(u64, Option<u64>, usize)]) {
            let range = self.machine.record_range(&self.stream).unwrap().unwrap();
            assert_eq!(range.first_record, self.oracle.first_record);
            assert_eq!(range.next_record, self.oracle.next_record());
            let tail = self.oracle.next_offset();
            let count = range.next_record - range.first_record;
            let stride = (count / 200).max(1);
            let mut record = range.first_record;
            while record <= range.next_record {
                let located = self
                    .machine
                    .locate_record(&self.stream, record)
                    .unwrap()
                    .unwrap();
                let oracle = self.oracle.offset_for(record).unwrap();
                match located {
                    RecordOffset::Exact(offset) => assert_eq!(offset, oracle, "record {record}"),
                    RecordOffset::Bracket(bracket) => {
                        // RC-3: one block.
                        assert!(bracket.from_offset < oracle && oracle < bracket.limit);
                        assert!(bracket.limit - bracket.from_offset <= MARK_BLOCK_BYTES);
                    }
                }
                record += stride;
            }
            for (fraction, max_records, max_bytes) in reads {
                if count == 0 {
                    break;
                }
                let record = range.first_record + count * fraction / 1_000;
                assert_eq!(
                    self.read_records(record, *max_records, *max_bytes)
                        .expect("record read"),
                    self.oracle_read(record, *max_records, *max_bytes),
                    "RC-6 read of record {record} k={max_records:?} b={max_bytes}"
                );
            }
            // Invariant 9 (F1 terms): marks <= ceil(cold MiB) + 2, and once
            // the tidy driver has paid any seal debt (a budget cut can leave
            // some) the dense part holds the unflushed records plus at most
            // the one straddling the seal point.
            let mut tidied = self.machine.clone();
            ok(
                tidied.apply(StreamCommand::TidyStream {
                    stream_id: self.stream.clone(),
                    now_ms: self.now_ms,
                }),
                "tidy probe",
            );
            let gauges = tidied.state_gauges();
            let seal_point = self.machine.hot_start_offset(&self.stream);
            let retained = self.oracle.offset_for(self.oracle.first_record).unwrap();
            let cold_mib = seal_point
                .saturating_sub(retained)
                .div_ceil(MARK_BLOCK_BYTES);
            assert!(
                gauges.record_marks <= cold_mib + 2,
                "marks {} for {cold_mib} cold MiB",
                gauges.record_marks
            );
            self.assert_dense_bound(&tidied);
            let _ = tail;
        }
    }

    #[derive(Debug, Clone)]
    enum Op {
        Inline { sizes: Vec<u64>, producer: bool },
        Retry,
        Batch(Vec<Vec<u64>>),
        External(Vec<u64>),
        Flush(usize),
        Tidy,
        Retain(u64),
        FailedTransaction(Vec<u64>),
        RoundTrip(bool),
        Race(u64, usize, u64),
    }

    fn size() -> impl Strategy<Value = u64> {
        prop_oneof![
            12 => 2_u64..200,
            3 => 200_u64..60_000,
        ]
    }

    /// Mostly small records, sometimes one of 0.5 to 3 MiB (records larger
    /// than a block).
    fn sizes() -> impl Strategy<Value = Vec<u64>> {
        prop_oneof![
            8 => prop::collection::vec(size(), 1..40),
            1 => ((MARK_BLOCK_BYTES / 2)..(3 * MARK_BLOCK_BYTES)).prop_map(|large| vec![large]),
        ]
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            6 => (sizes(), any::<bool>()).prop_map(|(sizes, producer)| Op::Inline { sizes, producer }),
            1 => Just(Op::Retry),
            2 => prop::collection::vec(sizes(), 1..4).prop_map(Op::Batch),
            2 => sizes().prop_map(Op::External),
            5 => prop_oneof![1_usize..64, 64_usize..4_096, 4_096_usize..(4 << 20)].prop_map(Op::Flush),
            1 => Just(Op::Tidy),
            2 => (0_u64..1_000).prop_map(Op::Retain),
            1 => sizes().prop_map(Op::FailedTransaction),
            1 => any::<bool>().prop_map(Op::RoundTrip),
            1 => (0_u64..1_000, 1_usize..(2 << 20), 0_u64..1_000)
                .prop_map(|(fraction, flush, retain)| Op::Race(fraction, flush, retain)),
        ]
    }

    fn read_spec() -> impl Strategy<Value = (u64, Option<u64>, usize)> {
        (
            0_u64..1_000,
            prop_oneof![Just(None), (1_u64..50).prop_map(Some)],
            prop_oneof![Just(usize::MAX), 1_usize..4_096, 4_096_usize..(3 << 20)],
        )
    }

    fn apply(harness: &mut Harness, op: &Op) {
        match op {
            Op::Inline { sizes, producer } => harness.append_inline(sizes, *producer, false),
            Op::Retry => harness.append_inline(&[], true, true),
            Op::Batch(frames) => harness.append_batch(frames),
            Op::External(sizes) => harness.append_external(sizes),
            Op::Flush(max) => harness.flush(*max),
            Op::Tidy => harness.tidy(),
            Op::Retain(fraction) => harness.retain(*fraction),
            Op::FailedTransaction(sizes) => harness.failed_transaction(sizes),
            Op::RoundTrip(serde) => harness.round_trip(*serde),
            Op::Race(fraction, flush, retain) => harness.race(*fraction, *flush, *retain),
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]

        /// RC-2: in every reachable state the sparse index resolves every
        /// record, every acknowledgement and every record read exactly as
        /// the dense reference model does.
        #[test]
        fn state_machine_matches_the_reference_model(
            ops in prop::collection::vec(op(), 1..24),
            reads in prop::collection::vec(read_spec(), 1..6),
        ) {
            let mut harness = Harness::new();
            for op in &ops {
                apply(&mut harness, op);
                harness.check(&reads);
            }
        }
    }

    /// The D1 sequence (hot prefix, external append above it, flush of the
    /// prefix, snapshot round trip) keeps record coordinates exact, and the
    /// external append's records seal once nothing hot lies below them.
    #[test]
    fn hot_prefix_external_append_and_flush_keep_coordinates_exact() {
        let mut harness = Harness::new();
        harness.append_inline(&[100; 50], false, false);
        harness.append_external(&[MARK_BLOCK_BYTES / 3; 6]);
        let gauges = harness.machine.state_gauges();
        assert_eq!(gauges.record_marks, 0, "hot bytes below keep it dense");
        harness.check(&[(0, None, usize::MAX), (500, Some(3), 1)]);
        harness.flush(5_000);
        harness.flush(1 << 20);
        let gauges = harness.machine.state_gauges();
        assert!(gauges.record_marks >= 2);
        assert_eq!(gauges.dense_record_entries, 0);
        harness.round_trip(false);
        harness.round_trip(true);
        harness.check(&[(0, None, usize::MAX), (500, Some(3), 1), (999, Some(1), 1)]);
    }

    /// Regression (F1 follow-up): retention that drops the hot bytes below
    /// an external append's dense records seals them in the same command,
    /// so no seal debt is left for the tidy driver.
    #[test]
    fn retention_below_dense_external_records_leaves_no_seal_debt() {
        let mut harness = Harness::new();
        harness.append_inline(&[100; 10], false, false);
        harness.append_external(&[MARK_BLOCK_BYTES / 3; 6]);
        assert_eq!(harness.machine.state_gauges().record_marks, 0);
        // Retain to record 10 of 16, the external's first: past every hot
        // byte, so nothing hot remains below the external's records.
        harness.retain(625);
        assert_eq!(harness.machine.hot_start_offset(&harness.stream), harness.oracle.next_offset());
        let gauges = harness.machine.state_gauges();
        assert_eq!(
            gauges.dense_record_entries, 0,
            "the external's records sealed at retention"
        );
        assert!(gauges.record_marks >= 1);
        harness.check(&[(0, None, usize::MAX), (500, Some(3), 1), (999, Some(1), 1)]);
    }

    /// Mid-record seal points, records larger than a block, budget cuts and
    /// retention onto marks, explicitly.
    #[test]
    fn explicit_block_edges_and_large_records() {
        let mut harness = Harness::new();
        harness.append_inline(&[MARK_BLOCK_BYTES / 4 - 7; 9], true, false);
        harness.append_inline(&[3 * MARK_BLOCK_BYTES, 10, 10], false, false);
        harness.append_inline(&[64; 300], false, false);
        harness.flush(MARK_BLOCK_BYTES as usize + 13);
        harness.flush(2 * MARK_BLOCK_BYTES as usize);
        harness.flush(4 * MARK_BLOCK_BYTES as usize);
        harness.flush(4 * MARK_BLOCK_BYTES as usize);
        harness.check(&[
            (0, Some(1), 1),
            (300, Some(4), MARK_BLOCK_BYTES as usize),
            (31, None, usize::MAX),
            (40, Some(2), 1),
        ]);
        harness.append_inline(&[], true, true);
        harness.retain(30);
        harness.check(&[(0, Some(2), 1), (900, None, 100)]);
        harness.retain(600);
        harness.round_trip(false);
        harness.check(&[(0, Some(2), 1), (500, None, 100)]);
        harness.append_inline(&[], true, true);
    }

    /// RC-21: a stale byte under a sealed block fails the read instead of
    /// returning shifted records.
    #[test]
    fn corrupt_cold_bytes_fail_bracketed_reads() {
        let mut harness = Harness::new();
        harness.append_inline(&[MARK_BLOCK_BYTES / 8; 24], false, false);
        harness.flush(8 * MARK_BLOCK_BYTES as usize);
        // Record 9 starts after the LF that ends record 8, inside mark 8's
        // block; turning that LF into 'x' must fail reads that cross it.
        let lf = harness.oracle.offset_for(9).unwrap() - 1;
        let (start, bytes) = harness
            .cold
            .ranges
            .iter_mut()
            .find(|(start, bytes)| **start <= lf && lf < **start + bytes.len() as u64)
            .unwrap();
        bytes[usize::try_from(lf - *start).unwrap()] = b'x';
        let plan = harness
            .machine
            .record_read_plan(&RecordReadRequest {
                stream_id: &harness.stream,
                record: 10,
                max_records: Some(6),
                max_bytes: usize::MAX,
                now_ms: harness.now_ms,
                anchor: None,
            })
            .unwrap();
        let trim = plan.record_trim.clone().expect("bracketed");
        let window = harness.materialize(&plan);
        assert!(trim_record_window(&window, plan.offset, &trim).is_err());
    }
}
