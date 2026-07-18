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
