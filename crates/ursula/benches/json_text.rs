//! Normalization cost of JSON write bodies: the P1 lexical path
//! (`normalize_json_messages`) against the previous `serde_json::Value` round
//! trip, over Pi-like payloads from 127 B to 56 KB. Criterion reports
//! throughput in bytes; ns/byte is the inverse.

use std::hint::black_box;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::criterion_group;
use criterion::criterion_main;
use ursula::json_text::normalize_json_messages;

/// The pre-P1 path: parse into a sorted `Value` tree and re-serialize.
fn legacy_normalize(body: &[u8]) -> Vec<u8> {
    let value: serde_json::Value = serde_json::from_slice(body).expect("valid JSON");
    let messages = match value {
        serde_json::Value::Array(items) => items,
        other => vec![other],
    };
    let mut out = Vec::new();
    for message in messages {
        serde_json::to_writer(&mut out, &message).expect("encode");
        out.push(b'\n');
    }
    out
}

/// A pretty-printed Pi-style message of roughly `target` bytes: a small
/// envelope with nested metadata and a text body containing escapes.
fn payload(target: usize) -> Vec<u8> {
    let filler_len = target.saturating_sub(120).max(1);
    let text: String = "lorem ipsum \\\"quoted\\\" tab\\t é "
        .chars()
        .cycle()
        .take(filler_len)
        .collect();
    let text = text.trim_end_matches('\\');
    format!(
        "{{\n  \"type\": \"message\",\n  \"id\": 12345,\n  \"meta\": {{ \"role\": \"assistant\", \"ts\": 1.5e3 }},\n  \"content\": [ {{ \"text\": \"{text}\" }} ]\n}}"
    )
    .into_bytes()
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_normalize");
    for size in [127usize, 1024, 8 * 1024, 56 * 1024] {
        let body = payload(size);
        group.throughput(Throughput::Bytes(body.len() as u64));
        group.bench_with_input(BenchmarkId::new("p1_lexical", size), &body, |b, body| {
            b.iter(|| normalize_json_messages(black_box(body), false).expect("valid"));
        });
        group.bench_with_input(BenchmarkId::new("legacy_value", size), &body, |b, body| {
            b.iter(|| legacy_normalize(black_box(body)));
        });
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
