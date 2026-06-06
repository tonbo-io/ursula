//! Micro-bench for the span machinery on the request path.
//!
//! Validates the design's central perf claim: with the default fmt subscriber
//! (OTLP off), an `info`-level request-boundary span costs a bounded amount,
//! while a `debug`-level span on a hot path is filtered out and is effectively
//! free — which is why hot-read spans should be `debug` level. Also measures
//! the `Span::current()` capture used to propagate context across mailboxes.
//!
//! Run with: `cargo bench -p ursula-observability`

use std::hint::black_box;

use criterion::Criterion;
use criterion::criterion_group;
use criterion::criterion_main;
use tracing::Span;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

fn install_default_subscriber() {
    // Mirror the server's default deployment: fmt + EnvFilter at `info`, no
    // OpenTelemetry layer. Discard formatted output so we measure span cost,
    // not formatting/IO.
    let _ = tracing_subscriber::registry()
        .with(EnvFilter::new("info"))
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::sink))
        .try_init();
}

fn bench_spans(c: &mut Criterion) {
    install_default_subscriber();

    // Enabled boundary span (what append/read pay per request by default).
    c.bench_function("info_span_enabled", |b| {
        b.iter(|| {
            let span = tracing::info_span!(
                "http.read",
                bucket = black_box("b"),
                stream = black_box("s"),
            );
            let _enter = span.enter();
            black_box(());
        });
    });

    // Filtered hot-path span (debug under an info filter): the recommended
    // level for very hot reads. Should be near-free.
    c.bench_function("debug_span_filtered", |b| {
        b.iter(|| {
            let span = tracing::debug_span!(
                "core.read",
                bucket = black_box("b"),
                stream = black_box("s"),
            );
            let _enter = span.enter();
            black_box(());
        });
    });

    // Cross-mailbox propagation cost: capturing the current span (an Arc bump).
    c.bench_function("span_current_capture", |b| {
        b.iter(|| black_box(Span::current()));
    });
}

criterion_group!(benches, bench_spans);
criterion_main!(benches);
