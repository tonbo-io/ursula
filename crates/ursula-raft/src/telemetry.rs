//! W3C trace-context propagation across the Raft gRPC transport.
//!
//! A follower that forwards a client write or read to the leader does so within
//! the originating request span. Injecting the span's context into the gRPC
//! request metadata (and extracting it on the server) lets a single trace span
//! the producer, the follower, and the leader.
//!
//! Injection and extraction go through the process-wide [`TextMapPropagator`].
//! When no OpenTelemetry layer/propagator is installed (the default), the
//! current context is empty and the propagator is a no-op, so no metadata is
//! added and nothing is paid.
//!
//! [`TextMapPropagator`]: opentelemetry::propagation::TextMapPropagator

use opentelemetry::Context;
use opentelemetry::global;
use opentelemetry::propagation::Extractor;
use opentelemetry::propagation::Injector;
use tonic::metadata::KeyRef;
use tonic::metadata::MetadataKey;
use tonic::metadata::MetadataMap;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

struct MetadataInjector<'a>(&'a mut MetadataMap);

impl Injector for MetadataInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let Ok(name) = MetadataKey::from_bytes(key.as_bytes())
            && let Ok(value) = value.parse()
        {
            self.0.insert(name, value);
        }
    }
}

struct MetadataExtractor<'a>(&'a MetadataMap);

impl Extractor for MetadataExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .filter_map(|key| match key {
                KeyRef::Ascii(name) => Some(name.as_str()),
                KeyRef::Binary(_) => None,
            })
            .collect()
    }
}

/// Inject the current span's trace context into outbound gRPC metadata.
pub(crate) fn inject_current_context(metadata: &mut MetadataMap) {
    let context = Span::current().context();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, &mut MetadataInjector(metadata));
    });
}

/// Extract a parent trace context from inbound gRPC metadata.
pub(crate) fn extract_parent_context(metadata: &MetadataMap) -> Context {
    global::get_text_map_propagator(|propagator| propagator.extract(&MetadataExtractor(metadata)))
}
