//! Shared tracing/OpenTelemetry initialization for Ursula binaries.
//!
//! Every binary installs the same layered [`tracing_subscriber`] registry: an
//! [`EnvFilter`] driven by `RUST_LOG` (defaulting to `info`) plus a stderr
//! `fmt` layer for local development. When the `otlp` feature is enabled and an
//! OTLP endpoint is configured via `OTEL_EXPORTER_OTLP_ENDPOINT`, an
//! OpenTelemetry layer is added that batch-exports spans to a collector.
//!
//! This crate only sets up the subscriber. The hot read/write path stays
//! span-free by design; these layers exist to carry request-boundary spans and
//! `warn!`/`error!` events, not per-record work.

use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// How a binary wants telemetry initialized.
pub struct InitOptions {
    /// Logical service name reported to the collector (`service.name`).
    pub service_name: &'static str,
    /// Default `EnvFilter` directives used when `RUST_LOG` is unset.
    pub default_directives: &'static str,
    /// Extra resource attributes (e.g. node id) attached to exported spans.
    pub resource: Vec<(&'static str, String)>,
}

impl InitOptions {
    /// Start from the defaults for `service_name` (level `info`, no extra
    /// resource attributes).
    pub fn new(service_name: &'static str) -> Self {
        Self {
            service_name,
            default_directives: "info",
            resource: Vec::new(),
        }
    }

    /// Attach a resource attribute reported alongside exported spans.
    #[must_use]
    pub fn with_resource(mut self, key: &'static str, value: impl Into<String>) -> Self {
        self.resource.push((key, value.into()));
        self
    }
}

/// Holds telemetry resources that must outlive the program and be flushed on
/// shutdown. Drop it as late as possible so buffered spans are exported.
#[must_use = "hold the guard until shutdown so buffered telemetry is flushed"]
pub struct ObservabilityGuard {
    #[cfg(feature = "otlp")]
    tracer_provider: Option<opentelemetry_sdk::trace::SdkTracerProvider>,
    #[cfg(feature = "otlp")]
    meter_provider: Option<opentelemetry_sdk::metrics::SdkMeterProvider>,
}

impl Drop for ObservabilityGuard {
    fn drop(&mut self) {
        #[cfg(feature = "otlp")]
        if let Some(provider) = self.tracer_provider.take() {
            // Flush buffered spans before the exporter task is torn down.
            if let Err(err) = provider.shutdown() {
                tracing::warn!(%err, "failed to flush OTLP span exporter on shutdown");
            }
        }
        #[cfg(feature = "otlp")]
        if let Some(provider) = self.meter_provider.take() {
            // Flush the final metrics reading before exit.
            if let Err(err) = provider.shutdown() {
                tracing::warn!(%err, "failed to flush OTLP metric exporter on shutdown");
            }
        }
    }
}

fn env_filter(default_directives: &str) -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_directives))
}

/// Install the global subscriber. Idempotent: a second call is a no-op because
/// a global subscriber is already registered (matching the previous
/// `fmt().try_init()` behavior).
pub fn init(options: InitOptions) -> ObservabilityGuard {
    let fmt_layer = tracing_subscriber::fmt::layer().with_target(true);

    #[cfg(feature = "otlp")]
    {
        if let Some((otel_layer, tracer_provider)) = otlp::build_layer(&options) {
            let _ = tracing_subscriber::registry()
                .with(env_filter(options.default_directives))
                .with(fmt_layer)
                .with(otel_layer)
                .try_init();
            // Metrics export is independent of the span layer; set the global
            // meter provider so the rest of the process can register
            // instruments via `opentelemetry::global::meter`.
            let meter_provider = otlp::build_meter_provider(&options);
            if let Some(provider) = meter_provider.as_ref() {
                opentelemetry::global::set_meter_provider(provider.clone());
            }
            return ObservabilityGuard {
                tracer_provider: Some(tracer_provider),
                meter_provider,
            };
        }
    }

    let _ = tracing_subscriber::registry()
        .with(env_filter(options.default_directives))
        .with(fmt_layer)
        .try_init();

    ObservabilityGuard {
        #[cfg(feature = "otlp")]
        tracer_provider: None,
        #[cfg(feature = "otlp")]
        meter_provider: None,
    }
}

#[cfg(feature = "otlp")]
mod otlp {
    use opentelemetry::KeyValue;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use tracing_opentelemetry::OpenTelemetryLayer;
    use tracing_subscriber::registry::LookupSpan;

    use super::InitOptions;

    /// Environment variable that opts a process into OTLP export. When unset we
    /// stay fmt-only so the default deployment pays nothing for telemetry.
    const ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";

    type Layer<S> = OpenTelemetryLayer<S, opentelemetry_sdk::trace::Tracer>;

    /// Build the OpenTelemetry tracing layer, or return `None` when no endpoint
    /// is configured (fmt-only) or the exporter cannot be constructed.
    pub(super) fn build_layer<S>(options: &InitOptions) -> Option<(Layer<S>, SdkTracerProvider)>
    where S: tracing::Subscriber + for<'span> LookupSpan<'span> {
        // Stay fmt-only unless an OTLP endpoint is configured.
        std::env::var_os(ENDPOINT_ENV)?;

        // W3C traceparent propagation so spans cross the Raft gRPC transport.
        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );

        // HTTP/protobuf exporter reuses the existing reqwest stack and avoids a
        // second gRPC/tonic dependency tree alongside the Raft transport. The
        // endpoint is read from `OTEL_EXPORTER_OTLP_ENDPOINT`.
        let exporter = match opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
            .build()
        {
            Ok(exporter) => exporter,
            Err(err) => {
                tracing::warn!(%err, "OTLP span exporter unavailable; continuing without export");
                return None;
            }
        };

        let mut attributes = vec![KeyValue::new("service.name", options.service_name)];
        attributes.extend(
            options
                .resource
                .iter()
                .map(|(key, value)| KeyValue::new(*key, value.clone())),
        );
        let resource = Resource::builder().with_attributes(attributes).build();

        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(resource)
            .build();

        let tracer = provider.tracer("ursula");
        let layer = tracing_opentelemetry::layer().with_tracer(tracer);
        Some((layer, provider))
    }

    /// Build a periodic OTLP metrics exporter, or `None` when no endpoint is
    /// configured or the exporter cannot be constructed.
    pub(super) fn build_meter_provider(
        options: &InitOptions,
    ) -> Option<opentelemetry_sdk::metrics::SdkMeterProvider> {
        std::env::var_os(ENDPOINT_ENV)?;

        let exporter = match opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
            .build()
        {
            Ok(exporter) => exporter,
            Err(err) => {
                tracing::warn!(%err, "OTLP metric exporter unavailable; continuing without export");
                return None;
            }
        };

        let mut attributes = vec![KeyValue::new("service.name", options.service_name)];
        attributes.extend(
            options
                .resource
                .iter()
                .map(|(key, value)| KeyValue::new(*key, value.clone())),
        );
        let resource = Resource::builder().with_attributes(attributes).build();

        Some(
            opentelemetry_sdk::metrics::SdkMeterProvider::builder()
                .with_periodic_exporter(exporter)
                .with_resource(resource)
                .build(),
        )
    }
}
