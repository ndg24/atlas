//! Tracing setup for atlas-worker: always installs a `tracing_subscriber`
//! text-log layer (so `cargo run`/local debugging shows spans with zero
//! extra infra), and additionally bridges to an OTLP-exporting
//! `tracing_opentelemetry` layer when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.
//!
//! `proto/worker.proto` carries no trace-id field on any RPC message, so a
//! request's trace context rides in tonic's gRPC **metadata** instead: the
//! Go coordinator's `otelgrpc` client handler
//! (coordinator/internal/scheduler/registry.go) injects a W3C `traceparent`
//! header automatically; `span_from_metadata` below reads it back out and
//! parents this worker's span under the coordinator's trace, so one trace id
//! spans coordinator -> catalog -> worker.

use std::env;

use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::TracerProvider as SdkTracerProvider;
use tonic::metadata::MetadataMap;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

/// Holds the SDK tracer provider for as long as the process runs — dropping
/// it (via `shutdown`) flushes any batched-but-unsent spans.
static TRACER_PROVIDER: std::sync::OnceLock<Option<SdkTracerProvider>> = std::sync::OnceLock::new();

/// Installs the global tracing subscriber. Call once, at startup, before
/// serving any requests.
pub fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer();

    let endpoint = env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok();
    let otel_provider = endpoint.and_then(|endpoint| {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
            .map_err(|err| {
                eprintln!("failed to build OTLP exporter, tracing will be local-only: {err:#}")
            })
            .ok()?;
        Some(
            SdkTracerProvider::builder()
                .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
                .build(),
        )
    });

    if let Some(provider) = &otel_provider {
        let tracer = provider.tracer("atlas-worker");
        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
        Registry::default()
            .with(filter)
            .with(fmt_layer)
            .with(otel_layer)
            .init();
    } else {
        Registry::default().with(filter).with(fmt_layer).init();
    }

    let _ = TRACER_PROVIDER.set(otel_provider);

    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
}

/// Best-effort flush of any batched spans not yet exported. Call on
/// graceful shutdown.
pub fn shutdown() {
    if let Some(Some(provider)) = TRACER_PROVIDER.get() {
        let _ = provider.shutdown();
    }
}

/// Adapts a tonic `MetadataMap` to `opentelemetry`'s `Extractor` trait, so
/// the global propagator can read a `traceparent` header out of incoming
/// gRPC request metadata.
struct MetadataExtractor<'a>(&'a MetadataMap);

impl<'a> Extractor for MetadataExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .filter_map(|k| match k {
                tonic::metadata::KeyRef::Ascii(k) => Some(k.as_str()),
                tonic::metadata::KeyRef::Binary(_) => None,
            })
            .collect()
    }
}

/// Builds a span for one incoming RPC, parented under the trace context
/// extracted from `metadata` (if the caller sent a `traceparent` header) —
/// a no-op remote parent if it didn't, so this works whether or not the
/// caller is trace-instrumented.
pub fn span_from_metadata(metadata: &MetadataMap, rpc_name: &'static str) -> Span {
    let propagator = TraceContextPropagator::new();
    let parent_cx = propagator.extract(&MetadataExtractor(metadata));

    let span = tracing::info_span!("worker_rpc", rpc = rpc_name);
    span.set_parent(parent_cx);
    span
}
