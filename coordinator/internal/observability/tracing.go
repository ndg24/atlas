// Package observability wires up OpenTelemetry tracing and shared
// Prometheus gRPC metrics for the coordinator and catalog binaries, so a
// request's trace id propagates from the REST entrypoint through gRPC calls
// to the catalog and, via the W3C traceparent header riding in gRPC
// metadata, into the Rust worker (engine/crates/atlas-worker/src/telemetry.rs
// reads it on that side).
package observability

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// InitTracer installs the W3C trace-context propagator (always) and, if
// OTEL_EXPORTER_OTLP_ENDPOINT is set, a batching OTLP/gRPC exporter under
// serviceName. If the env var is unset, no TracerProvider is installed —
// otel's default global TracerProvider is already a no-op, so every span
// created via otel.Tracer(...) is a cheap no-op and the binary runs
// standalone with zero extra infra. The returned shutdown func flushes and
// closes the exporter; safe to call even when no exporter was installed.
func InitTracer(ctx context.Context, serviceName string) (shutdown func(context.Context) error, err error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})

	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}

	exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpointURL(endpoint), otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("creating OTLP trace exporter: %w", err)
	}

	res, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceName(serviceName)))
	if err != nil {
		return nil, fmt.Errorf("building OTel resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)

	return tp.Shutdown, nil
}
