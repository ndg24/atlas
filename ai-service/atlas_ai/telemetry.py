"""OpenTelemetry tracing + Prometheus metrics for the AI service, mirroring
the pattern already used by the Go coordinator
(coordinator/internal/observability/tracing.go) and the Rust worker
(engine/crates/atlas-worker/src/telemetry.rs): a W3C `traceparent` header
riding in gRPC metadata (set automatically by the coordinator's
otelgrpc-instrumented client, cmd/coordinator/main.go) is read back out here
and used to parent this service's spans under the same trace, and
`OTEL_EXPORTER_OTLP_ENDPOINT` gates whether spans are exported anywhere at
all -- unset, tracing costs nothing and the service still runs standalone
with zero extra infra, the same guarantee the other two components make.

Metrics are the one piece with no Go/Rust equivalent to mirror exactly
(neither of those components calls an LLM), so this adds what the root
README's Phase 6 note calls out as still missing: LLM call latency and
token counts, exposed as Prometheus metrics on this service's own port
alongside the catalog/coordinator/worker ports already documented there.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from contextlib import contextmanager

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import extract
from opentelemetry.propagators.textmap import Getter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import Counter, Histogram, start_http_server

_TRACER_NAME = "atlas-ai"

# AI-service metrics port -- the next one free after the worker range
# (9100+, one per worker) in the root README's port table.
DEFAULT_METRICS_PORT = 9096


def init_tracer() -> None:
    """Installs an OTLP/gRPC batch exporter if OTEL_EXPORTER_OTLP_ENDPOINT
    is set; otherwise leaves the SDK's default no-op global TracerProvider
    in place, so every span created via trace.get_tracer(...) below is free
    and goes nowhere. Call once, at server startup.
    """
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    if not endpoint:
        return
    provider = TracerProvider(resource=Resource.create({SERVICE_NAME: "ai-service"}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True)))
    trace.set_tracer_provider(provider)


def init_metrics(port: int = DEFAULT_METRICS_PORT) -> None:
    """Starts a Prometheus exposition server on `port`. Call once, at
    server startup -- safe to call even if nothing ever scrapes it.
    """
    start_http_server(port)


class _GRPCMetadataGetter(Getter):
    """Adapts grpc.aio's invocation_metadata() (a tuple of (key, value)
    pairs, possibly None) to the Getter protocol opentelemetry.propagate.extract
    expects -- the Python-side equivalent of atlas-worker's MetadataExtractor
    over tonic's MetadataMap.
    """

    def get(self, carrier, key):
        key = key.lower()
        for k, v in carrier:
            if k.lower() == key:
                return [v]
        return None

    def keys(self, carrier):
        return [k for k, _ in carrier]


_getter = _GRPCMetadataGetter()


@contextmanager
def span_from_grpc_context(context, rpc_name: str) -> Iterator[trace.Span]:
    """Starts a span for one incoming RPC, parented under the trace context
    extracted from the caller's `traceparent` gRPC metadata header if
    present -- mirrors atlas-worker's `span_from_metadata`. A caller that
    didn't send one still works fine; the span is just a fresh root.
    """
    metadata = context.invocation_metadata() or ()
    parent_ctx = extract(metadata, getter=_getter)
    tracer = trace.get_tracer(_TRACER_NAME)
    with tracer.start_as_current_span(rpc_name, context=parent_ctx) as span:
        yield span


# ---- LLM call metrics ----
#
# Labeled by provider/model rather than folded into one series, since
# ATLAS_LLM_PROVIDER/ATLAS_LLM_MODEL can change between deployments (or,
# for the Anthropic-integration-test path, within one test run) and mixing
# them into one unlabeled number would hide that.

_llm_latency_seconds = Histogram(
    "atlas_ai_llm_call_duration_seconds",
    "Latency of one litellm completion call.",
    ["provider", "model"],
)
_llm_calls_total = Counter(
    "atlas_ai_llm_calls_total",
    "Total litellm completion calls, labeled by provider/model and outcome.",
    ["provider", "model", "outcome"],
)
_llm_tokens_total = Counter(
    "atlas_ai_llm_tokens_total",
    "Total tokens consumed by litellm completion calls, labeled by provider/model and token kind.",
    ["provider", "model", "kind"],
)


@contextmanager
def observe_llm_call(provider: str, model: str) -> Iterator[None]:
    """Wraps one litellm call: records latency and a success/error outcome
    count regardless of what happens inside. Token counts aren't known
    until the call returns a response, so they're reported separately via
    record_tokens once the caller has one.
    """
    started = time.monotonic()
    outcome = "error"
    try:
        yield
        outcome = "success"
    finally:
        _llm_latency_seconds.labels(provider=provider, model=model).observe(time.monotonic() - started)
        _llm_calls_total.labels(provider=provider, model=model, outcome=outcome).inc()


def record_tokens(provider: str, model: str, prompt_tokens: int, completion_tokens: int) -> None:
    """Records a completed call's prompt/completion token counts. Safe to
    call with zeros (some providers don't return usage) -- just a no-op
    increment.
    """
    _llm_tokens_total.labels(provider=provider, model=model, kind="prompt").inc(prompt_tokens)
    _llm_tokens_total.labels(provider=provider, model=model, kind="completion").inc(completion_tokens)
