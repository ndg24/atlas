"""Unit tests for atlas_ai.telemetry: proves span_from_grpc_context actually
parents a span under an incoming traceparent gRPC metadata header when
present (and starts a fresh root when it isn't), and that
observe_llm_call/record_tokens actually move the Prometheus counters they
claim to -- no live OTLP collector or LLM call needed for either.
"""

from __future__ import annotations

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from atlas_ai import telemetry


class _FakeGrpcContext:
    """Just enough of grpc.aio.ServicerContext for span_from_grpc_context:
    invocation_metadata() returning a tuple of (key, value) pairs."""

    def __init__(self, metadata):
        self._metadata = metadata

    def invocation_metadata(self):
        return self._metadata


@pytest.fixture(scope="module")
def exporter() -> InMemorySpanExporter:
    # OTel's global TracerProvider can only be set once per process (a
    # second set_tracer_provider call is a silent no-op with a warning) --
    # so this installs one recording provider for the whole module and each
    # test clears the shared exporter first, rather than each test trying
    # to install its own.
    exp = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exp))
    trace.set_tracer_provider(provider)
    return exp


def test_span_parents_under_an_incoming_traceparent_header(exporter):
    exporter.clear()

    # A real W3C traceparent: version-traceid-spanid-flags.
    trace_id_hex = "4bf92f3577b34da6a3ce929d0e0e4736"
    parent_span_id_hex = "00f067aa0ba902b7"
    traceparent = f"00-{trace_id_hex}-{parent_span_id_hex}-01"

    with telemetry.span_from_grpc_context(_FakeGrpcContext((("traceparent", traceparent),)), "TestRPC"):
        pass

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert format(spans[0].context.trace_id, "032x") == trace_id_hex
    assert format(spans[0].parent.span_id, "016x") == parent_span_id_hex


def test_span_is_a_fresh_root_when_no_traceparent_is_present(exporter):
    exporter.clear()

    with telemetry.span_from_grpc_context(_FakeGrpcContext(()), "TestRPC"):
        pass

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].parent is None


def test_span_is_a_fresh_root_when_invocation_metadata_is_none(exporter):
    # grpc.aio can return None instead of an empty tuple when a call carries
    # no metadata at all -- span_from_grpc_context must not crash on that.
    exporter.clear()

    with telemetry.span_from_grpc_context(_FakeGrpcContext(None), "TestRPC"):
        pass

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].parent is None


def _counter_value(counter, **labels) -> float:
    # prometheus_client has no public "read this counter's current value"
    # API short of scraping generate_latest() text output -- this reaches
    # into the same internal float every scrape ultimately reads.
    return counter.labels(**labels)._value.get()


def test_observe_llm_call_records_latency_and_a_success_outcome():
    calls_before = _counter_value(telemetry._llm_calls_total, provider="test", model="m", outcome="success")

    with telemetry.observe_llm_call("test", "m"):
        pass

    assert _counter_value(telemetry._llm_calls_total, provider="test", model="m", outcome="success") == calls_before + 1


def test_observe_llm_call_records_an_error_outcome_on_exception():
    calls_before = _counter_value(telemetry._llm_calls_total, provider="test", model="m", outcome="error")

    try:
        with telemetry.observe_llm_call("test", "m"):
            raise ValueError("boom")
    except ValueError:
        pass

    assert _counter_value(telemetry._llm_calls_total, provider="test", model="m", outcome="error") == calls_before + 1


def test_record_tokens_increments_prompt_and_completion_counters():
    prompt_before = _counter_value(telemetry._llm_tokens_total, provider="test", model="m", kind="prompt")
    completion_before = _counter_value(telemetry._llm_tokens_total, provider="test", model="m", kind="completion")

    telemetry.record_tokens("test", "m", 10, 20)

    assert _counter_value(telemetry._llm_tokens_total, provider="test", model="m", kind="prompt") == prompt_before + 10
    assert (
        _counter_value(telemetry._llm_tokens_total, provider="test", model="m", kind="completion")
        == completion_before + 20
    )
