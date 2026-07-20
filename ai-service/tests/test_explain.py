"""Asserts Explain's narration never states a number absent from the result
set it was given (docs/atlas-implementation-spec.md Phase 6 DoD) —
approximated, per the spec, by regex-extracting numbers from the narration
and checking each one appears among the table's own numeric values.
"""

from __future__ import annotations

import io
import re

import pyarrow as pa
import pyarrow.ipc as ipc

from atlas_ai.explain import narrate_result

from .conftest import MockProvider


def _encode_table(table: pa.Table) -> bytes:
    """Round-trip-compatible with engine/crates/atlas-worker/src/ipc.rs's
    `encode_batches`: a self-contained Arrow IPC *stream* (schema + batches)."""
    sink = io.BytesIO()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


def _numbers_in(text: str) -> set[float]:
    return {float(n) for n in re.findall(r"-?\d+(?:\.\d+)?", text)}


def test_narration_numbers_all_trace_to_result():
    table = pa.table({"diagnosis": ["flu", "cold"], "n": [42, 17]})
    provider = MockProvider(response=None)
    provider.complete = lambda prompt, **kw: "Flu appears 42 times and cold appears 17 times."

    result = narrate_result("How many of each diagnosis?", _encode_table(table), provider)

    table_numbers = _numbers_in(str(table.to_pylist()))
    for n in _numbers_in(result):
        assert n in table_numbers, f"narration stated {n}, which is not in the supplied result table"


def test_narration_receives_the_actual_table_contents():
    table = pa.table({"diagnosis": ["flu"], "n": [99]})
    provider = MockProvider(response=None)
    captured_prompts = []
    provider.complete = lambda prompt, **kw: captured_prompts.append(prompt) or "flu: 99"

    narrate_result("How many flu cases?", _encode_table(table), provider)

    assert captured_prompts, "provider.complete was never called"
    assert "99" in captured_prompts[0], "the result table's own values must reach the prompt"


def test_empty_result_does_not_crash():
    provider = MockProvider(response=None)
    provider.complete = lambda prompt, **kw: "There are no matching rows."
    result = narrate_result("Any records for diagnosis 'unknown'?", b"", provider)
    assert "no matching rows" in result
