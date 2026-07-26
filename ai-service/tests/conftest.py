"""MockProvider: a canned-response ModelProvider used by every golden test
so CI never makes a real LLM call. Real-provider runs are a separate,
integration-flag-gated pass (see test_nl_to_plan.py's
ATLAS_AI_INTEGRATION-gated tests).
"""

from __future__ import annotations

import base64
import io
import json

import pyarrow as pa
import pyarrow.ipc as ipc

from atlas_ai.providers.base import ModelProvider


def encode_ipc_batch(columns: dict) -> str:
    """Builds one self-contained Arrow IPC stream from `columns` (e.g.
    {"hospital": ["General", "City"], "n": [10, 5]}) and returns it
    base64-encoded, matching the shape /query/nl's arrow_ipc_batches carries
    (coordinator/internal/api/server.go's encodeBatches) -- used by agent
    tests that stand in for a coordinator response without a real HTTP call.
    """
    table = pa.table(columns)
    buf = io.BytesIO()
    with ipc.new_stream(buf, table.schema) as writer:
        writer.write_table(table)
    return base64.b64encode(buf.getvalue()).decode()


class MockProvider(ModelProvider):
    """Returns `response` (a plan dict, JSON-encoded) every call — enough for
    every golden case, which are all designed to validate on the first try."""

    def __init__(self, response: dict):
        self.response = response
        self.calls: list[str] = []

    def complete(self, prompt: str, **kwargs) -> str:
        self.calls.append(prompt)
        return json.dumps(self.response)


class FlakyThenGoodProvider(ModelProvider):
    """First call returns something that fails validation; second call (the
    one re-prompt nl_to_plan allows) returns a valid plan — proves the
    retry-once behavior actually recovers instead of just giving up."""

    def __init__(self, bad_response: str, good_response: dict):
        self.bad_response = bad_response
        self.good_response = good_response
        self.calls: list[str] = []

    def complete(self, prompt: str, **kwargs) -> str:
        self.calls.append(prompt)
        if len(self.calls) == 1:
            return self.bad_response
        return json.dumps(self.good_response)


class AlwaysBadProvider(ModelProvider):
    """Never produces a valid plan — proves nl_to_plan gives up after exactly
    one retry rather than looping forever."""

    def __init__(self, bad_response: str = "not json at all"):
        self.bad_response = bad_response
        self.calls: list[str] = []

    def complete(self, prompt: str, **kwargs) -> str:
        self.calls.append(prompt)
        return self.bad_response
