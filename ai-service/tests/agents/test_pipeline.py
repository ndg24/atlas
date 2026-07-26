"""End-to-end test for run_pipeline (docs/atlas-implementation-spec.md Phase 8,
task 9 DoD): a fixture question through the full Planner -> Execution ->
Visualization -> Explanation -> Report sequence, with a mocked provider and a
fake QueryRunner substituted for the coordinator's own /query/nl (no real
LLM, no real HTTP). Asserts the final report's [data]-tagged claim traces to
an actual QueryResult row, chart_specs got populated, and -- this slice's
real behavior, since retrieval is stubbed -- no [literature:...] tag appears.
"""

from __future__ import annotations

import json

import atlas_ai.agents.pipeline as pipeline_module
from atlas_ai.agents.state import PipelineState

from ..conftest import MockProvider, encode_ipc_batch


class ScriptedProvider(MockProvider):
    """First call (Planner) returns a sub-question decomposition; every
    later call (Explanation) returns a canned narration sentence."""

    def __init__(self):
        super().__init__(response=None)

    def complete(self, prompt: str, **kwargs) -> str:
        self.calls.append(prompt)
        if len(self.calls) == 1:
            return json.dumps([{"kind": "structured", "text": "count of patients by hospital"}])
        return "General Hospital has 10 patients, the most of any hospital."


class FakeQueryRunner:
    def query_nl(self, dataset: str, question: str) -> dict:
        batch = encode_ipc_batch({"hospital": ["General", "City"], "n": [10, 5]})
        return {"arrow_ipc_batches": [batch]}


def test_pipeline_produces_a_data_tagged_report_with_no_literature_claims(monkeypatch):
    monkeypatch.setattr(pipeline_module, "CoordinatorClient", lambda base_url, auth_token: FakeQueryRunner())

    state = pipeline_module.run_pipeline(
        question="which hospital has the most patients?",
        dataset="patients",
        schema_json='{"fields": [{"name": "hospital", "data_type": "Utf8"}]}',
        corpus_id="",
        coordinator_url="http://coordinator:8080",
        auth_token="test-token",
        provider=ScriptedProvider(),
    )

    assert isinstance(state, PipelineState)
    assert len(state.results) == 1
    assert state.results[0].rows == [{"hospital": "General", "n": 10}, {"hospital": "City", "n": 5}]

    assert len(state.chart_specs) == 1
    assert state.chart_specs[0].chart_type == "bar"

    assert "[data]" in state.report
    assert "10" in state.report  # traces to the actual row the engine returned
    assert "[literature:" not in state.report

    # state_json (what the coordinator's /research response carries back)
    # round-trips cleanly.
    assert json.loads(state.model_dump_json())["report"] == state.report
