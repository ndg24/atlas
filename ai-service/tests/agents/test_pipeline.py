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
import atlas_ai.retrieval as retrieval
from atlas_ai.agents.state import PipelineState
from atlas_ai.retrieval import Document

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


class ScriptedProviderWithLiterature(MockProvider):
    """Planner decomposes into one structured + one literature sub-question
    this time, so the full pipeline exercises both of ReportAgent's tagging
    branches in a single run -- closing Phase 8 task 9's actual DoD, which
    the stub-era test above couldn't: "a report with both data-sourced and
    literature-sourced claims"."""

    def __init__(self):
        super().__init__(response=None)

    def complete(self, prompt: str, **kwargs) -> str:
        self.calls.append(prompt)
        if len(self.calls) == 1:
            return json.dumps(
                [
                    {"kind": "structured", "text": "count of patients by hospital"},
                    {"kind": "literature", "text": "known readmission risk factors"},
                ]
            )
        return "General Hospital has 10 patients, the most of any hospital."


def test_pipeline_produces_a_report_with_both_data_and_literature_tagged_claims(monkeypatch):
    monkeypatch.setattr(pipeline_module, "CoordinatorClient", lambda base_url, auth_token: FakeQueryRunner())

    def fake_retrieve(query, corpus_id, k=5):
        assert query == "known readmission risk factors"
        assert corpus_id == "papers"
        return [Document(doc_id="doc-1", text="Prior admissions are a known risk factor.", score=0.9)]

    monkeypatch.setattr(retrieval, "retrieve", fake_retrieve)

    state = pipeline_module.run_pipeline(
        question="what predicts readmission, and what does the literature say?",
        dataset="patients",
        schema_json='{"fields": [{"name": "hospital", "data_type": "Utf8"}]}',
        corpus_id="papers",
        coordinator_url="http://coordinator:8080",
        auth_token="test-token",
        provider=ScriptedProviderWithLiterature(),
    )

    assert "[data]" in state.report
    assert "10" in state.report  # traces to the actual row the engine returned

    assert "[literature:doc-1]" in state.report
    assert "Prior admissions are a known risk factor." in state.report  # traces to the actual retrieved document

    assert len(state.documents) == 1
    assert state.documents[0].doc_id == "doc-1"
