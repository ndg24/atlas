"""Tests for ExplanationAgent (docs/atlas-implementation-spec.md Phase 8, task 6) --
the [data] tag must be appended by the agent itself, never trusted to the model.
"""

from __future__ import annotations

from atlas_ai.agents.explanation_agent import ExplanationAgent
from atlas_ai.agents.state import PipelineState, QueryResult

from ..conftest import MockProvider


def test_appends_data_tag_regardless_of_model_output():
    provider = MockProvider(response=None)
    provider.complete = lambda prompt, **kw: "General Hospital has the most patients."

    state = PipelineState(question="q", dataset="d", schema_json="{}")
    state.results = [QueryResult(sub_question="count by hospital", rows=[{"hospital": "General", "n": 10}], row_count=1)]

    ExplanationAgent(provider).run(state)

    assert state.explanation_sentences == ["General Hospital has the most patients. [data]"]


def test_tag_is_appended_even_if_model_already_tried_to_add_one():
    provider = MockProvider(response=None)
    provider.complete = lambda prompt, **kw: "General Hospital has the most patients. [data]"

    state = PipelineState(question="q", dataset="d", schema_json="{}")
    state.results = [QueryResult(sub_question="sq", rows=[{"n": 1}], row_count=1)]

    ExplanationAgent(provider).run(state)

    # The agent appends its own tag unconditionally -- it never trusts (or
    # strips) whatever the model produced.
    assert state.explanation_sentences == ["General Hospital has the most patients. [data] [data]"]


def test_one_sentence_per_result_in_order():
    provider = MockProvider(response=None)
    calls = []
    provider.complete = lambda prompt, **kw: calls.append(prompt) or f"sentence {len(calls)}"

    state = PipelineState(question="q", dataset="d", schema_json="{}")
    state.results = [
        QueryResult(sub_question="sq1", rows=[{"n": 1}], row_count=1),
        QueryResult(sub_question="sq2", rows=[{"n": 2}], row_count=1),
    ]

    ExplanationAgent(provider).run(state)

    assert state.explanation_sentences == ["sentence 1 [data]", "sentence 2 [data]"]
    assert "sq1" in calls[0]
    assert "sq2" in calls[1]


def test_empty_rows_still_produces_a_tagged_sentence():
    provider = MockProvider(response=None)
    provider.complete = lambda prompt, **kw: "No matching records were found."

    state = PipelineState(question="q", dataset="d", schema_json="{}")
    state.results = [QueryResult(sub_question="sq", rows=[], row_count=0)]

    ExplanationAgent(provider).run(state)

    assert state.explanation_sentences == ["No matching records were found. [data]"]
