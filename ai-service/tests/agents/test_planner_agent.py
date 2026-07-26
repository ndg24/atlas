"""Tests for PlannerAgent (docs/atlas-implementation-spec.md Phase 8, task 2)."""

from __future__ import annotations

import json

from atlas_ai.agents.planner_agent import PlannerAgent
from atlas_ai.agents.state import PipelineState

from ..conftest import MockProvider


def _state() -> PipelineState:
    return PipelineState(question="which diagnosis is most common?", dataset="patients", schema_json="{}")


def test_decomposes_into_structured_and_literature_sub_questions():
    provider = MockProvider(response=None)
    provider.complete = lambda prompt, **kw: json.dumps(
        [
            {"kind": "structured", "text": "count of records by diagnosis"},
            {"kind": "literature", "text": "known risk factors for the most common diagnosis"},
        ]
    )

    state = PlannerAgent(provider).run(_state())

    assert len(state.sub_questions) == 2
    assert state.sub_questions[0].kind == "structured"
    assert state.sub_questions[1].kind == "literature"


def test_falls_back_to_original_question_when_output_is_unparseable():
    provider = MockProvider(response=None)
    provider.complete = lambda prompt, **kw: "not json at all"

    state = PlannerAgent(provider).run(_state())

    assert len(state.sub_questions) == 1
    assert state.sub_questions[0].kind == "structured"
    assert state.sub_questions[0].text == _state().question


def test_falls_back_when_output_is_not_a_json_array():
    provider = MockProvider(response=None)
    provider.complete = lambda prompt, **kw: json.dumps({"kind": "structured", "text": "x"})

    state = PlannerAgent(provider).run(_state())

    assert len(state.sub_questions) == 1
    assert state.sub_questions[0].text == _state().question


def test_prompt_includes_question_and_schema():
    provider = MockProvider(response=None)
    captured = []
    provider.complete = lambda prompt, **kw: captured.append(prompt) or json.dumps(
        [{"kind": "structured", "text": "x"}]
    )

    state = PipelineState(question="how many patients per hospital?", dataset="patients", schema_json='{"fields": []}')
    PlannerAgent(provider).run(state)

    assert "how many patients per hospital?" in captured[0]
    assert '"fields": []' in captured[0]
