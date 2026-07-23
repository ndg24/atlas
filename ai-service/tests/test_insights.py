"""Tests for atlas_ai.insights: narrate_findings and suggest_questions
(docs/atlas-implementation-spec.md Phase 7, task 3 / task 5).

narrate_findings mirrors test_explain.py's approach: every number in the
narration must trace back to the findings JSON it was given. suggest_questions
asserts its hard requirement directly -- every returned question must have
actually produced a valid plan through nl_to_plan, not just looked plausible.
"""

from __future__ import annotations

import json
import re

from atlas_ai.insights.narrate import narrate_findings
from atlas_ai.insights.suggest import suggest_questions
from atlas_ai.providers.base import ModelProvider

from .conftest import MockProvider
from .golden_cases import CASES, SCHEMA_JSON


def _numbers_in(text: str) -> set[float]:
    return {float(n) for n in re.findall(r"-?\d+(?:\.\d+)?", text)}


FINDINGS_JSON = json.dumps(
    {
        "summary": {"row_count": 120, "columns": [{"name": "age", "null_rate": 0.05}]},
        "quality_findings": [{"kind": "HighNullRate", "column": "age", "null_rate": 0.3}],
        "outlier_findings": [
            {
                "group": "General Hospital",
                "value": 0.42,
                "group_mean": 0.1,
                "group_stddev": 0.05,
                "z_score": 6.4,
                "group_col": "hospital",
                "value_col": "readmit_rate",
            }
        ],
        "trend_finding": None,
    }
)


def test_narration_numbers_all_trace_to_findings():
    provider = MockProvider(response=None)
    provider.complete = (
        lambda prompt, **kw: "The dataset has 120 rows. Age is missing 30% of the time. "
        "General Hospital's readmit rate of 0.42 is a 6.4 standard-deviation outlier."
    )

    result = narrate_findings(FINDINGS_JSON, provider)

    findings_numbers = _numbers_in(json.dumps(json.loads(FINDINGS_JSON)))
    for n in _numbers_in(result):
        # 30% is derived from 0.3 in the source JSON, so allow the
        # human-readable percentage form alongside the raw fraction.
        assert n in findings_numbers or n / 100 in findings_numbers, (
            f"narration stated {n}, which is not traceable to the supplied findings"
        )


def test_narration_receives_the_actual_findings():
    provider = MockProvider(response=None)
    captured_prompts: list[str] = []
    provider.complete = lambda prompt, **kw: captured_prompts.append(prompt) or "General Hospital is an outlier."

    narrate_findings(FINDINGS_JSON, provider)

    assert captured_prompts, "provider.complete was never called"
    assert "General Hospital" in captured_prompts[0]
    assert "6.4" in captured_prompts[0]


def test_empty_findings_does_not_crash():
    provider = MockProvider(response=None)
    provider.complete = lambda prompt, **kw: "The dataset looks unremarkable."
    result = narrate_findings("{}", provider)
    assert "unremarkable" in result


class SuggestThenPlanProvider(ModelProvider):
    """First call returns the candidate-question list; every later call is
    nl_to_plan validating one candidate -- returns a known-good plan for
    questions containing `good_marker`, and unparseable JSON (so nl_to_plan
    exhausts its one retry and gives up) for everything else. This puts the
    suggest_questions filtering logic under test, not the LLM's judgment."""

    def __init__(self, candidates: list[str], good_plan: dict, good_marker: str):
        self.candidates = candidates
        self.good_plan = good_plan
        self.good_marker = good_marker
        self.calls: list[str] = []

    def complete(self, prompt: str, **kwargs) -> str:
        self.calls.append(prompt)
        if len(self.calls) == 1:
            return json.dumps(self.candidates)
        if self.good_marker in prompt:
            return json.dumps(self.good_plan)
        return "not valid json"


def test_valid_candidates_pass_and_invalid_ones_are_filtered():
    good_case = CASES[0]  # "List the diagnosis and age for every patient."
    candidates = [good_case.question, "asdfasdf this is not a real question"]
    provider = SuggestThenPlanProvider(candidates, good_case.plan, good_marker="diagnosis and age")

    questions = suggest_questions(SCHEMA_JSON, "{}", provider)

    assert questions == [good_case.question]


def test_returns_empty_when_llm_output_is_not_a_json_array():
    provider = MockProvider(response=None)
    provider.complete = lambda prompt, **kw: "here are some questions:\n1. how many rows?"
    assert suggest_questions(SCHEMA_JSON, "{}", provider) == []


def test_stops_once_count_is_reached():
    good_case = CASES[0]
    candidates = [good_case.question] * 10
    provider = SuggestThenPlanProvider(candidates, good_case.plan, good_marker="diagnosis and age")

    questions = suggest_questions(SCHEMA_JSON, "{}", provider, count=3)

    assert len(questions) == 3
