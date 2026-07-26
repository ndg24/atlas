"""Tests for VisualizationAgent (docs/atlas-implementation-spec.md Phase 8, task 5) --
purely rule-based, no LLM/provider involved.
"""

from __future__ import annotations

from atlas_ai.agents.state import PipelineState, QueryResult
from atlas_ai.agents.visualization_agent import VisualizationAgent


def _run(rows: list[dict]) -> str:
    state = PipelineState(question="q", dataset="d", schema_json="{}")
    state.results = [QueryResult(sub_question="sq", rows=rows, row_count=len(rows))]
    VisualizationAgent().run(state)
    return state.chart_specs[0]


def test_single_row_result_is_a_stat():
    spec = _run([{"n": 42}])
    assert spec.chart_type == "stat"


def test_category_plus_numeric_is_a_bar():
    spec = _run([{"hospital": "General", "n": 10}, {"hospital": "City", "n": 5}])
    assert spec.chart_type == "bar"
    assert spec.x == "hospital"
    assert spec.y == "n"


def test_date_plus_numeric_is_a_line():
    spec = _run([{"admit_date": "2024-01-01", "cost": 100}, {"admit_date": "2024-01-02", "cost": 200}])
    assert spec.chart_type == "line"
    assert spec.x == "admit_date"
    assert spec.y == "cost"


def test_no_rows_is_a_table():
    spec = _run([])
    assert spec.chart_type == "table"


def test_no_clear_category_value_pair_is_a_table():
    spec = _run([{"a": "x", "b": "y"}, {"a": "z", "b": "w"}])
    assert spec.chart_type == "table"
