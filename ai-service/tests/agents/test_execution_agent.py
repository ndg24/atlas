"""Tests for ExecutionAgent (docs/atlas-implementation-spec.md Phase 8, task 3)."""

from __future__ import annotations

from atlas_ai.agents.execution_agent import ExecutionAgent
from atlas_ai.agents.state import PipelineState, SubQuestion

from ..conftest import encode_ipc_batch


class FakeQueryRunner:
    def __init__(self, responses: dict[str, dict] | None = None, raise_for: set[str] | None = None):
        self.responses = responses or {}
        self.raise_for = raise_for or set()
        self.calls: list[tuple[str, str]] = []

    def query_nl(self, dataset: str, question: str) -> dict:
        self.calls.append((dataset, question))
        if question in self.raise_for:
            raise RuntimeError("coordinator error")
        return self.responses.get(question, {"arrow_ipc_batches": []})


def _state_with(*sub_questions: SubQuestion) -> PipelineState:
    state = PipelineState(question="q", dataset="patients", schema_json="{}")
    state.sub_questions = list(sub_questions)
    return state


def test_decodes_arrow_ipc_batches_into_rows():
    batch = encode_ipc_batch({"hospital": ["General", "City"], "n": [10, 5]})
    runner = FakeQueryRunner(responses={"count by hospital": {"arrow_ipc_batches": [batch]}})
    state = _state_with(SubQuestion(kind="structured", text="count by hospital"))

    ExecutionAgent(runner).run(state)

    assert len(state.results) == 1
    assert state.results[0].row_count == 2
    assert state.results[0].rows == [{"hospital": "General", "n": 10}, {"hospital": "City", "n": 5}]
    assert runner.calls == [("patients", "count by hospital")]


def test_skips_sub_question_when_coordinator_call_fails():
    runner = FakeQueryRunner(raise_for={"bad question"})
    state = _state_with(SubQuestion(kind="structured", text="bad question"))

    ExecutionAgent(runner).run(state)

    assert state.results == []


def test_skips_literature_sub_questions():
    runner = FakeQueryRunner()
    state = _state_with(SubQuestion(kind="literature", text="what does the literature say?"))

    ExecutionAgent(runner).run(state)

    assert state.results == []
    assert runner.calls == []


def test_handles_empty_arrow_ipc_batches():
    runner = FakeQueryRunner(responses={"q": {"arrow_ipc_batches": []}})
    state = _state_with(SubQuestion(kind="structured", text="q"))

    ExecutionAgent(runner).run(state)

    assert len(state.results) == 1
    assert state.results[0].rows == []
    assert state.results[0].row_count == 0
