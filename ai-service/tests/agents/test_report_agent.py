"""Tests for ReportAgent (docs/atlas-implementation-spec.md Phase 8, task 6/8) --
in particular that a report with no literature sub-questions (this slice's
real behavior, since retrieve() is stubbed) never contains a [literature:...]
tag, and that when documents ARE returned (monkeypatched here to simulate a
future slice), they're tagged correctly.
"""

from __future__ import annotations

import atlas_ai.retrieval as retrieval
from atlas_ai.agents.report_agent import ReportAgent
from atlas_ai.agents.state import PipelineState, SubQuestion
from atlas_ai.retrieval import Document


def test_report_contains_only_data_tags_when_there_are_no_literature_sub_questions():
    state = PipelineState(question="q", dataset="d", schema_json="{}")
    state.sub_questions = [SubQuestion(kind="structured", text="sq")]
    state.explanation_sentences = ["General Hospital leads. [data]"]

    ReportAgent().run(state)

    assert "[data]" in state.report
    assert "[literature:" not in state.report
    assert state.documents == []


def test_literature_sub_question_calls_retrieve_and_tags_documents(monkeypatch):
    def fake_retrieve(query, corpus_id, k=5):
        assert query == "known risk factors"
        assert corpus_id == "papers"
        return [Document(doc_id="doc-1", text="Risk factors include age.", score=0.9)]

    monkeypatch.setattr(retrieval, "retrieve", fake_retrieve)

    state = PipelineState(question="q", dataset="d", schema_json="{}", corpus_id="papers")
    state.sub_questions = [SubQuestion(kind="literature", text="known risk factors")]
    state.explanation_sentences = ["General Hospital leads. [data]"]

    ReportAgent().run(state)

    assert len(state.documents) == 1
    assert "[literature:doc-1]" in state.report
    assert "[data]" in state.report


def test_report_includes_heading_and_question():
    state = PipelineState(question="which hospital sees the most patients?", dataset="d", schema_json="{}")
    state.explanation_sentences = ["General Hospital leads. [data]"]

    ReportAgent().run(state)

    assert "which hospital sees the most patients?" in state.report
