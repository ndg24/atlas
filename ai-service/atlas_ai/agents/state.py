"""PipelineState: the one typed object the research pipeline's agents
accumulate into (docs/atlas-implementation-spec.md Phase 8, task 1). Each
agent reads only the fields it needs and writes only its own — see
pipeline.py for the fixed run order and the input/output logging that makes
this inspectable.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from ..retrieval import Document


class SubQuestion(BaseModel):
    # "structured" sub-questions are answered by ExecutionAgent via the
    # engine (through /query/nl); "literature" ones are answered by
    # ReportAgent's retrieve() call — currently always empty, see
    # ../retrieval/__init__.py.
    kind: Literal["structured", "literature"]
    text: str


class QueryResult(BaseModel):
    sub_question: str
    rows: list[dict] = Field(default_factory=list)
    row_count: int = 0


class ChartSpec(BaseModel):
    sub_question: str
    chart_type: Literal["stat", "bar", "line", "table"]
    x: str | None = None
    y: str | None = None
    reason: str


class PipelineState(BaseModel):
    question: str
    dataset: str
    schema_json: str
    corpus_id: str = ""

    sub_questions: list[SubQuestion] = Field(default_factory=list)
    results: list[QueryResult] = Field(default_factory=list)
    documents: list[Document] = Field(default_factory=list)
    chart_specs: list[ChartSpec] = Field(default_factory=list)
    # Each entry is one already-tagged claim (ends in " [data]" or
    # " [literature:doc_id]") — see explanation_agent.py, which appends the
    # tag itself rather than trusting the LLM to produce it.
    explanation_sentences: list[str] = Field(default_factory=list)
    report: str = ""

    model_config = {"arbitrary_types_allowed": True}
