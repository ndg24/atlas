"""PlannerAgent: decomposes a research question into sub-questions — some
answerable by the structured engine, some needing literature retrieval
(docs/atlas-implementation-spec.md Phase 8, task 2). E.g. "what factors are
associated with diabetes readmission" -> a structured sub-question
("readmission rate by comorbidity in the dataset") plus a literature one
("known diabetes readmission risk factors in clinical literature").
"""

from __future__ import annotations

import json
import logging

from ..plan.planner import _strip_code_fence
from ..providers.base import ModelProvider
from .state import PipelineState, SubQuestion

logger = logging.getLogger(__name__)

_PROMPT_TEMPLATE = """You are planning how to answer a research question about a
dataset. Decompose the question into a small number of sub-questions.

Rules:
- Output ONLY a JSON array of objects, each {{"kind": "structured" or
  "literature", "text": "..."}} -- no prose, no markdown code fences.
- "structured" sub-questions must be answerable by querying the dataset alone
  (grouping, filtering, aggregating its columns).
- "literature" sub-questions are ones that need outside domain knowledge or
  published research, not anything in the dataset itself.
- Include at least one "structured" sub-question.

Dataset schema:
{schema_json}

Research question: "{question}"

Sub-questions:"""


def _parse_sub_questions(raw_output: str) -> list[SubQuestion]:
    try:
        candidates = json.loads(_strip_code_fence(raw_output))
    except json.JSONDecodeError:
        return []
    if not isinstance(candidates, list):
        return []
    parsed = []
    for c in candidates:
        try:
            parsed.append(SubQuestion.model_validate(c))
        except Exception:
            continue
    return parsed


class PlannerAgent:
    def __init__(self, provider: ModelProvider):
        self._provider = provider

    def run(self, state: PipelineState) -> PipelineState:
        prompt = _PROMPT_TEMPLATE.format(schema_json=state.schema_json, question=state.question)
        raw_output = self._provider.complete(prompt)
        sub_questions = _parse_sub_questions(raw_output)
        if not sub_questions:
            logger.info("planner output didn't decompose into sub-questions; falling back to the original question")
            sub_questions = [SubQuestion(kind="structured", text=state.question)]
        state.sub_questions = sub_questions
        return state
