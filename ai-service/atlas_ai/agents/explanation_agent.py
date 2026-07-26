"""ExplanationAgent: one narrated sentence per structured QueryResult
(docs/atlas-implementation-spec.md Phase 8, task 6), same "state only numbers
already present in the input" constraint narrate.py/explain.py already hold
the LLM to. The " [data]" provenance tag is appended by this code *after*
the completion returns -- never requested from the model -- so a claim's
attribution can never be dropped or garbled by a bad completion.
"""

from __future__ import annotations

import json

from ..providers.base import ModelProvider
from .state import PipelineState

_PROMPT_TEMPLATE = """You are given the result of a data query as a table, and the
sub-question it answers. Write one short, plain-English sentence describing it.

Rules:
- State ONLY numbers that appear in the table below -- never estimate, round to a
  value not present, or invent a figure.
- If the table is empty, say so plainly instead of guessing.
- Output exactly one sentence, no preamble.

Sub-question: "{sub_question}"

Result table ({num_rows} rows):
{rows_json}

Sentence:"""


class ExplanationAgent:
    def __init__(self, provider: ModelProvider):
        self._provider = provider

    def run(self, state: PipelineState) -> PipelineState:
        for result in state.results:
            prompt = _PROMPT_TEMPLATE.format(
                sub_question=result.sub_question,
                num_rows=result.row_count,
                rows_json=json.dumps(result.rows) if result.rows else "(empty)",
            )
            sentence = self._provider.complete(prompt).strip()
            state.explanation_sentences.append(f"{sentence} [data]")
        return state
